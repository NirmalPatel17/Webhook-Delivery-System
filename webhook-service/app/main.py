from datetime import datetime
import os
import json
import hmac
import hashlib
import uuid

from fastapi import (
    Body,
    FastAPI,
    HTTPException,
    Request,
    BackgroundTasks,
)
from fastapi.concurrency import asynccontextmanager
from fastapi.responses import Response, JSONResponse
from pymongo import MongoClient, ASCENDING, errors as pymongo_errors
from pymongo.errors import DuplicateKeyError, PyMongoError
from dotenv import load_dotenv
from prometheus_client import generate_latest
import structlog

from app.models import SearchWebhooksBody, WebhookEvent
from app.tasks import deliver_webhook
from app.metrics import events_received
from app.logger import configure_logging


# -------------------------------------------------------------------
# Environment
# -------------------------------------------------------------------
load_dotenv()
MONGODB_URL = os.getenv("MONGODB_URL")
SECRET_KEY = os.getenv("SECRET_KEY").encode()

# -------------------------------------------------------------------
# Logging (structlog)
# -------------------------------------------------------------------
configure_logging()
logger = structlog.get_logger("webhook-service")


# -------------------------------------------------------------------
# Database
# -------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for MongoDB connection and indexing."""
    try:
        mongo_client = MongoClient(
            MONGODB_URL,
            maxPoolSize=5,  # max concurrent connections
            minPoolSize=1,  # warm connections
            retryWrites=True,
            retryReads=True,
        )
        mongo_client.admin.command("ping")
        db = mongo_client.webhooks
        events_collection = db.events

        # Ensure indexes (idempotent)
        events_collection.create_index([("status", ASCENDING)], name="idx_status")
        events_collection.create_index(
            [("event_type", ASCENDING)], name="idx_event_type"
        )
        events_collection.create_index(
            [("received_at", ASCENDING)], name="idx_received_at"
        )
        events_collection.create_index(
            [("status", ASCENDING), ("received_at", ASCENDING)],
            name="idx_status_received_at",
        )
        events_collection.create_index(
            [("event_type", ASCENDING), ("received_at", ASCENDING)],
            name="idx_event_type_received_at",
        )
        db.events.create_index([("idempotency_key", 1)], unique=True, sparse=True)

        # Store in app state for use in routes
        app.state.mongo_client = mongo_client
        app.state.db = db
        app.state.events_collection = events_collection

        logger.info("MongoDB connected and indexes ensured")
        yield
    except PyMongoError as e:
        logger.exception("mongodb_connection_failed", error=str(e))
        raise RuntimeError("Failed to connect to MongoDB") from e
    finally:
        mongo_client.close()
        logger.info("MongoDB connection closed")


# -------------------------------------------------------------------
# App
# -------------------------------------------------------------------
app = FastAPI(title="Webhook Receiver Service", lifespan=lifespan)


# -------------------------------------------------------------------
# Middleware: Request ID
# -------------------------------------------------------------------
@app.middleware("http")
async def request_id_middleware(request: Request, call_next):
    request_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
    structlog.contextvars.bind_contextvars(request_id=request_id)

    try:
        response = await call_next(request)
    except Exception as e:
        logger.exception("middleware_exception", error=str(e))
        raise
    response.headers["X-Request-ID"] = request_id
    return response


# -------------------------------------------------------------------
# Health Check
# -------------------------------------------------------------------
@app.get("/health")
async def health():
    return {"status": "ok"}


# -------------------------------------------------------------------
# Metrics
# -------------------------------------------------------------------
@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type="text/plain")


# try:
#     mongo_client = MongoClient(
#         MONGODB_URL,
#         maxPoolSize=5,  # max concurrent connections
#         minPoolSize=1,  # warm connections
#         maxIdleTimeMS=60_000,  # close idle connections
#         waitQueueTimeoutMS=5_000,  # wait time when pool is exhausted
#         serverSelectionTimeoutMS=5_000,
#         connectTimeoutMS=5_000,
#         socketTimeoutMS=10_000,
#         retryWrites=True,
#         retryReads=True,
#     )
#     mongo_client.admin.command("ping")
#     db = mongo_client.webhooks
#     events_collection = db.events
#     ensure_indexes()
# except pymongo_errors.PyMongoError as e:
#     logger.exception("mongodb_connection_failed", error=str(e))
#     raise RuntimeError("Failed to connect to MongoDB") from e


# -------------------------------------------------------------------
# Security
# -------------------------------------------------------------------
def verify_hmac(payload: bytes, signature: str) -> bool:
    expected_signature = hmac.new(SECRET_KEY, payload, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected_signature, signature)


@app.post("/webhooks/ingest")
async def ingest_webhook_batch(
    request: Request,
    background_tasks: BackgroundTasks,
    payload = Body(
        ...,
        openapi_examples={
            "example-1": {
                "summary": "Single Event",
                "description": "",
                "value": {
                    "event_type": "order.created",
                    "event_id": "evt_1002",
                    "timestamp": "2025-01-01T11:00:00Z",
                    "data": {"order_id": "ORD-123", "amount": 1499, "currency": "INR"},
                    "idempotency_key": "1234567890",
                },
            },
            "example-3": {
                "summary": "Batch Events",
                "description": "",
                "value": {
                    "event_type": "order.created",
                    "event_id": "evt_1002",
                    "timestamp": "2025-01-01T11:00:00Z",
                    "data": {"order_id": "ORD-123", "amount": 1499, "currency": "INR"},
                    "idempotency_key": "1234567890",
                },
            },
        },
    ),
):
    logger.info("webhook_ingest_started")
    events_collection = request.app.state.events_collection
    try:
        # Signature verification
        body = await request.body()
        signature = request.headers.get("X-Signature")

        if not signature:
            logger.warning("missing_signature")
            raise HTTPException(status_code=400, detail="Missing signature")

        if not verify_hmac(body, signature):
            logger.warning("invalid_signature")
            raise HTTPException(status_code=401, detail="Invalid signature")

        # Parse JSON
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            logger.warning("invalid_json_payload")
            raise HTTPException(status_code=400, detail="Invalid JSON payload")

        # Normalize to list (single or batch)
        events = payload if isinstance(payload, list) else [payload]

        request_id = structlog.contextvars.get_contextvars().get("request_id")
        response_events: list[dict] = []

        # Process each event independently (batch-safe)
        for event_payload in events:
            event_type = event_payload.get("event_type")
            idempotency_key = event_payload.get("idempotency_key")

            event = WebhookEvent(
                payload=event_payload,
                event_type=event_type,
                idempotency_key=idempotency_key,
            )

            try:
                # Atomic insert (replica-safe)
                result = events_collection.insert_one(event.dict(by_alias=True))
                event_id = str(result.inserted_id)

                events_received.inc()

                logger.info(
                    "event_stored",
                    event_id=event_id,
                    event_type=event_type,
                )

                # Schedule delivery ONLY for newly created events
                try:
                    background_tasks.add_task(
                        deliver_webhook.delay,
                        event_id,
                        request_id,
                    )
                except Exception as e:
                    logger.exception(
                        "background_task_failed",
                        error=str(e),
                        event_id=event_id,
                    )

                response_events.append(
                    {
                        "status": "received",
                        "event_id": event_id,
                        "idempotent": False,
                    }
                )

            except DuplicateKeyError:
                # Idempotent replay (safe under replicas)
                existing = events_collection.find_one(
                    {"idempotency_key": idempotency_key}, {"_id": 1}
                )

                if not existing:
                    logger.error(
                        "duplicate_key_but_event_not_found",
                        idempotency_key=idempotency_key,
                    )
                    raise HTTPException(status_code=500, detail="Idempotency conflict")

                event_id = str(existing["_id"])

                logger.info(
                    "duplicate_event",
                    event_id=event_id,
                    event_type=event_type,
                )

                response_events.append(
                    {
                        "status": "received",
                        "event_id": event_id,
                        "idempotent": True,
                    }
                )

            except pymongo_errors.PyMongoError as e:
                logger.exception(
                    "mongodb_insert_failed",
                    error=str(e),
                )
                raise HTTPException(status_code=500, detail="Internal server error")

        # Always return list (clean API contract)
        return JSONResponse(
            status_code=200,
            content=response_events,
        )

    except HTTPException:
        raise

    except Exception as e:
        logger.exception("ingest_webhook_failed", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/webhooks/search")
async def search_webhooks(request: Request, body: SearchWebhooksBody):
    events_collection = request.app.state.events_collection
    query: dict = {}

    if body.status:
        query["status"] = body.status

    if body.event_type:
        query["event_type"] = body.event_type

    if body.from_timestamp or body.to_timestamp:
        query["received_at"] = {}
        if body.from_timestamp:
            query["received_at"]["$gte"] = body.from_timestamp
        if body.to_timestamp:
            query["received_at"]["$lte"] = body.to_timestamp

    logger.info("search_started", filters=query)

    try:
        # Fetch events with pagination
        events_cursor = events_collection.find(query).skip(body.skip).limit(body.limit)
        events = []
        for event in events_cursor:
            event["_id"] = str(event["_id"])
            # Convert datetime fields to string
            if "received_at" in event and isinstance(event["received_at"], datetime):
                event["received_at"] = event["received_at"].isoformat()
            events.append(event)

        # Aggregations
        pipeline = [{"$match": query}]

        # Count by status
        pipeline_status = pipeline + [
            {"$group": {"_id": "$status", "count": {"$sum": 1}}}
        ]
        status_counts = list(events_collection.aggregate(pipeline_status))
        status_counts_dict = {item["_id"]: item["count"] for item in status_counts}

        # Count by event type
        pipeline_type = pipeline + [
            {"$group": {"_id": "$event_type", "count": {"$sum": 1}}}
        ]
        type_counts = list(events_collection.aggregate(pipeline_type))
        type_counts_dict = {item["_id"]: item["count"] for item in type_counts}

        # Hourly histogram
        pipeline_histogram = pipeline + [
            {
                "$group": {
                    "_id": {
                        "year": {"$year": "$received_at"},
                        "month": {"$month": "$received_at"},
                        "day": {"$dayOfMonth": "$received_at"},
                        "hour": {"$hour": "$received_at"},
                    },
                    "count": {"$sum": 1},
                }
            },
            {"$sort": {"_id": 1}},
        ]
        histogram = list(events_collection.aggregate(pipeline_histogram))
        # Format histogram timestamps
        for h in histogram:
            h["_id"] = (
                f"{h['_id']['year']}-{h['_id']['month']:02}-{h['_id']['day']:02} {h['_id']['hour']:02}:00"
            )

        logger.info("search_completed", returned=len(events))

        return JSONResponse(
            status_code=200,
            content=json.loads(
                json.dumps(
                    {
                        "data": events,
                        "summary": {
                            "status_counts": status_counts_dict,
                            "event_type_counts": type_counts_dict,
                            "hourly_histogram": histogram,
                        },
                    },
                    default=str,
                )
            ),
        )

    except pymongo_errors.PyMongoError as e:
        logger.exception("mongodb_search_failed", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")

    except HTTPException:
        raise

    except Exception as e:
        logger.exception("search_webhooks_failed", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")
