import os
import time
from datetime import datetime
import httpx
import structlog
from celery import Celery
from dotenv import load_dotenv
from pymongo import MongoClient, ReturnDocument, errors
from bson import ObjectId
from app.logger import configure_logging
from app.metrics import deliveries_successful, deliveries_failed, retry_attempts

# -------------------------------------------------------------------
# Environment
# -------------------------------------------------------------------
load_dotenv()

MONGODB_URL = os.getenv("MONGODB_URL")
REDIS_URL = os.getenv("REDIS_URL")
DOWNSTREAM_URL = os.getenv("DOWNSTREAM_URL")

# -------------------------------------------------------------------
# Celery
# -------------------------------------------------------------------
celery_app = Celery(
    "webhook_delivery",
    broker=REDIS_URL,
)

# -------------------------------------------------------------------
# Logging (structlog)
# -------------------------------------------------------------------
configure_logging()
logger = structlog.get_logger("webhook-delivery-task")

# -------------------------------------------------------------------
# Database
# -------------------------------------------------------------------
try:
    mongo_client = MongoClient(MONGODB_URL)
    # Trigger connection test
    mongo_client.admin.command('ping')
    db = mongo_client.webhooks
    events_collection = db.events
except errors.PyMongoError as e:
    logger.critical("mongodb_connection_failed", error=str(e))
    raise SystemExit("Cannot connect to MongoDB")

# -------------------------------------------------------------------
# Delivery Task
# -------------------------------------------------------------------
@celery_app.task(bind=True, acks_late=True, autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={"max_retries": 3})
def deliver_webhook(self, event_id: str, request_id: str | None = None):
    if request_id:
        structlog.contextvars.bind_contextvars(request_id=request_id)

    logger.info("delivery_started", event_id=event_id)

    # Validate event_id
    try:
        obj_id = ObjectId(event_id)
    except Exception as e:
        logger.error("invalid_event_id", event_id=event_id, error=str(e))
        deliveries_failed.inc()
        return

    # Atomically claim event
    try:
        event = events_collection.find_one_and_update(
            {"_id": obj_id, "status": "RECEIVED"},
            {"$set": {"status": "DELIVERING", "locked_at": datetime.utcnow()}},
            return_document=ReturnDocument.AFTER,
        )
    except errors.PyMongoError as e:
        logger.error("mongodb_find_and_update_failed", event_id=event_id, error=str(e))
        deliveries_failed.inc()
        return

    if not event:
        logger.info("event_already_claimed_or_missing", event_id=event_id)
        return

    max_attempts = 5
    backoff = [1, 2, 4, 8, 16]

    for attempt in range(max_attempts):
        attempt_number = attempt + 1
        try:
            logger.info("delivery_attempt_started", event_id=event_id, attempt=attempt_number)

            try:
                response = httpx.post(DOWNSTREAM_URL, json=event.get("payload", {}), timeout=10.0)
                http_status = response.status_code
                success = http_status == 200
            except httpx.RequestError as e:
                http_status = None
                success = False
                logger.exception("http_request_failed", event_id=event_id, attempt=attempt_number, error=str(e))

            # Update delivery attempts in DB
            try:
                events_collection.update_one(
                    {"_id": obj_id},
                    {
                        "$push": {
                            "delivery_attempts": {
                                "attempt_number": attempt_number,
                                "http_status_code": http_status,
                                "success": success,
                                "timestamp": datetime.utcnow(),
                            }
                        },
                        "$set": {"status": "DELIVERED" if success else "RECEIVED"},
                    },
                )
            except errors.PyMongoError as e:
                logger.error("mongodb_update_failed", event_id=event_id, attempt=attempt_number, error=str(e))

            if success:
                deliveries_successful.inc()
                logger.info("delivery_successful", event_id=event_id, attempt=attempt_number, http_status=http_status)
                return
            else:
                deliveries_failed.inc()
                logger.warning("delivery_failed", event_id=event_id, attempt=attempt_number, http_status=http_status)

        except Exception as exc:
            deliveries_failed.inc()
            logger.exception("unexpected_delivery_exception", event_id=event_id, attempt=attempt_number, error=str(exc))

        # Retry with backoff
        if attempt < max_attempts - 1:
            retry_attempts.inc()
            sleep_seconds = backoff[attempt]
            logger.info("retry_scheduled", event_id=event_id, attempt=attempt_number, backoff_seconds=sleep_seconds)
            time.sleep(sleep_seconds)

    # Permanently failed
    try:
        events_collection.update_one({"_id": obj_id}, {"$set": {"status": "FAILED_PERMANENTLY"}})
    except errors.PyMongoError as e:
        logger.error("mongodb_update_failed_final", event_id=event_id, error=str(e))

    logger.error("delivery_failed_permanently", event_id=event_id, max_attempts=max_attempts)
