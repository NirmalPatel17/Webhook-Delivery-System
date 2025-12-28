import httpx
from dotenv import load_dotenv
import os
import hmac, hashlib
import json
from pymongo import MongoClient
import uuid

load_dotenv()
SECERET_KEY = os.getenv("SECRET_KEY").encode()
MONGODB_URL = os.getenv("MONGODB_URL")

mongo_client = MongoClient(MONGODB_URL)
db = mongo_client.webhooks
events_collection = db.events


def test_webhook_ingest():
    uid = str(uuid.uuid4())
    payload = {"idempotency_key": uid, "event_type": "order_created", "order_id": 123}
    signature = hmac.new(
        SECERET_KEY, json.dumps(payload).encode(), hashlib.sha256
    ).hexdigest()

    response = httpx.post(
        "http://webhook-service:8000/webhooks/ingest",
        json=payload,
        headers={"X-Signature": signature},
    )
    assert response.status_code == 200
    event_id1 = response.json()[0]["event_id"]

    # check duplication
    response2 = httpx.post(
        "http://webhook-service:8000/webhooks/ingest",
        json=payload,
        headers={"X-Signature": signature},
    )
    assert response2.status_code == 200
    event_id2 = response2.json()[0]["event_id"]

    assert event_id1 == event_id2

