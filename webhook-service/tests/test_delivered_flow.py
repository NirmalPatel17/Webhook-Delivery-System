# tests/integration/test_ingest_flow.py
import time
from bson import ObjectId
import httpx
from dotenv import load_dotenv
import os
import hmac, hashlib
import json
from pymongo import MongoClient

load_dotenv()
SECERET_KEY = os.getenv("SECRET_KEY").encode()
MONGODB_URL = os.getenv("MONGODB_URL")

mongo_client = MongoClient(MONGODB_URL)
db = mongo_client.webhooks
events_collection = db.events


def test_webhook_ingest():
    payload = {"event_type": "order_created", "order_id": 123}
    signature = hmac.new(SECERET_KEY, json.dumps(payload).encode(), hashlib.sha256).hexdigest()
    
    response = httpx.post(
        "http://webhook-service:8000/webhooks/ingest",
        json=payload,
        headers={"X-Signature": signature}
    )

    assert response.status_code == 200
    time.sleep(10) 
    event_id = response.json()[0]["event_id"]
    assert events_collection.find_one({"_id": ObjectId(event_id)})['status'] == "DELIVERED"


def test_webhook_ingest_betch():
    payload_batch = [{"event_type": "order_created", "order_id": 123}, {"event_type": "order_created", "order_id": 456}]
    signature = hmac.new(SECERET_KEY, json.dumps(payload_batch).encode(), hashlib.sha256).hexdigest()
    
    response = httpx.post(
        "http://webhook-service:8000/webhooks/ingest",
        json=payload_batch,
        headers={"X-Signature": signature}
    )

    assert response.status_code == 200
    time.sleep(30)
    for item in response.json():
        event_id = item["event_id"]
        assert events_collection.find_one({"_id": ObjectId(event_id)})['status'] == "DELIVERED"


