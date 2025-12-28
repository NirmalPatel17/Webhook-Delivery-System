import httpx
from dotenv import load_dotenv
import os
import hmac, hashlib
import json

load_dotenv()
SECERET_KEY = os.getenv("SECRET_KEY").encode()


def test_webhook_ingest():
    payload = {"event_type": "order_created", "order_id": 123}
    signature = hmac.new(SECERET_KEY, json.dumps(payload).encode(), hashlib.sha256).hexdigest()
    
    response = httpx.post(
        "http://webhook-service:8000/webhooks/ingest",
        json=payload,
        headers={"X-Signature": signature}
    )

    assert response.status_code == 200
    assert response.json()[0]["status"] == "received"


def test_webhook_ingest_betch():
    payload_batch = [{"event_type": "order_created", "order_id": 123}, {"event_type": "order_created", "order_id": 456}]
    signature = hmac.new(SECERET_KEY, json.dumps(payload_batch).encode(), hashlib.sha256).hexdigest()
    
    response = httpx.post(
        "http://webhook-service:8000/webhooks/ingest",
        json=payload_batch,
        headers={"X-Signature": signature}
    )

    assert response.status_code == 200
    
    for item in response.json():
        assert item["status"] == "received"

