import os
import httpx
from datetime import datetime, timedelta


SEARCH_URL = f"http://webhook-service:8000/webhooks/search"


def assert_histogram_format(hist):
    assert isinstance(hist, list)
    for h in hist:
        assert "count" in h
        assert "_id" in h
        # format: YYYY-MM-DD HH:00
        datetime.strptime(h["_id"], "%Y-%m-%d %H:00")


def test_search_without_filters():
    payload = {
        "skip": 0,
        "limit": 10
    }

    r = httpx.post(SEARCH_URL, json=payload)
    assert r.status_code == 200

    body = r.json()
    assert "data" in body
    assert "summary" in body
    assert "status_counts" in body["summary"]
    assert "event_type_counts" in body["summary"]
    assert "hourly_histogram" in body["summary"]


def test_search_by_status():
    payload = {
        "status": "RECEIVED",
        "skip": 0,
        "limit": 10
    }

    r = httpx.post(SEARCH_URL, json=payload)
    assert r.status_code == 200

    data = r.json()["data"]
    for event in data:
        assert event["status"] == "RECEIVED"


def test_search_by_event_type():
    payload = {
        "event_type": "order.created",
        "skip": 0,
        "limit": 10
    }

    r = httpx.post(SEARCH_URL, json=payload)
    assert r.status_code == 200

    data = r.json()["data"]
    for event in data:
        assert event["event_type"] == "order.created"


def test_search_by_time_range():
    now = datetime.utcnow()
    payload = {
        "from_timestamp": (now - timedelta(days=1)).isoformat(),
        "to_timestamp": now.isoformat(),
        "skip": 0,
        "limit": 10
    }

    r = httpx.post(SEARCH_URL, json=payload)
    assert r.status_code == 200

    data = r.json()["data"]
    for event in data:
        assert payload["from_timestamp"] <= event["received_at"] <= payload["to_timestamp"]


def test_pagination():
    payload_1 = {
        "skip": 0,
        "limit": 2
    }
    payload_2 = {
        "skip": 2,
        "limit": 2
    }

    r1 = httpx.post(SEARCH_URL, json=payload_1).json()["data"]
    r2 = httpx.post(SEARCH_URL, json=payload_2).json()["data"]

    assert r1 != r2


def test_summary_aggregations():
    payload = {
        "skip": 0,
        "limit": 5
    }

    r = httpx.post(SEARCH_URL, json=payload)
    assert r.status_code == 200

    summary = r.json()["summary"]

    assert isinstance(summary["status_counts"], dict)
    assert isinstance(summary["event_type_counts"], dict)
    assert_histogram_format(summary["hourly_histogram"])
