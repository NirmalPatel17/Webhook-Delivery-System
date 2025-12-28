# tests/unit/test_hmac.py
import hmac
import hashlib
from app.main import verify_hmac
from dotenv import load_dotenv
import os

load_dotenv()
SECERET_KEY = os.getenv("SECRET_KEY").encode()


def test_valid_hmac():
    body = b'{"event":"order_created"}'
    signature = hmac.new(SECERET_KEY, body, hashlib.sha256).hexdigest()
    assert verify_hmac(body, signature) is True


def test_invalid_hmac():
    body = b'{"event":"order_created"}'
    assert verify_hmac(body, "invalid sign") is False
