import structlog
import uuid
from fastapi import Request

def bind_request_id(request: Request):
    request_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
    structlog.contextvars.bind_contextvars(request_id=request_id)
    return request_id
