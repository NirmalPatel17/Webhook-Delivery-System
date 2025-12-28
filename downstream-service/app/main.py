import time
import random
import asyncio
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
import structlog
from redis.exceptions import RedisError

from app.logger import configure_logging
from app.redis_client import redis_client, RATE_LIMIT_SHA

# -------------------------------------------------------------------
# App setup
# -------------------------------------------------------------------

app = FastAPI(title="Mock Downstream Service")

RATE_LIMIT = 3
WINDOW = 1  # seconds
FAILURE_RATE = 0.20

configure_logging()
logger = structlog.get_logger("downstream-service")

# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------


def get_client_ip(request: Request) -> str:
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def allow_request(ip: str) -> bool:
    """
    Distributed per-IP rate limiter using Redis + Lua
    Fixed window, fail-open
    """
    try:
        key = f"rate_limit:{ip}"

        allowed = redis_client.evalsha(
            RATE_LIMIT_SHA,
            1,
            key,
            RATE_LIMIT,
            WINDOW,
        )

        return allowed == 1

    except RedisError as exc:
        logger.error("redis_rate_limit_failed", ip=ip, error=str(exc))
        return True  # fail-open

# -------------------------------------------------------------------
# Health Check
# -------------------------------------------------------------------
@app.get("/health")
async def health():
    return {"status": "ok"}

# -------------------------------------------------------------------
# Route
# -------------------------------------------------------------------

@app.post("/downstream/receive")
async def receive_event(request: Request):
    try:
        client_ip = get_client_ip(request)

        # -------- Rate limiting --------
        if not allow_request(client_ip):
            logger.warning("rate_limited", ip=client_ip)
            raise HTTPException(
                status_code=429,
                detail=f"Rate limit exceeded for IP {client_ip}",
            )

        # -------- Failure injection --------
        failure_type = random.choices(
            ["500", "429", "timeout", "success"],
            weights=[
                FAILURE_RATE * 0.5,  # 500 errors
                FAILURE_RATE * 0.25,  # 429 errors
                FAILURE_RATE * 0.25,  # timeouts
                1 - FAILURE_RATE,  # success
            ],
            k=1,
        )[0]

        if failure_type == "500":
            logger.info("inject_500", ip=client_ip)
            raise HTTPException(status_code=500, detail="Simulated internal error")

        if failure_type == "429":
            logger.info("inject_429", ip=client_ip)
            raise HTTPException(status_code=429, detail="Simulated external rate limit")

        if failure_type == "timeout":
            delay = random.uniform(2.0, 5.0)
            logger.info("simulate_timeout", ip=client_ip, delay=round(delay, 2))
            await asyncio.sleep(delay)

            return JSONResponse(
                status_code=200,
                content={
                    "status": "received_with_delay",
                    "ip": client_ip,
                    "delay_sec": round(delay, 2),
                    "timestamp": int(time.time()),
                },
            )

        # -------- Success --------
        logger.info("received_successfully", ip=client_ip)
        return JSONResponse(
            status_code=200,
            content={
                "status": "received",
                "ip": client_ip,
                "timestamp": int(time.time()),
            },
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("unexpected_error", ip=client_ip, error=str(exc))
        raise HTTPException(status_code=500, detail="Internal server error") from exc
