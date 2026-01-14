# =====================
# Imports
# =====================
import time
import uuid
import hmac
import hashlib
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field

from prometheus_client import generate_latest

from app.config import WEBHOOK_SECRET, DATABASE_URL
from app.models import init_db, get_messages, get_stats
from app.storage import insert_message
from app.logging_utils import log_request
from app.metrics import webhook_requests_total

logger = logging.getLogger(__name__)


# ===================== 
# FastAPI App Lifespan
# =====================
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    try:
        logger.info("Initializing database...")
        init_db()
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}", exc_info=True)
        raise
    yield
    # Shutdown
    logger.info("Shutting down application")


# ===================== 
# FastAPI App
# =====================
app = FastAPI(lifespan=lifespan)


# =====================
# Health Endpoints
# =====================
@app.get("/health/live")
def live():
    return {"status": "alive"}


@app.get("/health/ready")
def ready():
    if not DATABASE_URL or not WEBHOOK_SECRET:
        raise HTTPException(status_code=503)
    return {"status": "ready"}



# =====================
# Pydantic Model
# =====================
class WebhookMessage(BaseModel):
    message_id: str = Field(..., min_length=1)
    from_: str = Field(..., alias="from", pattern=r"^\+\d+$")
    to: str = Field(..., pattern=r"^\+\d+$")
    ts: str
    text: str | None = Field(None, max_length=4096)



# =====================
# Signature Verification
# =====================
def verify_signature(secret: str, body: bytes, signature: str) -> bool:
    expected = hmac.new(
        secret.encode(),
        body,
        hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, signature)


# =====================
# Webhook Endpoint
# =====================
@app.post("/webhook")
async def webhook(request: Request):
    start = time.time()
    request_id = str(uuid.uuid4())

    if not WEBHOOK_SECRET:
        raise HTTPException(status_code=503, detail="Webhook secret not configured")

    body = await request.body()
    signature = request.headers.get("X-Signature")

    if not signature or not verify_signature(WEBHOOK_SECRET, body, signature):
        webhook_requests_total.labels("invalid_signature").inc()
        raise HTTPException(status_code=401, detail="invalid signature")

    try:
        data = await request.json()
        result = insert_message(data)

        webhook_requests_total.labels(result).inc()

        log_request(
            request_id=request_id,
            method="POST",
            path="/webhook",
            status=200,
            latency_ms=int((time.time() - start) * 1000),
            extra={
                "message_id": data.get("message_id"),
                "dup": result == "duplicate",
                "result": result
            }
        )

        return {"status": "ok"}
    except ValueError as e:
        logger.warning(f"Invalid JSON in webhook: {e}")
        raise HTTPException(status_code=400, detail="Invalid JSON")
    except Exception as e:
        logger.error(f"Webhook processing failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# =====================
# Metrics Endpoint
# =====================
@app.get("/metrics")
def metrics():
    return PlainTextResponse(generate_latest())


# =====================
# Messages Endpoint (GET)
# =====================
@app.get("/messages")
def list_messages(
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    from_param: str | None = Query(None, alias="from"),
    since: str | None = Query(None),
    q: str | None = Query(None)
):
    """
    List stored messages with pagination and filters.
    """
    try:
        result = get_messages(
            limit=limit,
            offset=offset,
            from_msisdn=from_param,
            since=since,
            q=q
        )
        return result
    except Exception as e:
        logger.error(f"Failed to list messages: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# =====================
# Stats Endpoint
# =====================
@app.get("/stats")
def stats():
    """
    Provide simple message-level analytics.
    
    Returns:
    - total_messages: Total count of messages
    - by_sender: Message count grouped by sender MSISDN
    - by_recipient: Message count grouped by recipient MSISDN
    """
    try:
        result = get_stats()
        return result
    except Exception as e:
        logger.error(f"Failed to fetch stats: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")
