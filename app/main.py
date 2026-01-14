# =====================
# Imports
# =====================
import time
import uuid
import hmac
import hashlib
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field

from prometheus_client import generate_latest

from app.config import WEBHOOK_SECRET, DATABASE_URL
from app.models import init_db
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

    body = await request.body()
    signature = request.headers.get("X-Signature")

    if not signature or not verify_signature(WEBHOOK_SECRET, body, signature):
        webhook_requests_total.labels("invalid_signature").inc()
        raise HTTPException(status_code=401, detail="invalid signature")

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


# =====================
# Metrics Endpoint
# =====================
@app.get("/metrics")
def metrics():
    return PlainTextResponse(generate_latest())
