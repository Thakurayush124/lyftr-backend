import os

DATABASE_URL = os.getenv("DATABASE_URL")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")

if not WEBHOOK_SECRET:
    # App can start, but readiness must fail
    WEBHOOK_SECRET = None
