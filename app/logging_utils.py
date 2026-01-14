import json
import uuid
import time
from datetime import datetime
import logging

logger = logging.getLogger("app")

def log_request(**kwargs):
    log = {
        "ts": datetime.utcnow().isoformat() + "Z",
        "request_id": kwargs.get("request_id"),
        "level": kwargs.get("level", "INFO"),
        "method": kwargs.get("method"),
        "path": kwargs.get("path"),
        "status": kwargs.get("status"),
        "latency_ms": kwargs.get("latency_ms"),
    }
    log.update(kwargs.get("extra", {}))
    logger.info(json.dumps(log))
