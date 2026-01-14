import sqlite3
import logging
from datetime import datetime
from app.models import get_db_context

logger = logging.getLogger(__name__)

def insert_message(msg):
    """Insert a message into the database."""
    try:
        with get_db_context() as conn:
            cursor = conn.cursor()

            cursor.execute("""
            INSERT INTO messages (
                message_id, from_msisdn, to_msisdn, ts, text, created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """, (
                msg["message_id"],
                msg["from"],
                msg["to"],
                msg["ts"],
                msg.get("text"),
                datetime.utcnow().isoformat() + "Z"
            ))
            conn.commit()
            logger.info(f"Message {msg['message_id']} inserted successfully")
            return "created"
    except sqlite3.IntegrityError:
        logger.warning(f"Duplicate message: {msg['message_id']}")
        return "duplicate"
    except Exception as e:
        logger.error(f"Failed to insert message {msg.get('message_id')}: {e}", exc_info=True)
        raise
