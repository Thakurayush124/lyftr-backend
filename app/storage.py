import sqlite3
from datetime import datetime
from app.models import get_connection

def insert_message(msg):
    conn = get_connection()
    cursor = conn.cursor()

    try:
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
        return "created"
    except sqlite3.IntegrityError:
        return "duplicate"
    finally:
        conn.close()
