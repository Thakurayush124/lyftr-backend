import sqlite3
from app.config import DATABASE_URL

def get_connection():
    # sqlite:////data/app.db → /data/app.db
    path = DATABASE_URL.replace("sqlite:///", "")
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS messages (
        message_id TEXT PRIMARY KEY,
        from_msisdn TEXT NOT NULL,
        to_msisdn TEXT NOT NULL,
        ts TEXT NOT NULL,
        text TEXT,
        created_at TEXT NOT NULL
    );
    """)

    conn.commit()
    conn.close()
