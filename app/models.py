import sqlite3
import logging
import os
from contextlib import contextmanager
from app.config import DATABASE_URL

logger = logging.getLogger(__name__)

def get_connection():
    """Get a SQLite database connection."""
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL environment variable is not set")
    
    try:
        # Parse SQLite URL and handle both Unix and Windows paths
        # sqlite:////data/app.db → /data/app.db (Unix)
        # sqlite:///C:/path/app.db → C:/path/app.db (Windows)
        path = DATABASE_URL.replace("sqlite:///", "")
        
        # Create directory if it doesn't exist
        db_dir = os.path.dirname(path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)
            logger.info(f"Created database directory: {db_dir}")
        
        conn = sqlite3.connect(path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error as e:
        logger.error(f"Failed to connect to database at {DATABASE_URL}: {e}")
        raise

@contextmanager
def get_db_context():
    """Context manager for database connections."""
    conn = None
    try:
        conn = get_connection()
        yield conn
    except Exception as e:
        logger.error(f"Database operation failed: {e}")
        raise
    finally:
        if conn:
            conn.close()

def init_db():
    """Initialize the database schema."""
    try:
        with get_db_context() as conn:
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
            logger.info("Database initialized successfully")
    except sqlite3.Error as e:
        logger.error(f"Failed to initialize database: {e}")
        raise

def get_messages(limit: int = 50, offset: int = 0, from_msisdn: str = None, 
                 since: str = None, q: str = None):
    """Fetch messages with filters and pagination."""
    try:
        with get_db_context() as conn:
            cursor = conn.cursor()
            
            # Build base query
            where_clauses = []
            params = []
            
            if from_msisdn:
                where_clauses.append("from_msisdn = ?")
                params.append(from_msisdn)
            
            if since:
                where_clauses.append("ts >= ?")
                params.append(since)
            
            if q:
                where_clauses.append("text LIKE ?")
                params.append(f"%{q}%")
            
            where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"
            
            # Get total count
            count_query = f"SELECT COUNT(*) as total FROM messages WHERE {where_clause}"
            cursor.execute(count_query, params)
            total = cursor.fetchone()["total"]
            
            # Get paginated results
            query = f"""
                SELECT message_id, from_msisdn, to_msisdn, ts, text 
                FROM messages 
                WHERE {where_clause}
                ORDER BY ts ASC, message_id ASC
                LIMIT ? OFFSET ?
            """
            cursor.execute(query, params + [limit, offset])
            rows = cursor.fetchall()
            
            data = [
                {
                    "message_id": row["message_id"],
                    "from": row["from_msisdn"],
                    "to": row["to_msisdn"],
                    "ts": row["ts"],
                    "text": row["text"]
                }
                for row in rows
            ]
            
            return {
                "data": data,
                "total": total,
                "limit": limit,
                "offset": offset
            }
    except sqlite3.Error as e:
        logger.error(f"Failed to fetch messages: {e}")
        raise

def get_stats():
    """Fetch message statistics."""
    try:
        with get_db_context() as conn:
            cursor = conn.cursor()
            
            # Total messages
            cursor.execute("SELECT COUNT(*) as total FROM messages")
            total = cursor.fetchone()["total"]
            
            # Messages by from_msisdn
            cursor.execute("""
                SELECT from_msisdn, COUNT(*) as count 
                FROM messages 
                GROUP BY from_msisdn
            """)
            by_sender_rows = cursor.fetchall()
            by_sender = {row["from_msisdn"]: row["count"] for row in by_sender_rows}
            senders_count = len(by_sender)
            
            # Aggregate messages per sender
            cursor.execute("""
                SELECT COUNT(DISTINCT from_msisdn) as unique_senders
                FROM messages
            """)
            unique_senders = cursor.fetchone()["unique_senders"]
            
            messages_per_sender = total / unique_senders if unique_senders > 0 else 0
            
            # First and last message timestamps
            cursor.execute("SELECT MIN(ts) as first_ts FROM messages")
            first_ts_row = cursor.fetchone()
            first_message_ts = first_ts_row["first_ts"] if first_ts_row["first_ts"] else None
            
            cursor.execute("SELECT MAX(ts) as last_ts FROM messages")
            last_ts_row = cursor.fetchone()
            last_message_ts = last_ts_row["last_ts"] if last_ts_row["last_ts"] else None
            
            return {
                "total_messages": total,
                "senders_count": senders_count,
                "messages_per_sender": messages_per_sender,
                "first_message_ts": first_message_ts,
                "last_message_ts": last_message_ts
            }
    except sqlite3.Error as e:
        logger.error(f"Failed to fetch stats: {e}")
        raise
