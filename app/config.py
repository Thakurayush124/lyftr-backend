import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

def config_ok() -> bool:
    return DATABASE_URL is not None
