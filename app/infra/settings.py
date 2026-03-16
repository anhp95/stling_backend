"""Application settings — single source of truth."""

import os
from dotenv import load_dotenv

load_dotenv()


OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
# Railway typically provides REDIS_URL or REDIS_PRIVATE_URL automatically.
REDIS_URL = os.getenv("REDIS_PRIVATE_URL") or os.getenv(
    "REDIS_URL", "redis://localhost:6379/0"
)
