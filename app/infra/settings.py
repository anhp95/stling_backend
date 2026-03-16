"""Application settings — single source of truth."""

import os
from dotenv import load_dotenv

load_dotenv()


OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
REDIS_URL = os.getenv(
    "REDIS_URL",
    "redis://default:JSQmeYwShbftAbbOsOWXxlfdtckFtRPh@redis.railway.internal:6379",
)
