"""
Symbio Data Engine - Configuration
==================================
Central configuration management with environment variable support.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# ============================================
# PATHS
# ============================================
BASE_DIR = Path(__file__).parent.resolve()
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
EXPORTS_DIR = BASE_DIR / "exports"

# Create directories if they don't exist
for directory in [DATA_DIR, RAW_DIR, PROCESSED_DIR, EXPORTS_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# ============================================
# DATABASE - PostgreSQL
# ============================================
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "symbio_data")
POSTGRES_USER = os.getenv("POSTGRES_USER", "symbio")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "symbio_secure_password")

POSTGRES_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# ============================================
# DATABASE - ChromaDB (Vector Store)
# ============================================
CHROMA_HOST = os.getenv("CHROMA_HOST", "localhost")
CHROMA_PORT = int(os.getenv("CHROMA_PORT", "8000"))
CHROMA_PERSIST_DIR = DATA_DIR / "chroma"

# ============================================
# SPIDER CONFIGURATION
# ============================================
# Rate limiting (requests per second)
SPIDER_RATE_LIMIT = float(os.getenv("SPIDER_RATE_LIMIT", "1.0"))

# User agents for rotation
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

# Wayback Machine
WAYBACK_API_URL = "https://archive.org/wayback/available"
WAYBACK_CDX_URL = "https://web.archive.org/cdx/search/cdx"

# Target domains for historical crawling
TARGET_DOMAINS = {
    "kalundborg": "symbiosis.dk",
    "epa_tri": "epa.gov/tri",
    "eprtr": "prtr.eea.europa.eu",
    "borouge": "borouge.com",
    "adnoc": "adnoc.ae",
}

# ============================================
# PROCESSOR CONFIGURATION
# ============================================
# OCR settings
TESSERACT_CMD = os.getenv("TESSERACT_CMD", r"C:\Program Files\Tesseract-OCR\tesseract.exe")

# Extraction confidence threshold
MIN_EXTRACTION_CONFIDENCE = float(os.getenv("MIN_EXTRACTION_CONFIDENCE", "0.7"))

# ============================================
# LLM / AI CONFIGURATION
# ============================================
# For structured extraction (can be OpenAI, local, or other)
LLM_API_KEY = os.getenv("LLM_API_KEY", "")
LLM_MODEL = os.getenv("LLM_MODEL", "gpt-4o-mini")
LLM_BASE_URL = os.getenv("LLM_BASE_URL", "https://api.openai.com/v1")

# Embedding model for ChromaDB
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "all-MiniLM-L6-v2")

# ============================================
# EXPORT CONFIGURATION
# ============================================
EXPORT_FORMATS = ["jsonl", "parquet", "csv"]
DEFAULT_EXPORT_FORMAT = "jsonl"

# ============================================
# LOGGING
# ============================================
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)
