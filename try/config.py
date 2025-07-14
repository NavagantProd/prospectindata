# config.py
import os
from pathlib import Path

# API Configuration
API_KEY = os.getenv("CORESIGNAL_API_KEY", "w5jfmFnwtLAPRWH5UcB6D23XWEIlPneI")
API_BASE_URL = "https://api.coresignal.com"

# Cache Configuration
CACHE_DIR = Path("cache")
CACHE_TTL_DAYS = 7

# Rate Limiting Configuration
MAX_CONCURRENT_REQUESTS = 5
REQUEST_DELAY = 2.0  # seconds between requests
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds

# Output Configuration
OUTPUT_DIR = Path("output")
BACKUP_DIR = Path("backup")

# File handling
MAX_FILE_LOCK_WAIT = 30  # seconds
