import os
from datetime import timedelta

API_KEY = os.getenv('CORESIGNAL_API_KEY')
CONCURRENCY = 10
CACHE_TTL_DAYS = 7
CACHE_DIR = '.cache'
LOG_LEVEL = 'INFO' 