# gunicorn settings for app.py (operator API + director).  Bind address and
# port come from config.py (SRTM_APP_BIND / SRTM_APP_PORT, .env honoured).
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: E402

bind = f"{config.APP_BIND}:{config.APP_PORT}"
workers = config.env_int("SRTM_APP_WORKERS", 2)
threads = config.env_int("SRTM_APP_THREADS", 4)
timeout = 600
chdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
raw_env = [
    "GDAL_HTTP_MERGE_CONSECUTIVE_RANGES=YES",
    "GDAL_HTTP_MULTIPLEX=YES",
    "VSI_CACHE=TRUE",
    "VSI_CACHE_SIZE=134217728",
]
