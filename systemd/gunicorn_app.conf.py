# gunicorn settings for app.py (operator API + director).  Bind address and
# port come from config.py (SRTM_APP_BIND / SRTM_APP_PORT, .env honoured).
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config as _cfg  # noqa: E402  (not "config": gunicorn treats module globals as settings)

bind = f"{_cfg.APP_BIND}:{_cfg.APP_PORT}"
workers = _cfg.env_int("SRTM_APP_WORKERS", 2)
threads = _cfg.env_int("SRTM_APP_THREADS", 4)
timeout = 600
chdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
del os, sys, _cfg  # leave only gunicorn settings in the module namespace
raw_env = [
    "GDAL_HTTP_MERGE_CONSECUTIVE_RANGES=YES",
    "GDAL_HTTP_MULTIPLEX=YES",
    "VSI_CACHE=TRUE",
    "VSI_CACHE_SIZE=134217728",
]
