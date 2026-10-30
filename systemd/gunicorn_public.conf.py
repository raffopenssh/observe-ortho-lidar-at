# gunicorn settings for public_api.py (read-only public tier).  Bind address
# and port come from config.py (SRTM_PUBLIC_BIND / SRTM_PUBLIC_PORT).
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config as _cfg  # noqa: E402  (not "config": gunicorn treats module globals as settings)

bind = f"{_cfg.PUBLIC_BIND}:{_cfg.PUBLIC_PORT}"
workers = _cfg.env_int("SRTM_PUBLIC_WORKERS", 2)
threads = _cfg.env_int("SRTM_PUBLIC_THREADS", 24)
timeout = 620
graceful_timeout = 60
keepalive = 15
# recycle workers periodically (slow-leak insurance; service_guard.py handles
# the fast case with its in-process memory watchdog)
max_requests = 20000
max_requests_jitter = 2000
chdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
del os, sys, _cfg  # leave only gunicorn settings in the module namespace
