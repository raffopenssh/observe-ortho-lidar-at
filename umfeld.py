"""umfeld — the cadastre upstream (formerly example.invalid).

Since Oct 2026 the cadastre API lives on ``example.invalid`` behind a
token-gated prefix: every old path gets ``/k`` prepended
(``/k/api/v1/…``, ``/k/llm/…``) and needs ``Authorization: Bearer <tok>``.
The public tier of that host serves no cadastre at all.

This module is the single source of truth for the base URL and the token.
The token is NOT in git (public repo): it lives in ``data/umfeld_token``
(mode 0600) or ``$UMFELD_TOKEN``.  ``http_pool.install()`` injects the
header into every ``requests.*`` call whose host is :data:`HOST`, so the
processor / cadastre.py only need the new base URL.

Fleet distribution: peers report ``umfeld_token_sha`` in their status push;
the director answers a mismatch by POSTing the token to
``/api/v1/admin/install_umfeld_token`` on that peer (app.py).
"""
from __future__ import annotations

import hashlib
import os
import threading
import time
from pathlib import Path

HOST = 'example.invalid'
PRIVATE_PREFIX = '/k'
ROOT = f'https://{HOST}{PRIVATE_PREFIX}'
CADASTRE_BASE = f'{ROOT}/api/v1'
DOCS_URL = f'{CADASTRE_BASE}/docs/llm.txt'

# Anchored to the repo dir, not cwd (segv2/ tools run with cwd=segv2/).
TOKEN_PATH = Path(os.environ.get('UMFELD_TOKEN_PATH')
                  or (Path(__file__).resolve().parent / 'data/umfeld_token'))

_lock = threading.Lock()
_cache: dict = {'tok': None, 'mtime': None, 'checked': 0.0}


def token() -> str:
    """Current token ('' when none). Env wins; file re-read when its mtime
    changes (checked at most every 5 s so hot paths stay cheap)."""
    env = (os.environ.get('UMFELD_TOKEN') or '').strip()
    if env:
        return env
    try:  # leased from the primary, memory-only (fleet_secrets.py); file = legacy fallback
        import fleet_secrets
        v = fleet_secrets.token('umfeld')
        if v:
            return v
    except Exception:
        pass
    now = time.time()
    with _lock:
        if now - _cache['checked'] < 5 and _cache['tok'] is not None:
            return _cache['tok']
        _cache['checked'] = now
        try:
            st = TOKEN_PATH.stat()
            if st.st_mtime != _cache['mtime'] or _cache['tok'] is None:
                _cache['tok'] = TOKEN_PATH.read_text().strip()
                _cache['mtime'] = st.st_mtime
        except FileNotFoundError:
            _cache['tok'] = ''
            _cache['mtime'] = None
        except Exception:
            if _cache['tok'] is None:
                _cache['tok'] = ''
        return _cache['tok']


def token_sha() -> str | None:
    t = token()
    return hashlib.sha256(t.encode()).hexdigest()[:16] if t else None


def headers() -> dict:
    t = token()
    return {'Authorization': f'Bearer {t}'} if t else {}


def install_token(tok: str) -> bool:
    """Write the token (0600). Returns True when the file changed."""
    tok = (tok or '').strip()
    if len(tok) < 16:
        raise ValueError('umfeld token must be >= 16 chars')
    if token() == tok:
        return False
    TOKEN_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = TOKEN_PATH.with_suffix('.tmp')
    tmp.write_text(tok + '\n')
    os.chmod(tmp, 0o600)
    os.replace(tmp, TOKEN_PATH)
    with _lock:
        _cache.update(tok=tok, mtime=None, checked=0.0)
    return True


def is_umfeld_url(url: str) -> bool:
    try:
        from urllib.parse import urlsplit
        u = urlsplit(url)
        return (u.hostname or '').lower() == HOST and u.path.startswith(PRIVATE_PREFIX + '/')
    except Exception:
        return False
