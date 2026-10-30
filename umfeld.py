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
# Public (tokenless) tier of the same host.  OSM geometry lives here since
# 2026-10-23: ``/k/api/v1/osm/geometry`` is gone (404) — the only /k-specific
# extra was ``?kg=<code>``; the public tier is bbox-keyed (``bbox=`` required,
# ``parcel=`` / ``cat=water_parcels`` removed on every tier).
PUBLIC_BASE = f'https://{HOST}/api/v1'
OSM_GEOMETRY_URL = f'{PUBLIC_BASE}/osm/geometry'
# NE cells (2026-10-24): our token is a CONTRIBUTOR token — every /k/… path
# answers 404 for it.  Writes go to the ``/contrib`` prefix
# (``POST /contrib/api/v1/ne/{kg}/report|chunks|obs``, ``GET …/ne/revisit``,
# ``GET …/ne/{kg}/report``, ``GET …/ne/manifest``); reads are the public,
# tokenless ``/api/v1/ne/…`` tier (``/ne/manifest``, ``/ne/{kg}?layers=lu``).
CONTRIB_PREFIX = '/contrib'
CONTRIB_BASE = f'https://{HOST}{CONTRIB_PREFIX}/api/v1'
NE_CONTRIB_BASE = f'{CONTRIB_BASE}/ne'
NE_PUBLIC_BASE = f'{PUBLIC_BASE}/ne'
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


# Parcel-keyed routes retired on umfeld-at (plain 404 since 2026-10-30; 410
# before): /api/v1/search/parcel, /query, /spatial/parcels|footprints|points|
# bbox|kgs, /osm/parcel, /natura2000/parcel, /land_prices/parcel,
# /legal/parcel, /kg/{kg}/parcels, /llm/*, /export/*, /search/*.  Replacements
# are point / bbox / cell / KG keyed: /api/v1/context?lon&lat,
# /api/v1/osm/geometry?bbox=&cat=, /api/v1/ne/*, /api/v1/kgs.  Cadastre
# geometry (parcels, footprints) comes from the local bevdirect-serve
# (``live_cadastre.py`` / ``austria_processor.fetch_cadastre_data_bevdirect``).
RETIRED_K_PATHS = ('/search/', '/query', '/spatial/', '/osm/parcel', '/natura2000/parcel',
                   '/land_prices/parcel', '/legal/parcel', '/export/', '/llm/')


class KCadastreRetired(RuntimeError):
    """A caller still wants umfeld's parcel-keyed ``/k`` cadastre routes."""


def assert_k_cadastre_available(what: str) -> None:
    """Legacy v1 offline tools (bulk_export, calibrate, train_rf_*) were
    built on ``/k/api/v1/export/geojson`` + ``/search/kg``.  Fail loudly up
    front instead of 404-ing per KG for hours.  The live harness
    (``segv2/labels.fetch_cadastre``) is already ported to bevdirect — copy
    that if one of these tools is ever needed again."""
    raise KCadastreRetired(
        f"{what}: umfeld-at's parcel-keyed /k cadastre routes (export/geojson, search/kg, "
        f"spatial/*) are gone (404). Parcel/footprint geometry now comes from the local "
        f"bevdirect-serve (live_cadastre.py, austria_processor.fetch_cadastre_data_bevdirect); "
        f"KG lists from the public {PUBLIC_BASE}/kgs. Port this tool before running it.")


def is_umfeld_url(url: str) -> bool:
    try:
        from urllib.parse import urlsplit
        u = urlsplit(url)
        return (u.hostname or '').lower() == HOST and (
            u.path.startswith(PRIVATE_PREFIX + '/') or u.path.startswith(CONTRIB_PREFIX + '/'))
    except Exception:
        return False
