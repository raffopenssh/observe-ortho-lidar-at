"""Central runtime configuration for the public single-VM deployment.

Every value is read from the environment (optionally via a ``.env`` file in
the repository root — see ``.env.example``).  Nothing here is required: an
empty value means "feature disabled" and the code paths that would need it
must degrade gracefully (no network call, a clear log line, a ``None``
return).  Never put hostnames, tokens or peer lists into source files —
import the accessor you need from this module instead.

Usage::

    import config
    if config.POWER_API_URL:
        ...                       # feature available
    peers = config.peers()        # [] in single-VM mode
    tok = config.zenodo_token()   # '' when uploads are disabled
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import List

ROOT = Path(__file__).resolve().parent


def _load_dotenv(path: Path = ROOT / ".env") -> None:
    """Minimal ``.env`` loader (KEY=VALUE, ``#`` comments, no interpolation).
    Existing process-environment values always win."""
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k, v = k.strip(), v.split("#", 1)[0].strip().strip("'\"")
        if k and k not in os.environ:
            os.environ[k] = v


_load_dotenv()


def env(name: str, default: str = "") -> str:
    return (os.environ.get(name) or default).strip()


def env_bool(name: str, default: bool = False) -> bool:
    v = env(name)
    if not v:
        return default
    return v.lower() in ("1", "true", "yes", "on")


def env_int(name: str, default: int) -> int:
    try:
        return int(env(name) or default)
    except ValueError:
        return default


def _url(name: str) -> str:
    return env(name).rstrip("/")


# --- Where the data lives -------------------------------------------------
DATA_DIR = Path(env("SRTM_DATA_DIR") or (ROOT / "data"))
PROCESSOR_DIR = DATA_DIR / "austria_processor"
# Local directory outside the repo for operator secrets (tokens etc.).
SECRETS_DIR = Path(env("SRTM_SECRETS_DIR") or (Path.home() / ".config" / "srtm-lidar"))

# --- Ports / bind addresses ------------------------------------------------
APP_PORT = env_int("SRTM_APP_PORT", 8000)          # app.py (operator API + processor/director)
PUBLIC_PORT = env_int("SRTM_PUBLIC_PORT", 8100)    # public_api.py (read-only public tier)
APP_BIND = env("SRTM_APP_BIND", "127.0.0.1")       # operator tier is loopback-only by default
PUBLIC_BIND = env("SRTM_PUBLIC_BIND", "0.0.0.0")

# URL the public tier / processor use to reach app.py on the same host.
APP_URL = _url("SRTM_APP_URL") or f"http://127.0.0.1:{APP_PORT}"

# --- Multi-VM (optional) ----------------------------------------------------
# Base URL of the director instance.  Empty ⇒ this instance is the only
# one (single-VM mode): the director runs locally and never contacts peers.
PRIMARY_URL = _url("SRTM_PRIMARY_URL")
# Public base URL of this instance (used in links / share URLs); optional.
SELF_URL = _url("SRTM_SELF_URL")


def peers() -> List[str]:
    """Comma-separated ``SRTM_PEERS`` → list of base URLs.  ``[]`` ⇒ single-VM."""
    return [p.strip().rstrip("/") for p in env("SRTM_PEERS").split(",") if p.strip()]


def single_vm() -> bool:
    return not peers() and not PRIMARY_URL


def is_director() -> bool:
    """This instance runs the director (always true unless it is a worker
    pointed at a remote ``SRTM_PRIMARY_URL``)."""
    return not PRIMARY_URL


def self_id() -> str:
    """Identifier of this instance in a multi-VM roster (``SRTM_PEER_ID``);
    ``primary`` for the director, else the hostname."""
    v = env("SRTM_PEER_ID")
    if v:
        return v
    if is_director():
        return "primary"
    import socket
    return socket.gethostname().split(".")[0] or "peer"


# --- Optional upstream services (all disabled when empty) -------------------
POWER_API_URL = _url("POWER_API_URL")          # power-infrastructure lookup service
NE_SERVER_URL = _url("NE_SERVER_URL")                # NE declared-layer / contrib endpoint
INVEKOS_API_URL = _url("INVEKOS_API_URL")      # agricultural-use labels for segv2 training
ZENODO_LOCK_URL = _url("ZENODO_LOCK_URL")      # external Zenodo write mutex; empty ⇒ local lock

# --- Zenodo ----------------------------------------------------------------
ZENODO_BASE_URL = _url("ZENODO_BASE_URL") or "https://zenodo.org"
# Read-only public tile-cache / products deposit used by the fetchers.
ZENODO_CACHE_DEPOSIT = env_int("ZENODO_CACHE_DEPOSIT", 19650075)


def _file_secret(fname: str) -> str:
    try:
        return (SECRETS_DIR / fname).read_text(encoding="utf-8").strip()
    except OSError:
        return ""


def zenodo_token(required: bool = False) -> str:
    """``ZENODO_TOKEN`` env or ``$SRTM_SECRETS_DIR/zenodo_token``.  Empty ⇒
    uploads disabled (products are written to ``OUT_DIR`` instead)."""
    tok = env("ZENODO_TOKEN") or _file_secret("zenodo_token")
    if not tok and required:
        raise RuntimeError("ZENODO_TOKEN not configured — uploads disabled")
    return tok


def uploads_enabled() -> bool:
    return bool(zenodo_token()) and not env_bool("SRTM_DRY_RUN", False)


# Where products land when uploads are disabled / dry-run.
OUT_DIR = Path(env("SRTM_OUT_DIR") or (ROOT / "out"))

# Per-instance scratch state shared between gunicorn workers (async task
# progress / results, heavy-request slots, locks).  Scoped by uid and port so
# two instances on one host never see each other's files.
RUNTIME_DIR = Path(env("SRTM_RUNTIME_DIR")
                   or f"/tmp/srtm-lidar-{os.getuid()}-{APP_PORT}")

# --- Copernicus / openEO credentials ----------------------------------------
# Path to a JSON list of credential entries.  Default lives outside the repo.
COPERNICUS_CREDENTIALS_FILE = Path(
    env("COPERNICUS_CREDENTIALS_FILE") or (SECRETS_DIR / "copernicus_credentials.json"))


# --- Operator token for mutating routes -------------------------------------
# Mutating routes (/processing/*, /director/*, /admin/*, /credentials) accept
# requests from loopback without a token.  To allow them from elsewhere set
# ``SRTM_OPERATOR_TOKEN`` (or write ``$SRTM_SECRETS_DIR/operator_token``) and
# send it as ``X-Operator-Token``.
def operator_token() -> str:
    return env("SRTM_OPERATOR_TOKEN") or _file_secret("operator_token")


def operator_headers() -> dict:
    """``{'X-Operator-Token': …}`` when a token is configured, else ``{}``
    (loopback needs none).  For callers using their own ``requests.Session``;
    module-level ``requests.*`` toward ``PRIMARY_URL`` get it from http_pool."""
    tok = operator_token()
    return {"X-Operator-Token": tok} if tok else {}


def director_url() -> str:
    """Where processor-side helpers reach the director's API: ``PRIMARY_URL``
    in a multi-VM setup, else the local ``app.py`` (``APP_URL``)."""
    return PRIMARY_URL or APP_URL


# --- Feature flags ----------------------------------------------------------
# Building footprints for the segmentation model context, derived in-RAM
# from public BEV vector tiles (vct_pilot / ne_h3 bevdirect mode).  Off by
# default; nothing from it is ever written to disk or into a product.
BEV_VECTOR_CONTEXT = env_bool("SRTM_BEV_VECTOR_CONTEXT", False)

# Public /segmentation cap (km²) and other limits.
PUBLIC_MAX_AREA_KM2 = float(env("PUBLIC_MAX_AREA_KM") or 5.0)
SHARE_CAP_BYTES = env_int("SRTM_SHARE_CAP_MB", 1024) * 1024 * 1024

# --- Outbound allow-list (SSRF guard) ----------------------------------------
# Hosts the server may fetch from when a URL is derived from user input.
OUTBOUND_ALLOWED_HOSTS = tuple(h for h in (
    "data.bev.gv.at", "gis.bev.gv.at", "kataster.bev.gv.at",
    "openeo.dataspace.copernicus.eu", "identity.dataspace.copernicus.eu",
    "zenodo.org", "sandbox.zenodo.org",
    "storage.googleapis.com", "glad.umd.edu",
    "overpass-api.de", "overpass.kumi.systems",
    *[h.strip() for h in env("SRTM_EXTRA_ALLOWED_HOSTS").split(",")],
) if h)


def describe() -> dict:
    """Non-secret summary for /info and the dashboard."""
    return {
        "single_vm": single_vm(),
        "peers": len(peers()),
        "primary_url": bool(PRIMARY_URL),
        "uploads_enabled": uploads_enabled(),
        "power_api": bool(POWER_API_URL),
        "ne_server": bool(NE_SERVER_URL),
        "bev_vector_context": BEV_VECTOR_CONTEXT,
        "data_dir": str(DATA_DIR),
    }
