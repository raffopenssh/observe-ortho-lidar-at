"""BEV read lane — **direct only** (proxy pool removed 2026-10-23).

History: until 2026-10-23 this module scraped ~30 public "free proxy"
aggregator lists (GitHub raw files, proxyscrape, …), validated up to
5 000 random third-party hosts per peer every 30 min (HTTPS CONNECT to
httpbin.org + BEV range reads) and routed BEV GeoTIFF reads through the
survivors via ``GDAL_HTTP_PROXY``.  Free open proxies are, to a large
extent, compromised machines / botnet nodes, so every peer regularly
opened connections to IPs on command-and-control block-lists.  That is
what tripped the network-security alert; it was never needed for
throughput (BEV direct egress works; the pool was "insurance").

The module keeps its public API so callers (``bev_retry``,
``austria_processor``, ``app.py`` fleet_proxy summary) need no change,
but it never fetches a list, never opens a third-party connection and
``next_proxy()`` always returns ``None`` (= direct).

**Do not reintroduce proxy lists.**  If BEV ever rate-limits a peer IP
again, the right tools are the director's `/24` outage parks
(``docs/fleet-bandwidth.md``) and a *paid, contractually known* egress —
never anonymous third-party hosts.
"""
import logging
import time

log = logging.getLogger(__name__)

PROXIES_REMOVED = True

# Still used by /api/v1/bev_probe (direct range probe of a known BEV tile).
_BEV_TEST_URL = "https://data.bev.gv.at/download/ALS/DTM/20220915/ALS_DTM_CRS3035RES50000mN2800000E4750000.tif"
_TIFF_MAGIC_LE = (b"II*\x00", b"II+\x00")

DIRECT_WEIGHT = 1
REFRESH_INTERVAL = 0

_started_at = time.time()


def _ensure_refresh_started() -> None:
    """No-op: there is no pool to refresh."""
    return None


def _refresh_pool() -> None:
    """No-op (kept for ``austria_processor``'s forced-refresh path)."""
    log.info("bev_proxy: proxy pool removed — direct BEV reads only")


def next_proxy() -> str | None:
    """Always direct."""
    return None


def report_failure(proxy_url: str | None, error_msg: str = "") -> None:
    return None


def report_success(proxy_url: str | None) -> None:
    return None


def status() -> dict:
    return {
        "proxies_removed": True,
        "total": 1, "healthy": 1, "cooling_down": 0,
        "direct": 1, "proxies": 0,
        "entries": [{"proxy": "direct", "status": "healthy", "fail_score": 0}],
        "last_refresh": {},
    }


def summary() -> dict:
    """Slim snapshot shipped on every peer_status push (fleet_proxy line)."""
    return {
        "total": 1, "healthy": 1, "cooling_down": 0,
        "direct": 1, "proxies": 0,
        "proxies_removed": True,
        "last_refresh": {"ts": _started_at, "phase1": 0, "phase2": 0,
                         "verified": 0, "raw": 0, "wall_s": 0.0},
    }
