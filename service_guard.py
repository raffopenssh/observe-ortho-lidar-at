"""service_guard — keeps the public tier answering under load.

Three independent mechanisms, all per gunicorn worker, all env-tunable:

* ``Gate`` — bounded concurrency for CPU-heavy endpoints (tile rendering).
  Callers wait at most ``max_wait_s`` for a slot, then get ``Overloaded`` →
  the view answers 503 + ``Retry-After`` in a millisecond instead of holding
  a worker thread.  Without this, a tile flood pins all 24 threads in numpy
  and the cheap endpoints (status, peer heartbeats) starve.

* ``RateLimiter`` — per-client token bucket (LAST ``X-Forwarded-For`` hop —
  the one the exe.dev proxy appends; earlier hops are client-supplied).  429 + ``Retry-After`` + ``X-RateLimit-*``
  headers so well-behaved clients back off instead of retrying blindly.

* ``MemoryWatchdog`` — the Oct 2026 outage: both workers crossed the cgroup
  ``MemoryHigh`` and were reclaim-throttled into D state for hours while the
  listen backlog filled.  The watchdog samples RSS every few seconds from the
  request path; above ``soft_mb`` it sheds every ``mem_lru`` cache and GCs,
  above ``hard_mb`` it asks gunicorn to recycle this worker (SIGTERM to self
  → graceful drain, the arbiter re-spawns a fresh one).  Thresholds default
  to well under the unit's ``MemoryHigh`` so throttling can never start.
"""
from __future__ import annotations

import gc
import logging
import os
import signal
import threading
import time

log = logging.getLogger(__name__)


def _env_f(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except ValueError:
        return default


def rss_mb() -> float:
    try:
        with open('/proc/self/statm') as fh:
            return int(fh.read().split()[1]) * os.sysconf('SC_PAGE_SIZE') / 2 ** 20
    except Exception:  # noqa: BLE001
        return 0.0


def cgroup_memory() -> dict:
    """current/high/max of our cgroup + PSI 'some avg10' — the throttling evidence."""
    out = {}
    try:
        with open('/proc/self/cgroup') as fh:
            cg = fh.read().strip().split(':', 2)[-1]
        base = '/sys/fs/cgroup' + cg
        for k in ('current', 'high', 'max'):
            with open(f'{base}/memory.{k}') as fh:
                v = fh.read().strip()
            out[k + '_mb'] = None if v == 'max' else round(int(v) / 2 ** 20)
        with open(f'{base}/memory.pressure') as fh:
            some = fh.readline().split()
        out['pressure_some_avg10'] = float(some[1].split('=')[1])
    except Exception:  # noqa: BLE001
        pass
    return out


# --------------------------------------------------------------------------
class Overloaded(Exception):
    def __init__(self, retry_after: int = 2, kind: str = 'busy'):
        super().__init__(kind)
        self.retry_after = retry_after
        self.kind = kind


class Gate:
    """Bounded-concurrency semaphore with a bounded queue wait."""

    def __init__(self, name: str, max_concurrent: int, max_wait_s: float, env: str | None = None):
        if env:
            max_concurrent = int(_env_f(env, max_concurrent))
            max_wait_s = _env_f(env + '_WAIT_S', max_wait_s)
        self.name = name
        self.max_concurrent = max(1, max_concurrent)
        self.max_wait_s = max_wait_s
        self._sem = threading.BoundedSemaphore(self.max_concurrent)
        self._lock = threading.Lock()
        self.active = 0
        self.waiting = 0
        self.rejected = 0
        self.served = 0
        self.wait_total = 0.0

    def __enter__(self):
        t0 = time.monotonic()
        with self._lock:
            self.waiting += 1
        ok = self._sem.acquire(timeout=self.max_wait_s)
        with self._lock:
            self.waiting -= 1
            if not ok:
                self.rejected += 1
            else:
                self.active += 1
                self.served += 1
                self.wait_total += time.monotonic() - t0
        if not ok:
            raise Overloaded(retry_after=2, kind='render_gate')
        return self

    def __exit__(self, *exc):
        with self._lock:
            self.active -= 1
        self._sem.release()
        return False

    def stats(self) -> dict:
        return {'max': self.max_concurrent, 'active': self.active, 'waiting': self.waiting,
                'served': self.served, 'rejected': self.rejected,
                'avg_wait_ms': round(1000 * self.wait_total / self.served, 1) if self.served else 0.0}


# --------------------------------------------------------------------------
class RateLimiter:
    """Token bucket per client key.  ``rate`` tokens/s, ``burst`` capacity."""

    def __init__(self, name: str, rate: float, burst: float, env: str | None = None, max_keys: int = 10_000):
        if env:
            rate = _env_f(env, rate)
            burst = _env_f(env + '_BURST', burst)
        self.name = name
        self.rate = rate
        self.burst = burst
        self.max_keys = max_keys
        self._b: dict[str, list] = {}       # key -> [tokens, last_ts]
        self._lock = threading.Lock()
        self.limited = 0

    def take(self, key: str, cost: float = 1.0) -> tuple[bool, float, float]:
        """→ (allowed, tokens_left, retry_after_s)."""
        if self.rate <= 0:
            return True, self.burst, 0.0
        now = time.monotonic()
        with self._lock:
            b = self._b.get(key)
            if b is None:
                if len(self._b) >= self.max_keys:
                    # drop the staleest third — cheap enough at 10k entries
                    for k in sorted(self._b, key=lambda k: self._b[k][1])[: self.max_keys // 3]:
                        del self._b[k]
                b = self._b[key] = [self.burst, now]
            tokens = min(self.burst, b[0] + (now - b[1]) * self.rate)
            if tokens >= cost:
                b[0], b[1] = tokens - cost, now
                return True, tokens - cost, 0.0
            b[0], b[1] = tokens, now
            self.limited += 1
            return False, tokens, max(0.5, (cost - tokens) / self.rate)

    def stats(self) -> dict:
        return {'rate_per_s': self.rate, 'burst': self.burst, 'clients': len(self._b), 'limited': self.limited}


def client_key(request) -> str:
    """Rate-limit key.  The exe.dev proxy reaches us on loopback and
    APPENDS the client address to whatever ``X-Forwarded-For`` the client
    sent, so only the **last** hop is trustworthy — keying on the first let
    anyone mint a fresh bucket per request with a forged header (2026-11-10
    security check).  Anything not arriving on loopback (a direct LAN
    connection to the 0.0.0.0 listener) is keyed on its socket address and
    its headers are ignored."""
    xff = request.headers.get('X-Forwarded-For')
    ra = request.remote_addr or ''
    if xff and _from_proxy(ra):
        return xff.split(',')[-1].strip() or 'unknown'
    return ra or 'unknown'


def _from_proxy(addr: str) -> bool:
    try:
        import ipaddress
        return ipaddress.ip_address(addr).is_loopback
    except ValueError:
        return False


def is_local(request) -> bool:
    """On-box caller (watchdog probe, CLI): loopback socket, no proxy header."""
    return _from_proxy(request.remote_addr or '') and not request.headers.get('X-Forwarded-For')


class ClientGuard:
    """Whole-tier fairness: a generous per-client token bucket plus a cap on
    one client's requests *in flight* per worker, so no single address can
    hold every gunicorn thread or hammer the uncached endpoints forever.

    ``data/rate_friends`` (one IP or CIDR per line, ``#`` comments; re-read
    every 30 s) lists sibling services (trusted consumers) that get
    ``friend_x`` × the rate/burst and a higher in-flight cap.  Per worker —
    the effective budget is × the number of workers."""

    FRIENDS_PATH = 'data/rate_friends'

    def __init__(self):
        self.rate = _env_f('PUBLIC_RATE_PER_S', 30.0)
        self.burst = _env_f('PUBLIC_RATE_PER_S_BURST', 900.0)
        self.friend_x = _env_f('PUBLIC_FRIEND_X', 5.0)
        self.inflight_max = int(_env_f('PUBLIC_INFLIGHT_MAX', 14))
        self.inflight_friend = int(_env_f('PUBLIC_INFLIGHT_FRIEND', 20))
        self._rl = RateLimiter('public', self.rate, self.burst)
        self._rl_friend = RateLimiter('public_friend', self.rate * self.friend_x, self.burst * self.friend_x)
        self._in: dict[str, int] = {}
        self._lock = threading.Lock()
        self._friends: list = []
        self._friends_t = 0.0
        self.inflight_rejected = 0

    def _is_friend(self, key: str) -> bool:
        now = time.monotonic()
        if now - self._friends_t > 30:
            self._friends_t = now
            nets = []
            try:
                import ipaddress
                for line in open(self.FRIENDS_PATH):
                    line = line.split('#')[0].strip()
                    if line:
                        try:
                            nets.append(ipaddress.ip_network(line, strict=False))
                        except ValueError:
                            pass
            except OSError:
                pass
            self._friends = nets
        if not self._friends:
            return False
        try:
            import ipaddress
            ip = ipaddress.ip_address(key)
        except ValueError:
            return False
        return any(ip.version == n.version and ip in n for n in self._friends)

    def enter(self, key: str, count_inflight: bool = True) -> tuple[bool, float, str]:
        """→ (allowed, retry_after_s, reason).  On allowed + count_inflight
        the caller MUST call ``leave(key)`` when the request ends."""
        friend = self._is_friend(key)
        ok, _left, retry = (self._rl_friend if friend else self._rl).take(key)
        if not ok:
            return False, retry, 'rate_limited'
        if count_inflight:
            cap = self.inflight_friend if friend else self.inflight_max
            with self._lock:
                n = self._in.get(key, 0)
                if cap > 0 and n >= cap:
                    self.inflight_rejected += 1
                    return False, 1.0, 'too_many_in_flight'
                self._in[key] = n + 1
        return True, 0.0, ''

    def leave(self, key: str) -> None:
        with self._lock:
            n = self._in.get(key, 0) - 1
            if n <= 0:
                self._in.pop(key, None)
            else:
                self._in[key] = n

    def stats(self) -> dict:
        with self._lock:
            top = sorted(self._in.items(), key=lambda kv: -kv[1])[:5]
        return {'rate': self._rl.stats(), 'friend_rate': self._rl_friend.stats(),
                'friends': [str(n) for n in self._friends],
                'inflight_max': self.inflight_max, 'inflight_friend': self.inflight_friend,
                'inflight_rejected': self.inflight_rejected, 'inflight_top': top}


CLIENTS = ClientGuard()


# --------------------------------------------------------------------------
class MemoryWatchdog:
    def __init__(self, soft_mb: float = 900, hard_mb: float = 1300, interval_s: float = 5.0):
        self.soft_mb = _env_f('PUBLIC_RSS_SOFT_MB', soft_mb)
        self.hard_mb = _env_f('PUBLIC_RSS_HARD_MB', hard_mb)
        self.interval_s = interval_s
        self._last = 0.0
        self._lock = threading.Lock()
        self.last_rss_mb = 0.0
        self.sheds = 0
        self.recycling = False
        self._last_shed = 0.0
        self._shed_backoff = 30.0   # grows while shedding frees nothing
        self._above_soft = False    # last known state (for state-change logging)

    def check(self, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now - self._last < self.interval_s:
            return
        with self._lock:
            if not force and now - self._last < self.interval_s:
                return
            self._last = now
        rss = self.last_rss_mb = rss_mb()
        if rss >= self.hard_mb and not self.recycling:
            self.recycling = True
            log.error('memory watchdog: rss %.0f MB ≥ hard %.0f MB — recycling worker pid %d',
                      rss, self.hard_mb, os.getpid())
            try:
                import mem_lru
                mem_lru.clear_all()
            except Exception:  # noqa: BLE001
                pass
            # gunicorn worker: SIGTERM → stop accepting, drain in-flight, exit; arbiter re-spawns
            threading.Timer(0.5, lambda: os.kill(os.getpid(), signal.SIGTERM)).start()
        elif rss >= self.soft_mb:
            if now - self._last_shed < self._shed_backoff:
                return
            self._last_shed = now
            try:
                import mem_lru
                dropped = mem_lru.clear_all()
            except Exception:  # noqa: BLE001
                dropped = {}
            dropped = {k: v for k, v in dropped.items() if v}
            if dropped:
                # actual shedding: log it and reset the back-off
                self.sheds += 1
                gc.collect()
                self._shed_backoff = 30.0
                log.warning('memory watchdog: rss %.0f MB ≥ soft %.0f MB — shed caches %s → rss %.0f MB',
                            rss, self.soft_mb, dropped, rss_mb())
            else:
                # nothing left to shed (all caches already empty): only log the
                # state change, then back off exponentially (30 s → 10 min)
                if not self._above_soft:
                    log.warning('memory watchdog: rss %.0f MB ≥ soft %.0f MB with empty caches — '
                                'nothing to shed; next check in %.0f s',
                                rss, self.soft_mb, self._shed_backoff)
                self._shed_backoff = min(self._shed_backoff * 2, 600.0)
            self._above_soft = True
        elif self._above_soft:
            self._above_soft = False
            self._shed_backoff = 30.0
            log.info('memory watchdog: rss %.0f MB back below soft %.0f MB', rss, self.soft_mb)

    def stats(self) -> dict:
        return {'rss_mb': round(self.last_rss_mb or rss_mb()), 'soft_mb': self.soft_mb, 'hard_mb': self.hard_mb,
                'sheds': self.sheds, 'recycling': self.recycling, 'pid': os.getpid()}


WATCHDOG = MemoryWatchdog()
