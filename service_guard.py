"""service_guard — keeps the public tier answering under load.

Three independent mechanisms, all per gunicorn worker, all env-tunable:

* ``Gate`` — bounded concurrency for CPU-heavy endpoints (tile rendering).
  Callers wait at most ``max_wait_s`` for a slot, then get ``Overloaded`` →
  the view answers 503 + ``Retry-After`` in a millisecond instead of holding
  a worker thread.  Without this, a tile flood pins all 24 threads in numpy
  and the cheap endpoints (status, peer heartbeats) starve.

* ``RateLimiter`` — per-client token bucket (first ``X-Forwarded-For`` hop,
  which the exe.dev proxy sets).  429 + ``Retry-After`` + ``X-RateLimit-*``
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
    xff = request.headers.get('X-Forwarded-For')
    if xff:
        return xff.split(',')[0].strip() or 'unknown'
    return request.remote_addr or 'unknown'


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
        elif rss >= self.soft_mb and now - self._last_shed > 30:
            self._last_shed = now
            self.sheds += 1
            try:
                import mem_lru
                dropped = mem_lru.clear_all()
            except Exception:  # noqa: BLE001
                dropped = {}
            gc.collect()
            log.warning('memory watchdog: rss %.0f MB ≥ soft %.0f MB — shed caches %s → rss %.0f MB',
                        rss, self.soft_mb, dropped, rss_mb())

    def stats(self) -> dict:
        return {'rss_mb': round(self.last_rss_mb or rss_mb()), 'soft_mb': self.soft_mb, 'hard_mb': self.hard_mb,
                'sheds': self.sheds, 'recycling': self.recycling, 'pid': os.getpid()}


WATCHDOG = MemoryWatchdog()
