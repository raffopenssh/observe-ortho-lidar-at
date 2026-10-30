"""Zenodo write mutex.

All Zenodo write operations (product uploads, tile-cache archives,
checkpoint bundles) must be serialised: concurrent PUTs against one draft
deposition fail with 409s and orphan files.

* Single VM (default, ``config.ZENODO_LOCK_URL`` empty): an ``fcntl`` file
  lock in ``config.PROCESSOR_DIR/zenodo_upload.lock`` shared by every
  process on this host (processor, app.py maintenance threads).
* Multi-VM: an HTTP lease broker at ``ZENODO_LOCK_URL`` (the director's
  ``/api/v1/zenodo/lock`` routes); the request carries
  ``X-Operator-Token`` from ``config.operator_token()``.

Usage::

    from zenodo_lock import zenodo_upload_lock

    with zenodo_upload_lock(purpose='kg_upload', kg='91109'):
        client.upload_stream(...)
"""
from __future__ import annotations

import contextlib
import fcntl
import logging
import os
import socket
import threading
import time
from pathlib import Path
from typing import Optional

import requests

import config

log = logging.getLogger(__name__)

_DEFAULT_TTL = 120.0  # must match the broker's TTL
_HEARTBEAT_INTERVAL = 30.0
_ACQUIRE_RETRY_INTERVAL = 5.0
_ACQUIRE_TIMEOUT = 7200.0
_BROKER_HTTP_TIMEOUT = (5, 60)  # (connect, read)

LOCAL_LOCK_FILE = config.PROCESSOR_DIR / "zenodo_upload.lock"


def _headers() -> dict:
    tok = config.operator_token()
    return {'X-Operator-Token': tok} if tok else {}


def _broker_url() -> Optional[str]:
    return config.ZENODO_LOCK_URL or None


def _peer_id() -> str:
    return os.environ.get('ZENODO_LOCK_PEER') or os.environ.get('INSTANCE_ID') or socket.gethostname() or 'unknown'


# --- local (single-VM) lock ----------------------------------------------

@contextlib.contextmanager
def _local_lock(purpose: str, kg: str | None, max_wait_s: float):
    LOCAL_LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(LOCAL_LOCK_FILE), os.O_RDWR | os.O_CREAT, 0o644)
    deadline = time.monotonic() + max_wait_s
    waited = False
    try:
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    log.error('Zenodo local lock timeout after %.0fs — proceeding without lock', max_wait_s)
                    yield None
                    return
                if not waited:
                    log.info('Zenodo local lock busy (purpose=%s, kg=%s) — waiting', purpose, kg)
                    waited = True
                time.sleep(1.0)
        try:
            os.ftruncate(fd, 0)
            os.write(fd, f"{os.getpid()} {purpose} {kg or ''} {time.time():.0f}\n".encode())
        except OSError:
            pass
        yield 'local'
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        except OSError:
            pass
        os.close(fd)


# --- HTTP broker lease (multi-VM) ------------------------------------------

class ZenodoLease:
    """Active lease on the broker.  Renews itself in a daemon thread."""

    def __init__(self, broker: str, peer: str, token: str, ttl_s: float):
        self.broker = broker
        self.peer = peer
        self.token = token
        self.ttl_s = ttl_s
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._heartbeat_loop, name='zenodo-lease-hb', daemon=True)
        self._thread.start()

    def _heartbeat_loop(self):
        consecutive_fail = 0
        while not self._stop.wait(_HEARTBEAT_INTERVAL):
            try:
                r = requests.post(f"{self.broker}/api/v1/zenodo/lock/heartbeat", json={'token': self.token},
                                  timeout=_BROKER_HTTP_TIMEOUT, headers=_headers())
                if r.status_code == 410:
                    log.warning('Upload lease lost (410) — broker reclaimed it')
                    self._stop.set()
                    return
                r.raise_for_status()
                consecutive_fail = 0
            except Exception as e:
                consecutive_fail += 1
                level = log.info if consecutive_fail < 3 else log.warning
                level('Upload broker heartbeat blip (%d): %s', consecutive_fail, str(e)[:160])

    def release(self):
        self._stop.set()
        try:
            requests.delete(f"{self.broker}/api/v1/zenodo/lock", json={'token': self.token},
                            timeout=(3, 10), headers=_headers())
            return
        except Exception as e:
            log.info('Upload broker release: sync attempt failed (%s) — retrying in background', str(e)[:120])

        def _release_in_background():
            for attempt in range(3):
                try:
                    requests.delete(f"{self.broker}/api/v1/zenodo/lock", json={'token': self.token},
                                    timeout=_BROKER_HTTP_TIMEOUT, headers=_headers())
                    return
                except Exception as e:
                    if attempt == 2:
                        log.info('Upload broker release blip (will TTL-reclaim in %.0fs): %s',
                                 _DEFAULT_TTL, str(e)[:160])
                    else:
                        time.sleep(2 * (attempt + 1))
        threading.Thread(target=_release_in_background, name='zenodo-lease-release', daemon=True).start()


@contextlib.contextmanager
def zenodo_upload_lock(purpose: str = 'unknown', kg: str | None = None,
                       max_wait_s: float = _ACQUIRE_TIMEOUT):
    """Block until a Zenodo write lease is granted, then auto-release."""
    broker = _broker_url()
    if broker is None:
        with _local_lock(purpose, kg, max_wait_s) as lk:
            yield lk
        return
    peer = _peer_id()
    deadline = time.monotonic() + max_wait_s
    lease: Optional[ZenodoLease] = None
    backoff = _ACQUIRE_RETRY_INTERVAL
    transport_failures = 0
    overload_5xx = 0
    while True:
        try:
            r = requests.post(f"{broker}/api/v1/zenodo/lock", json={'peer': peer, 'purpose': purpose, 'kg': kg},
                              timeout=_BROKER_HTTP_TIMEOUT, headers=_headers())
        except Exception as e:
            transport_failures += 1
            if transport_failures <= 2 and time.monotonic() < deadline:
                log.info('Zenodo lock broker transport error (%s) — retry %d', str(e)[:120], transport_failures)
                time.sleep(min(backoff, max(1.0, deadline - time.monotonic())))
                backoff = min(backoff * 1.5, 30.0)
                continue
            log.warning('Zenodo lock broker unreachable after %d attempts (%s) — proceeding without lease',
                        transport_failures, str(e)[:160])
            yield None
            return
        if r.status_code == 200:
            data = r.json()
            lease = ZenodoLease(broker, peer, data['token'], float(data.get('ttl_s', _DEFAULT_TTL)))
            log.info('Zenodo lease acquired (purpose=%s, kg=%s)', purpose, kg)
            break
        if r.status_code == 423:
            data = r.json() if r.content else {}
            log.info('Zenodo lock busy (%s; held by %s for %ss) — waiting',
                     data.get('reason') or 'locked', data.get('holder'), data.get('age_s'))
        elif r.status_code in (500, 502, 503, 504):
            overload_5xx += 1
            log.info('Zenodo lock broker %d (transient overload) — retry %d', r.status_code, overload_5xx)
        else:
            log.warning('Zenodo lock acquire returned HTTP %d: %s', r.status_code, r.text[:200])
        if time.monotonic() >= deadline:
            log.error('Zenodo lock acquire timeout after %.0fs — proceeding without lease', max_wait_s)
            yield None
            return
        time.sleep(min(backoff, max(1.0, deadline - time.monotonic())))
        backoff = min(backoff * 1.5, 30.0)
    try:
        yield lease
    finally:
        if lease is not None:
            lease.release()
