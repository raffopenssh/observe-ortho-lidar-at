"""Zenodo PUT throughput watchdog (``zenodo_client.ProgressFileWrapper``)
driven by synthetic timelines.  Run: ``python3 tests/test_put_watchdog.py``."""
import io
import json
import os
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import zenodo_client as zc  # noqa: E402

KB = 1024
MB = 1024 * 1024
W = zc.Client._STALL_WINDOW_S
G = zc.Client._STALL_GRACE_S


class Clock:
    def __init__(self):
        self.t = 1_000_000.0

    def __call__(self):
        return self.t


def run(rate_fn, total, floor, fleet=None, abs_min=20 * KB, hours=6.0,
        slow_on=True, step=1.0):
    """Drive the wrapper with ``rate_fn(t_rel) -> bytes/s`` for *hours*.
    Returns (kind, t_rel) of the abort or (None, sent)."""
    clk = Clock()
    fh = io.BytesIO(b"\0" * 16)        # content irrelevant; we fake _sent
    w = zc.ProgressFileWrapper(
        fh, total, stall_min_bps=floor if slow_on else 0,
        stall_abs_min_bps=abs_min if slow_on else 0, stall_fleet_bps=fleet,
        stall_window_s=W, stall_grace_s=G, stall_skip_frac=zc.Client._STALL_SKIP_FRAC,
        stall_dead_s=zc.Client._STALL_DEAD_S, clock=clk)
    t0 = clk.t
    while clk.t - t0 < hours * 3600 and w._sent < total:
        clk.t += step
        w._sent = min(total, w._sent + int(rate_fn(clk.t - t0) * step))
        try:
            w._check_stall(clk.t)
        except zc.UploadStalledError:
            return w.stalled_kind, clk.t - t0, w.stalled_reason
    return None, w._sent, None


def test_steady_under_fleet_floor_no_abort():
    # 60 KB/s, fleet_med 70 → floor = max(20, 17.5) = 20 → fine
    kind, _, _ = run(lambda t: 60 * KB, 2000 * MB, floor=20 * KB, fleet=70 * KB, hours=10)
    assert kind is None, kind


def test_steady_but_below_floor_protected():
    # 60 KB/s steady, fleet 1 MB/s → floor 256 KB/s: below floor, but
    # monotone ≥ ABS_MIN ⇒ steady protection (req. 2)
    kind, _, _ = run(lambda t: 60 * KB, 2000 * MB, floor=256 * KB, fleet=1024 * KB, hours=10)
    assert kind is None, kind


def test_slow_under_fast_fleet_aborts_after_window():
    # 15 KB/s < ABS_MIN under fleet_med 400 → abort once window full
    kind, t, reason = run(lambda t: 15 * KB, 2000 * MB, floor=100 * KB, fleet=400 * KB)
    assert kind == "slow", (kind, reason)
    assert W <= t <= W + 10, t
    assert "floor=100 KB/s" in reason and "fleet_med=400 KB/s" in reason, reason


def test_dead_10min_aborts():
    def rate(t):
        return 500 * KB if t < 900 else 0
    kind, t, reason = run(rate, 2000 * MB, floor=20 * KB, fleet=None)
    assert kind == "dead", (kind, reason)
    assert 900 + 600 - 5 <= t <= 900 + 600 + 5, t
    assert "fleet_med=n/a" in reason


def test_dead_rule_survives_slow_off():
    def rate(t):
        return 500 * KB if t < 900 else 0
    kind, t, _ = run(rate, 2000 * MB, floor=0, fleet=None, slow_on=False)
    assert kind == "dead", kind


def test_ninety_percent_skip_never_aborts():
    # fast to 95 %, then 1 KB/s crawl: slow rule skipped; bytes still move so not dead
    total = 100 * MB
    def rate(t):
        return 5 * MB if t < 19 else 1 * KB
    kind, _, _ = run(rate, total, floor=200 * KB, fleet=800 * KB, hours=4)
    assert kind is None, kind


def test_finishing_cheaper_protected():
    # Bursty (non-steady): 400 KB/s for 2 h, then 10 s-on/50 s-off bursts
    # averaging ~5 KB/s with gaps — but only 10 MB remain → finishing is
    # cheaper than restarting a 2 h PUT.
    total = int(400 * KB * 7200) + 10 * MB
    def rate(t):
        if t < 7200:
            return 400 * KB
        return 30 * KB if int(t) % 60 < 10 else 0
    kind, _, _ = run(rate, total, floor=100 * KB, fleet=400 * KB, hours=4)
    assert kind is None, kind


def test_gappy_outlier_aborts():
    # Non-monotone: 10 s bursts at 30 KB/s then 50 s nothing (≈5 KB/s)
    # on a 2 GB body with nothing sunk yet → slow abort (not dead: gaps < 10 min)
    def rate(t):
        return 30 * KB if int(t) % 60 < 10 else 0
    kind, t, _ = run(rate, 2000 * MB, floor=100 * KB, fleet=400 * KB)
    assert kind == "slow", kind


def test_fleet_put_floor_file():
    d = tempfile.mkdtemp()
    old = zc.FLEET_PUT_FILE
    try:
        zc.FLEET_PUT_FILE = Path(d) / "f.json"
        zc._fleet_put_cache.update(mtime=-1.0, d={})
        r = zc.fleet_put_floor()
        assert r["floor_bps"] == zc.STALL_ABS_MIN_BPS and r["fleet_med_bps"] is None
        zc.FLEET_PUT_FILE.write_text(json.dumps(
            {"ts": time.time(), "fleet_med_bps": 400 * KB, "n": 5,
             "floor_bps": 100 * KB, "abs_min_bps": 20 * KB, "frac": 0.25}))
        r = zc.fleet_put_floor()
        assert r["floor_bps"] == 100 * KB and r["fleet_med_bps"] == 400 * KB, r
        # stale → abs only
        zc.FLEET_PUT_FILE.write_text(json.dumps(
            {"ts": time.time() - 3 * 3600, "fleet_med_bps": 400 * KB,
             "floor_bps": 100 * KB, "abs_min_bps": 20 * KB, "frac": 0.25}))
        r = zc.fleet_put_floor()
        assert r["floor_bps"] == 20 * KB and r["fleet_med_bps"] is None, r
    finally:
        zc.FLEET_PUT_FILE = old
        zc._fleet_put_cache.update(mtime=-1.0, d={})


def test_director_fleet_median():
    import peer_director as pd
    d = Path(tempfile.mkdtemp())
    old = (pd._FLEET_PUT_SAMPLES, pd._FLEET_PUT_LOCK, pd._FLEET_PUT_LOCAL)
    pd._FLEET_PUT_SAMPLES, pd._FLEET_PUT_LOCK, pd._FLEET_PUT_LOCAL = (
        d / "s.json", d / "s.lock", d / "local.json")
    try:
        now = time.time()
        for i, bps in enumerate((50, 100, 400)):
            st0 = {"current_kg": {"upload_progress": {"file": "full_gpkg", "sent": 0, "total": 10 ** 9}}}
            st1 = {"current_kg": {"upload_progress": {"file": "full_gpkg", "sent": bps * KB * 120, "total": 10 ** 9}}}
            pd.note_put_progress(f"p{i}", st0, now=now - 120)
            pd.note_put_progress(f"p{i}", st1, now=now)
        pd._fleet_put_cache.update(at=0.0, v={})
        v = pd.fleet_put_rate(ttl=0)
        assert v["n"] == 3 and abs(v["fleet_med_bps"] - 100 * KB) < 2, v
        assert v["floor_bps"] == max(20 * KB, 0.25 * 100 * KB) == 25 * KB, v
        assert json.loads(pd._FLEET_PUT_LOCAL.read_text())["floor_bps"] == 25 * KB
        ack = pd.fleet_put_ack()
        assert ack["floor"] == 25 * KB and len(json.dumps(ack)) < 80, ack
    finally:
        pd._FLEET_PUT_SAMPLES, pd._FLEET_PUT_LOCK, pd._FLEET_PUT_LOCAL = old
        pd._fleet_put_cache.update(at=0.0, v={})


if __name__ == "__main__":
    fails = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print("ok  ", name)
            except Exception as e:  # noqa: BLE001
                fails += 1
                print("FAIL", name, "->", repr(e))
    sys.exit(1 if fails else 0)
