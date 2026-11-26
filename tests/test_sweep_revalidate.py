"""sliver_sweep: oversize view-fill clusters are grid-cut; open units whose
cells were observed since the plan are dropped, not run (2026-11-26).

    python3 tests/test_sweep_revalidate.py
"""
import os, sys, tempfile
from pathlib import Path
os.environ["SRTM_APP_LIB_ONLY"] = "1"
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import numpy as np
import h3.api.numpy_int as h3n
import sliver_sweep as s
import ne_viewfill

tmp = Path(tempfile.mkdtemp())
s.DATA_DIR = tmp; s.STATE_FILE = tmp / "sliver_sweep.json"; s.LOCK_FILE = tmp / "sliver_sweep.lock"
s.NE_DIR = tmp
s.enabled = lambda: True

# a 15 km x 9 km block of res-12 cells (sampled) — one connected cluster
lat = np.arange(47.00, 47.08, 0.004); lon = np.arange(11.00, 11.20, 0.004)
ids = np.unique(np.array([h3n.latlng_to_cell(a, o, 12) for a in lat for o in lon], dtype=np.uint64))
r = s.plan_cells("00001", ids, bbox_fallback=[11.0, 47.0, 11.2, 47.08])
st = s.load_state()["kgs"]["00001"]
us = st["units"]
assert r["planned"] == len(us) and 4 <= len(us) <= s.MAX_UNITS_SPLIT, r
assert sum(u["cells"] for u in us) + r["deferred_cells"] == ids.size
for u in us:
    w, s_, e, n = u["bbox"]
    assert (n - s_) * 111_000 <= s.MAX_UNIT_M + 400 and (e - w) * 111_000 * np.cos(np.radians(47)) <= s.MAX_UNIT_M + 400, u
print("ok split:", len(us), "units")

# small cluster stays one unit
r2 = s.plan_cells("00002", ids[:1], bbox_fallback=[11.0, 47.0, 11.2, 47.08])
assert r2["planned"] == 1

# revalidate: everything observed by now → all units dropped, parent handed to integrate
(tmp / "00001.h3b").write_bytes(b"x"); (tmp / "00002.h3b").write_bytes(b"x")
s._parent_path = lambda c: tmp / f"{c}.h3b"
ne_viewfill._file_digest = lambda kg: "d1"
SEEN = {"00001": ids, "00002": ids[:0]}
ne_viewfill._ids_observed = lambda p: (ids, SEEN[p.stem], "d1")
s.observed_elsewhere = lambda g: np.zeros(g.size, bool)
import ne_h3
ne_h3.DECLARED_DIR = tmp / "none"
out = {r["parent"]: r for r in s.revalidate_open(dry=True)}
assert len(out["00001"]["dropped"]) == len(us) and out["00002"]["kept"], out
assert len(s.load_state()["kgs"]["00001"]["units"]) == len(us)          # dry: untouched
s.revalidate_open()
k = s.load_state()["kgs"]
assert k["00001"]["units"] == [] and k["00001"]["state"] == "received", k["00001"]
assert len(k["00002"]["units"]) == 1 and k["00002"]["state"] == "pending" and k["00002"]["reval_digest"] == "d1"
assert s.revalidate_open() == []                                        # same file digest → no second check
# a late result of a dropped unit is accepted as stale, never an error
rr = s.accept_result(us[0]["id"], "00001", "p1", 0, "failed", None, error="x")
assert rr.get("stale"), rr
# ids of dropped units are never reused
with s._Locked() as _st:
    _st["kgs"]["00001"]["state"] = "done"
r3 = s.plan_cells("00001", ids[:1])
assert s.load_state()["kgs"]["00001"]["units"][-1]["id"] == f"00001-sw{len(us) + 1}", r3
print("ok revalidate")
