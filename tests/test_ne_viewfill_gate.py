"""ne_viewfill: no periodic re-walk / re-plan of unchanged parents (2026-11-12).

    python3 tests/test_ne_viewfill_gate.py
"""
import os, sys, time, types, tempfile
from pathlib import Path
os.environ["SRTM_APP_LIB_ONLY"] = "1"
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import numpy as np
import ne_viewfill as vf
import ne_ingest, ne_h3, inbound_budget

tmp = Path(tempfile.mkdtemp())
vf.STAGED_DIR = tmp / "staged"
(tmp / "00001.h3b").write_bytes(b"x")
ne_ingest.path_for = lambda kg: tmp / f"{kg}.h3b"
ne_ingest._coverage_complete = lambda kg, entries=None: True
inbound_budget.add = lambda *a, **k: None
FILE = {"digest": "d1"}
IDS = np.arange(1, 11, dtype=np.uint64)
vf._ids_observed = lambda p: (IDS, IDS[:8], FILE["digest"])          # 9,10 unseen
VIEW = {"etag": '"e1"', "from": "stored", "ids": np.arange(1, 13, dtype=np.uint64)}   # 11,12 not in file
vf._view_ids = lambda v: VIEW["ids"]
ne_h3.declared_view = lambda kg, expect_etag=None, refresh=False: types.SimpleNamespace(
    header={"_from": VIEW["from"], "_etag": VIEW["etag"], "digest": "vd", "source": "chunks@x"}, raw=b"")
ne_h3.declared_net_snapshot = lambda: 0
ne_h3.declared_net_delta = lambda s: {"gets": 0, "bytes": 0, "n304": 0}
ne_h3.corpus_obs_rows = lambda gap, exclude_kg=None: (np.array([9], np.uint64), None)   # 9 borrowable
vf._sources_for = lambda *a, **k: []
SWEPT = {"v": False}
vf._view_swept = lambda kg: SWEPT["v"]
vf._file_digest = lambda kg: FILE["digest"]

def walk(prev):
    return vf.process("00001", entries={}, idx={"files": {}}, st={"kgs": {"00001": prev} if prev else {}})

# 1. first walk: residual {10,11,12} planned
r1 = walk(None)
assert r1["residual_n"] == 3 and r1.get("planned_ts") and not r1.get("skip"), r1
# 2. same view + file → skip, planned_ts carried — also when the walk is very old
old = dict(r1, ts=time.time() - 400 * 86400)
r2 = walk(old)
assert r2.get("skip") == "unchanged" and r2["planned_ts"] == r1["planned_ts"], r2
# 3. view moved (new etag) but same residual, plan 30 days old → held, never re-planned, planned_ts kept
VIEW.update(etag='"e2"', **{"from": "200"})
r3 = walk(dict(r1, planned_ts=time.time() - 30 * 86400))
assert not r3.get("skip") and r3.get("residual_held") and r3.get("planned_ts"), r3
import json
assert json.loads((vf.STAGED_DIR / "00001.json").read_text())["plan_cells"] == []
# 4. skip after a held row keeps residual_held
r4 = walk(r3)
assert r4.get("skip") == "unchanged" and r4.get("residual_held") and r4.get("planned_ts"), r4
# 5. legacy row that lost both markers: healed only when the sweep ran view units
legacy = {k: v for k, v in r3.items() if k not in ("planned_ts", "residual_held")}
r5 = walk(legacy); assert not r5.get("skip") and r5.get("planned_ts"), r5     # never swept → planned now
SWEPT["v"] = True
r5 = walk(legacy); assert r5.get("skip") and r5.get("residual_held"), r5
# ... and ne_h3.view_coverage accepts it
ctx = {"sweep": {"00001": {"state": "done", "units": [{"state": "ok"}]}}, "vf": {"00001": r5}, "staged": set()}
assert ne_h3.view_coverage("00001", ctx) == ne_h3.COVERAGE_SWEPT
ctx["vf"]["00001"] = legacy
assert ne_h3.view_coverage("00001", ctx) is None
# 6. a changed residual set is planned again
VIEW.update(etag='"e3"', ids=np.arange(1, 14, dtype=np.uint64))
r6 = walk(r5); assert r6["residual_n"] == 4 and r6.get("planned_ts") and not r6.get("residual_held"), r6
# 7. changed file → not skipped
FILE["digest"] = "d2"
r7 = walk(r6); assert not r7.get("skip"), r7

# candidates(): gate on the file digest, not on age
ne_ingest.load_manifest_entries = lambda *a, **k: {}
ne_ingest.load_index_ro = lambda: {"files": {}}
vf.mixed_parents = lambda e, i: []
OM = {}
vf.ne_server_obs_missing = lambda st=None: OM
def cands(row):
    st = {"kgs": {"00001": row}, "obs_missing": {}, "stats": {"local_scan": {"ts": time.time(), "unseen": {"00001": 2}}}}
    return vf.candidates(st=st)
aged = lambda r: dict(r, ts=time.time() - 30 * 86400)
assert cands(aged(r7)) == [], "unchanged file, residual planned: not a candidate after the gate"
assert cands(aged(dict(r7, base_digest="other"))) != [], "file changed: candidate"
assert cands(aged(legacy | {"base_digest": "d2"})) != [], "residual never handed to the sweep: candidate"
assert cands(aged(dict(r7, error="x"))) != []
OM["00001"] = {"blank_n": 1, "missing_n": 0}
c = cands(aged(r7)); assert c and c[0][2] == "NE server", c
print("ok")
