"""Sliver-sweep add-on products (docs/sliver-sweep.md → Products).

Sweep units (``<parent>-sw<k>``) upload ``_light_gpkg_v2`` + ``_json_v2``
only.  They must be visible to the bbox-keyed product services (store
``codes_for_parent``) and INVISIBLE to everything that means "blocks of
the layout": index aggregate, ne_ingest live blocks, zen_stall, the
re-queue invalidation.  Plus: accept_result's products policy and the
R-tree bbox_r2 envelope."""
import os, sys, json, tempfile, re
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("SRTM_APP_IMPORT_ONLY", "1")

from kg_splitter import is_sweep_code, is_block_code, parent_kg_code

assert is_sweep_code("63308-sw1") and is_sweep_code("63308-sw12")
assert not is_sweep_code("63308-south") and not is_sweep_code("63308") and not is_sweep_code("63308-sw") \
    and not is_sweep_code("63308-sw1-x") and not is_sweep_code("")
assert is_block_code("63308-sw1") and parent_kg_code("63308-sw1") == "63308"   # still a block for dup-kg etc.

# --- zen_stall regex + sweep skip (mirrors app.py) ---
rx = re.compile(r'^(\d+(?:-[a-z][-a-z0-9]*)?)_(json|full_gpkg|light_gpkg|light_gpkg_v2)$')
m = rx.match("57306-sw2_light_gpkg_v2"); assert m and is_sweep_code(m.group(1))
m = rx.match("57306-south_light_gpkg_v2"); assert m and not is_sweep_code(m.group(1))

# --- ne_ingest._live_blocks ignores sweep codes ---
import ne_ingest
ent = {"57306-south_json_v2": {"size": 10, "uploaded_at": "2026-10-27T00:00:00Z"},
       "57306-sw2_json_v2": {"size": 10, "uploaded_at": "2026-10-27T00:00:00Z"}}
assert ne_ingest._live_blocks("57306", ent) == ["57306-south"], ne_ingest._live_blocks("57306", ent)

# --- kg_docs.codes_for_parent skips sweep codes (index aggregate) ---
import kg_docs
class _St:
    def codes_for_parent(self, p): return [p + "-south", p + "-sw1", p + "-sw2"]
kg_docs._store = lambda: _St()
with tempfile.TemporaryDirectory() as td:
    plain, blocks = kg_docs.codes_for_parent("57306", td)
    assert not plain and blocks == ["57306-south"], blocks

# --- accept_result products policy ---
import sliver_sweep as sls
with tempfile.TemporaryDirectory() as td:
    from pathlib import Path
    sls.STATE_FILE = Path(td) / "sliver_sweep.json"
    sls.LOCK_FILE = Path(td) / "sliver_sweep.lock"
    sls.SWEEP_DIR = Path(td) / "sweeps"
    st = sls.load_state()
    st["kgs"]["57306"] = {"state": "dispatched", "attempts": 0, "units": [
        {"id": "57306-sw1", "state": "dispatched", "attempts": 0, "bbox": [0, 0, 1, 1]},
        {"id": "57306-sw2", "state": "dispatched", "attempts": 0, "bbox": [0, 0, 1, 1]}]}
    sls._save_state(st)
    import ne_h3
    ne_h3.verify_h3b = lambda p, **k: {"ok": True}
    # 1) upload failure with attempts left → unit failed (retry), h3b kept
    r = sls.accept_result("57306-sw1", "57306", "at1", 0, "ok", b"h3b", cells_observed=5,
                          products="upload: Zenodo 502")
    u = sls.load_state()["kgs"]["57306"]["units"][0]
    assert r["status"] == "failed" and u["state"] == "failed" and u["attempts"] == 1 and (sls.SWEEP_DIR / "57306-sw1.h3b").exists(), (r, u)
    # 2) verify failure → accepted ok, products recorded
    r = sls.accept_result("57306-sw2", "57306", "at1", 0, "ok", b"h3b", cells_observed=5,
                          products="verify: tiles_segmented")
    u = sls.load_state()["kgs"]["57306"]["units"][1]
    assert r["status"] == "ok" and u["state"] == "ok" and u["products"].startswith("verify:"), (r, u)
    assert sls.load_state()["kgs"]["57306"]["state"] == "pending"          # sw1 re-dispatch
    # 3) last attempt: upload failure is accepted (NE cells never wait on Zenodo)
    st = sls.load_state(); st["kgs"]["57306"]["units"][0].update(state="dispatched", attempts=sls.MAX_ATTEMPTS - 1); sls._save_state(st)
    r = sls.accept_result("57306-sw1", "57306", "at1", 1, "ok", b"h3b", cells_observed=5, products="upload: 502")
    k = sls.load_state()["kgs"]["57306"]
    assert r["status"] == "ok" and k["units"][0]["state"] == "ok" and k["state"] == "received", (r, k)
    assert sls.status()["products"] == {"ok": 0, "missing": 2, "unknown": 0}, sls.status()["products"]
    assert any("add-on products" in ln for ln in sls.text_lines())

# --- search index: R-tree envelope = bbox ∪ bbox_r2; kg row untouched ---
import search_index as si
env = si.SearchIndex._rtree_envelope({"bbox": {"min_lon": 16.34, "min_lat": 48.21, "max_lon": 16.37, "max_lat": 48.236},
                                      "bbox_r2": {"min_lon": 16.3408, "min_lat": 48.2137, "max_lon": 16.3702, "max_lat": 48.2364}})
assert env == (16.34, 16.3702, 48.21, 48.2364), env
assert si.SearchIndex._rtree_envelope({"bbox": {"min_lon": 1, "min_lat": 2, "max_lon": 3, "max_lat": 4}}) == (1, 3, 2, 4)
print("ALL OK")
