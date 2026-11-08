"""Eviction of merged block / sweep inputs (ne_ingest.evict_merged_blocks,
sliver_sweep.evict_integrated_units) and the restore-before-remerge guards."""
import os, sys, json, hashlib, tempfile, time
from pathlib import Path
os.environ.setdefault("SRTM_APP_IMPORT_ONLY", "1")
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import ne_ingest


def _entry(blob, at="2026-11-01T00:00:00+00:00"):
    return {"status": "cf", "size": len(blob), "checksum": "md5:" + hashlib.md5(blob).hexdigest(), "uploaded_at": at}


def test_blocks(tmp: Path):
    ne_ingest.NE_DIR = tmp; ne_ingest.BLOCK_DIR = tmp / "blocks"; ne_ingest.BLOCK_DIR.mkdir()
    (tmp / "12345.h3b").write_bytes(b"union")
    blob = b"H3B1blockbytes"
    for b in ("12345-north", "12345-south"):
        (ne_ingest.BLOCK_DIR / f"{b}.h3b").write_bytes(blob)
    entries = {f"{b}_ne_h3b": _entry(blob) for b in ("12345-north", "12345-south")}
    entries.update({f"{b}_json_v2": {"status": "cf", "size": 1, "uploaded_at": "x"} for b in ("12345-north", "12345-south")})
    old = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - 10 * 86400))
    idx = {"files": {b: {"uploaded_at": entries[f"{b}_ne_h3b"]["uploaded_at"], "size": len(blob),
                         "md5": hashlib.md5(blob).hexdigest(), "kind": "block"} for b in ("12345-north", "12345-south")},
           "parents": {"12345": {"complete": True, "dirty": False, "merged_at": old,
                                 "blocks": ["12345-north", "12345-south"]}},
           "failed": {}, "stats": {}}
    # too young → nothing
    idx["parents"]["12345"]["merged_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    assert ne_ingest.evict_merged_blocks(idx, entries)["evicted"] == 0
    idx["parents"]["12345"]["merged_at"] = old
    # checksum mismatch → kept
    entries["12345-south_ne_h3b"]["checksum"] = "md5:deadbeef"
    r = ne_ingest.evict_merged_blocks(idx, entries)
    assert r["evicted"] == 1 and r["kept"] == 1, r
    assert not (ne_ingest.BLOCK_DIR / "12345-north.h3b").exists() and (ne_ingest.BLOCK_DIR / "12345-south.h3b").exists()
    assert idx["files"]["12345-north"]["evicted"] is True
    # pending() must NOT re-pull an evicted block
    assert [c for c, _ in ne_ingest.pending(entries, idx)] == []
    # dirty parent → merge guard un-evicts, keeps dirty, no merge
    idx["parents"]["12345"]["dirty"] = True
    assert ne_ingest.merge_dirty_parents(idx, entries) == []
    assert idx["parents"]["12345"]["dirty"] is True and idx["parents"]["12345"]["restore_wait"] == 1
    assert "evicted" not in idx["files"]["12345-north"]
    assert [c for c, _ in ne_ingest.pending(entries, idx)] == ["12345-north"]
    # a new upload of an evicted block is still picked up
    idx["files"]["12345-north"]["evicted"] = True
    entries["12345-north_ne_h3b"]["uploaded_at"] = "2026-11-09T00:00:00+00:00"
    assert [c for c, _ in ne_ingest.pending(entries, idx)] == ["12345-north"]
    print("blocks ok")


def test_sweeps(tmp: Path):
    import sliver_sweep as ss
    ss.DATA_DIR = tmp; ss.STATE_FILE = tmp / "sweeps.json"; ss.LOCK_FILE = tmp / "sweeps.lock"
    ss.SWEEP_DIR = tmp / "sweeps"; ss.SWEEP_DIR.mkdir()
    blob = b"H3B1sweepbytes"
    (ss.SWEEP_DIR / "12345-sw1.h3b").write_bytes(blob)
    (ss.SWEEP_DIR / "12345-sw2.h3b").write_bytes(blob)
    entries = {"12345-sw1_ne_h3b": _entry(blob), "12345-sw2_ne_h3b": _entry(blob)}
    st = ss._empty()
    st["kgs"]["12345"] = {"state": "done", "integrated_digest": "d1", "units": [
        {"id": "12345-sw1", "state": "ok"}, {"id": "12345-sw2", "state": "ok"}],
        "done_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - 10 * 86400))}
    ss._save_state(st)
    ss._file_digest = lambda code, idx: "d1"
    entries["12345-sw2_ne_h3b"]["checksum"] = "md5:deadbeef"
    r = ss.evict_integrated_units({}, entries)
    assert r["evicted"] == 1 and r["kept"] == 1, r
    assert not (ss.SWEEP_DIR / "12345-sw1.h3b").exists() and (ss.SWEEP_DIR / "12345-sw2.h3b").exists()
    st = ss.load_state()
    assert st["kgs"]["12345"]["units"][0].get("evicted_at")
    # digest drift → not evictable while a restore is pending, and integrate would wait
    ss._file_digest = lambda code, idx: "d2"
    assert ss.evict_integrated_units({}, entries)["evicted"] == 0
    print("sweeps ok")


if __name__ == "__main__":
    for fn in (test_blocks, test_sweeps):
        with tempfile.TemporaryDirectory() as d:
            (Path(d) / "a").mkdir(); fn(Path(d) / "a")
