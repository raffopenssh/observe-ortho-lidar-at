"""Restored-checkpoint label-id collision (Oct 2026): the restore path must
renumber objects AND labels together; the stitch guard must reject a tile
whose labels point at foreign objects; the GPKG gate must measure 0 on a
clean file.  Run: PYTHONPATH=. python3 tests/test_label_mix.py"""
import pickle, types, numpy as np
import austria_processor as ap


def _obj(i):
    o = types.SimpleNamespace(obj_id=i, type_code=3)
    return o


def test_remap():
    # original run: tile objects 101..104, core = 102,103 ; labels carry 101..104
    objs = [_obj(i) for i in (101, 102, 103, 104)]
    core = [objs[1], objs[2]]
    lab = np.array([[0, 101, 102], [103, 104, 104]], dtype=np.int32)
    ck = pickle.loads(pickle.dumps({"seg_result": {"objects": objs, "labels": lab}, "core_objects": core}))
    # emulate the closure helper via a tiny shim: pull the function source off process_one_kg
    import inspect, textwrap
    src = inspect.getsource(ap.process_one_kg)
    start = src.index("    def _remap_restored_tile_ids(")
    end = src.index("    def _load_tile_checkpoint(")
    ns = {"np": np, "log": ap.log}
    exec(textwrap.dedent(src[start:end]), ns)
    nxt = ns["_remap_restored_tile_ids"](ck, 7, "T", "tile 1/1")
    assert nxt == 11, nxt
    ids = [o.obj_id for o in ck["seg_result"]["objects"]]
    assert ids == [7, 8, 9, 10], ids
    assert [o.obj_id for o in ck["core_objects"]] == [8, 9]
    assert ck["seg_result"]["labels"].tolist() == [[0, 7, 8], [9, 10, 10]]


def test_guard():
    a = [_obj(1), _obj(2)]
    b = [_obj(3), _obj(4)]
    obj_map = {o.obj_id: o for o in a + b}
    tr_ok = {"objects": a, "labels": np.array([[1, 2, 0]])}
    ap._assert_tile_labels_consistent(tr_ok, obj_map, 0, "T")
    tr_bad = {"objects": a, "labels": np.array([[3, 4, 3, 4, 3, 4]])}
    try:
        ap._assert_tile_labels_consistent(tr_bad, obj_map, 1, "T")
    except ap.LabelMixError:
        pass
    else:
        raise AssertionError("foreign labels not rejected")


def test_fp_deterministic(tmp_path, monkeypatch):
    """A code re-detected after a rerun with unchanged counts → fp, not detected."""
    import label_mix as lm
    monkeypatch.setattr(lm, "STATE_FILE", tmp_path / "lm.json")
    monkeypatch.setattr(lm, "NE_DIR", tmp_path)
    (tmp_path / "99999.h3b").write_bytes(b"x")
    r = {"n": 5000, "geb": 10, "geb_flat": 0, "wald": 7194, "wald_flat": 153}
    monkeypatch.setattr(lm, "audit_h3b", lambda p: dict(r))
    s = lm.scan(full=True)
    assert s["new_detected"] == ["99999"]
    d = lm._load(); d["codes"]["99999"]["state"] = "done"; lm._save(d)
    r["wald_flat"] = 160                       # within FP_REL_TOL → deterministic
    s = lm.scan(full=True)
    assert s["new_fp"] == ["99999"] and lm._load()["codes"]["99999"]["state"] == "fp"
    assert lm.summary()["counts"] == {"fp": 1}
    # a changed signature after rerun is a genuine re-detect
    d = lm._load(); d["codes"]["99999"].update(state="done", counts={"geb_flat": 0, "wald_flat": 153}); lm._save(d)
    r["wald_flat"] = 600
    s = lm.scan(full=True)
    assert s["new_detected"] == ["99999"] and lm._load()["codes"]["99999"]["state"] == "detected"


if __name__ == "__main__":
    import pathlib, tempfile

    class _MP:
        def setattr(self, o, n, v): setattr(o, n, v)
    test_remap(); test_guard(); test_fp_deterministic(pathlib.Path(tempfile.mkdtemp()), _MP()); print("ok")
