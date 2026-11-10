"""stale_blocks.scan classification: stale vs drift, drift kinds."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import stale_blocks as s

# toy geometry: parent bbox (0,0,2,2); blocks are named rects
RECTS = {
    "10000": (0, 0, 2, 2),
    "10000-west": (0, 0, 1, 2), "10000-east": (1, 0, 2, 2),           # current 2-block layout
    "10000-north": (0, 1, 2, 1), "10000-south": (0, 0, 2, 1),         # old 2-block layout (south = full-width strip)
    "10000-center": (0, 0.5, 2, 1.5),
    "10000-southwest": (0, 0, 1, 1), "10000-southeast": (1, 0, 2, 1),
    "10000-northwest": (0, 1, 1, 2), "10000-northeast": (1, 1, 2, 2),
}


def _cover(parent, entries, geom=None):
    geom = geom or RECTS
    rects = [geom[k[:-5]] for k, e in entries.items() if k.endswith("_json") and s._live(e)]
    pts = [(x / 10 + 0.05, y / 10 + 0.05) for x in range(20) for y in range(20)]
    return (all(any(r[0] <= px < r[2] and r[1] <= py < r[3] for r in rects) for px, py in pts), "")


def _ent(*codes, sfx=("json", "full_gpkg", "light_gpkg_v2", "json_v2", "ne_h3b")):
    out = {}
    for i, c in enumerate(codes):
        for x in sfx:
            out[f"{c}_{x}"] = {"size": 10, "status": "", "uploaded_at": f"2026-10-{10 + i:02d}"}
    return out


def _scan(entries, layout, geom=None):
    return s.scan(entries, kgs={"10000": {"kg_code": "10000"}},
                  layout_fn=lambda kg: [{"kg_code": c} for c in layout],
                  cover_fn=lambda p, e: _cover(p, e, geom), sweep_fn=lambda c: False)


def test_unsplit_product_with_split_layout_is_drift_unsplit():
    r = _scan(_ent("10000"), ["10000-west", "10000-east"])
    assert r["stale"] == []
    assert [d["kind"] for d in r["drift"]] == ["unsplit"] and r["drift"][0]["layout_done"] == []


def test_unsplit_plus_one_layout_block_is_drift_mixed():
    r = _scan(_ent("10000-east", "10000"), ["10000-west", "10000-east"])
    assert r["stale"] == []
    d = r["drift"][0]
    assert d["kind"] == "mixed" and d["codes"] == ["10000"] and d["layout_done"] == ["10000-east"]


def test_old_layout_residue_over_complete_current_layout_is_stale():
    r = _scan(_ent("10000-north", "10000-west", "10000-east"), ["10000-west", "10000-east"])
    assert r["drift"] == []
    assert r["stale"][0]["codes"] == ["10000-north"]


def test_label_collision_keeps_load_bearing_old_block():
    # 32024/85207: committed P-south is the OLD full-width strip, current
    # layout's P-south is a centre-column block never processed; P-center
    # + strip + 4 corner blocks cover → P-center must stay (old-blocks).
    layout = ["10000-south", "10000-southwest", "10000-southeast", "10000-northwest", "10000-northeast", "10000-north"]
    geom = dict(RECTS, **{   # 3 columns × 2 rows; committed south/center are old horizontal strips
        "10000-southwest": (0, 0, 0.7, 1), "10000-southeast": (1.3, 0, 2, 1),
        "10000-northwest": (0, 1, 0.7, 2), "10000-northeast": (1.3, 1, 2, 2),
        "10000-north": (0.7, 1, 1.3, 2), "10000-south": (0, 0, 2, 0.6), "10000-center": (0, 0.6, 2, 1.4)})
    ents = _ent("10000-center", "10000-south", "10000-southwest", "10000-southeast", "10000-northwest", "10000-northeast", "10000-north")
    r = _scan(ents, layout, geom)
    assert r["stale"] == []
    d = r["drift"][0]
    assert d["kind"] == "old-blocks" and d["codes"] == ["10000-center"]


def test_removed_records_are_not_live():
    e = _ent("10000-north", "10000-west", "10000-east")
    for k in list(e):
        if k.startswith("10000-north_"):
            e[k]["status"] = "removed"; e[k]["size"] = 0
    r = _scan(e, ["10000-west", "10000-east"])
    assert r == {"stale": [], "drift": []}


if __name__ == "__main__":
    for n, f in list(globals().items()):
        if n.startswith("test_"):
            f(); print("ok", n)
