"""product_repair: missing_cells must judge the tile footprint, not the bbox.

Edge tiles overshoot the KG bbox (``_compute_tile_grid``) and parents
grow their bbox from the cadastre union, so a KG whose bbox ends at
13.778 can need the 13.8 cell (58032, 2026-10-19 → ``stuck``).
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import product_repair as pr  # noqa: E402


def test_footprint_from_tiles():
    doc = {"bbox": {"min_lon": 13.7289, "min_lat": 47.0912, "max_lon": 13.7776, "max_lat": 47.1242},
           "data_quality": {"tiles": [
               {"bbox_wgs": [13.7263, 47.0869, 13.7463, 47.1004], "segmentation": True, "valid_pixels": 10},
               {"bbox_wgs": [13.7817, 47.1121, 13.8017, 47.1257], "segmentation": True, "valid_pixels": 10},
               # inactive tiles do not count
               {"bbox_wgs": [20.0, 50.0, 20.1, 50.1], "upstream_fail": True},
               {"bbox_wgs": [21.0, 50.0, 21.1, 50.1], "outside_austria": True, "segmentation": True},
           ]}}
    fp = pr._footprint_of(doc)
    assert fp == [13.7263, 47.0869, 13.8017, 47.1257]
    assert pr.fetch_extent({"bbox": pr._bbox_of(doc), "fp": fp}) == fp


def test_padded_fallback_covers_tile_overshoot():
    bb = [13.7289, 47.0912, 13.7776, 47.1242]
    ext = pr.fetch_extent({"bbox": bb})
    assert ext[0] < bb[0] and ext[1] < bb[1]
    assert ext[2] >= bb[2] + 1.5 / (111 * 0.68)   # one tile east
    assert ext[3] >= bb[3] + 1.5 / 111             # one tile north
    assert pr.fetch_extent({"bbox": None}) is None
    assert pr.fetch_extent({}) is None


def test_missing_cells_uses_footprint():
    from tile_cache import CopernicusTileCache, HansenTileCache
    cached = {(13.7, 47.0), (13.7, 47.1)}          # bbox cells only
    cop = CopernicusTileCache.__new__(CopernicusTileCache)
    cop.GRID_STEP = 0.1
    def has_cached(cell, **kw):
        return (round(cell["west"], 1), round(cell["south"], 1)) in cached
    cop.has_cached = has_cached
    cop._iter_cells = lambda bb: CopernicusTileCache._iter_cells(cop, bb)
    cop._snap = lambda bb: CopernicusTileCache._snap(cop, bb)
    hansen = HansenTileCache.__new__(HansenTileCache)
    ent = {"bbox": [13.7289, 47.0912, 13.7776, 47.1242], "defects": ["harmonics"]}
    # bbox-only entry: padded → 13.8 column is missing
    miss = pr.missing_cells(ent, cop=cop, hansen=hansen)
    assert any(abs(m["w"] - 13.8) < 1e-6 for m in miss)
    # recorded footprint that stays inside the two cached cells → fixable
    ent["fp"] = [13.7263, 47.0869, 13.7990, 47.1257]
    assert pr.missing_cells(ent, cop=cop, hansen=hansen) == []
    # footprint crossing 13.8 → needs the extra cell
    ent["fp"] = [13.7263, 47.0869, 13.8017, 47.1257]
    miss = pr.missing_cells(ent, cop=cop, hansen=hansen)
    assert {(m["w"], m["s"]) for m in miss} == {(13.8, 47.0), (13.8, 47.1)}


if __name__ == "__main__":
    for _n, _f in sorted(globals().items()):
        if _n.startswith("test_") and callable(_f):
            _f()
            print("ok", _n)
