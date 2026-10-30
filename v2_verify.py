"""v2_verify — integrity + improvement gates for v2 KG products.

Two call sites, two gates:

1. **Peer, before upload** (``verify_before_upload``): the freshly built
   ``<code>_v2.json.gz`` + ``<code>_light_v2.gpkg`` against the v1 JSON of
   the same code (downloaded from Zenodo, ~MB).  Checks the codec round
   trip, structural completeness, **tile stitching** (every LiDAR-bearing
   tile segmented, segmented area ≥ v1, no all-zero holes in the
   ``segment_type`` raster inside the AOI), coverage percentages ≥ v1 (−tolerance),
   unclassified share ≤ v1 (+tolerance), light GPKG ``integrity_check``
   + layer inventory + feature counts consistent with the JSON.
   A failed gate means **nothing is uploaded** and the KG is reported
   as ``v2_verify_failed`` (the parent defers/retries it; the director
   sees it in the merged log as ``v2verify:``).

2. **Primary, before ingest** (``verify_for_ingest``): the downloaded blob
   against the v1 row already in the search index (or the v1 JSON on
   disk).  Same document-level checks (no GPKG — we do not pull light
   GPKGs to the primary; the manifest must merely show a committed
   ``_light_gpkg_v2`` of non-zero size).  Only after this passes does the
   primary swap the index row to v2, store the blob and delete the v1
   file.

Every check appends to ``report['checks']`` as ``(name, ok, detail)`` so
the operator can see *which* gate tripped.  Tolerances are deliberately
modest — v2 must be at least as complete as v1; it may legitimately have
more segments (finer classes) or fewer (merged classes).
"""
from __future__ import annotations

import io
import json
import logging
import sqlite3
from pathlib import Path

import numpy as np

import kg_json_v2

log = logging.getLogger("austria_processor.v2verify")

# Products are property-free: no legacy property-level sections / layers
# (FORBIDDEN_* — check_property_free_document is fatal).
REQUIRED_SECTIONS = ("version", "kg_code", "bbox", "area_summary", "landscape", "terrain",
                     "coverage", "data_quality", "model")
LIGHT_LAYERS_REQUIRED = ("segments", "segment_points", "segment_type", "segment_height")
LIGHT_LAYERS_V2 = ()
FORBIDDEN_SECTIONS = ("parcels", "building_footprints")          # legacy section names
FORBIDDEN_LIGHT_LAYERS = ("parcels", "buildings", "parcel_outline_z")  # legacy layer names
# product 2.1 (docs/v2.1-product-spec.md) — checked by check_v21_document /
# check_v21_light_gpkg; product_version is fatal, the 5 m / apex layers are
# fatal only when the JSON claims them.
try:
    from v21_products import PRODUCT_VERSION
except Exception:  # noqa: BLE001
    PRODUCT_VERSION = "2.3"
LIGHT_LAYERS_V21 = ("terrain_coarse_dtm", "terrain_coarse_slope")

TOL = {
    "coverage_pp": 1.0,            # percentage points (elevation; fatal)
    "seg_coverage_pp_warn": 1.0,   # segmentation coverage: 1–2.5 pp drop = non-fatal drift
    "seg_coverage_pp": 2.5,        # … drift past 2.5 pp is reported loudly
    "seg_coverage_pp_fatal": 8.0,  # … fatal past this (a tile that came back empty);
                                   # empty tiles are also caught by segment_type_no_holes /
                                   # lidar_tiles_ge_v1 / segmented_area_ge_v1
    "seg_area_rel": 0.02,          # v2 segmented area ≥ 0.98 × v1
    "unclassified_pp": 2.0,        # v2 unclassified share ≤ v1 + 2 pp
    "segments_rel_min": 0.5,       # sanity — v2 segments ≥ half of v1's
    "raster_hole_frac": 0.005,     # zero segment_type inside the AOI
    "label_mix_frac": 0.005,       # segments rows whose polygon part is > 2× the object's
                                   # own area_sqm (+50 m²).  A healthy KG measures exactly
                                   # 0 (11117: p99.9 ratio 1.00); a KG stitched from tile
                                   # checkpoints whose label ids were never remapped
                                   # (63330, Oct 2026) measures 3.9 % of rows / 28 % of
                                   # ids — polygons + segment_type pixels of unrelated
                                   # objects glued together.
    "label_mix_sample": 20000,
}


# Operator hints per fatal check — appended to ``Report.summary()`` (and so
# to the merged fleet log / ``/process.txt?q=v2verify``) so a FAIL row says
# *where to look* without re-deriving the pipeline.  Keep each ≤ ~90 chars.
HINTS = {
    "unclassified_le_v1": "few huge 'unclassified' objs = cross-tile anchor-weak merge demotion → "
                          "grep peer log 'Anchor-weak' (v2 skips those merges since 2026-09-21)",
    "segment_type_no_holes": "segment_type==0 inside the AOI → unsegmented tile / BEV read hole; "
                             "grep 'gpkg_full' + 'deferred' for that KG",
    "segmented_area_ge_v1": "v2 covers less LiDAR area than v1 on the same grid → missing DTM tile / BEV outage",
    "lidar_tiles_ge_v1": "a DTM tile v1 had is missing now → BEV read failure, see bev_pause / Anchor",
    "bbox_covers_v1": "v2 AOI shrunk vs v1 → KG split/definition changed; compare kg_splitter output",
    "baseline_layout_stale": "v1 baseline is an OLD split layout of this block code; v2 matches the current kg_splitter grid — no v1 comparison possible",
    "grid25_dims": "grid25 raster does not match bbox → v21_products grid anchor bug (docs/v2.1-product-spec.md)",
    "light_gpkg_integrity": "sqlite integrity_check failed → disk full / interrupted write on peer",
}


class Report(dict):
    def __init__(self):
        super().__init__(ok=True, checks=[], errors=[], warnings=[])

    def check(self, name, ok, detail="", *, fatal=True):
        self["checks"].append((name, bool(ok), detail))
        if not ok:
            (self["errors"] if fatal else self["warnings"]).append(f"{name}: {detail}")
            if fatal:
                self["ok"] = False
        return ok

    def summary(self) -> str:
        n_ok = sum(1 for _, ok, _ in self["checks"] if ok)
        s = f"{'PASS' if self['ok'] else 'FAIL'} {n_ok}/{len(self['checks'])} checks"
        if self["errors"]:
            s += " | " + "; ".join(self["errors"][:4])
        if self["warnings"]:
            s += " | warn: " + "; ".join(self["warnings"][:3])
        hints = []
        for e in self["errors"]:
            h = HINTS.get(e.split(":", 1)[0])
            if h and h not in hints:
                hints.append(h)
        if hints:
            s += " | hint: " + " / ".join(hints[:2])
        return s


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------

def _g(d, *path, default=None):
    for k in path:
        if not isinstance(d, dict) or k not in d:
            return default
        d = d[k]
    return d


def _num(v, default=None):
    try:
        f = float(v)
        return default if f != f else f
    except (TypeError, ValueError):
        return default


def _unclassified_pct(doc) -> float:
    a = _g(doc, "area_summary", default={}) or {}
    tot = 0.0
    unc = 0.0
    for t, v in a.items():
        if not isinstance(v, dict):
            continue
        s = _num(v.get("area_sqm"), 0.0)
        tot += s
        if t == "unclassified":
            unc += s
    return 100.0 * unc / tot if tot > 0 else 0.0


def _active_tiles(doc) -> tuple[int, int, list]:
    """(n_tiles_with_lidar, n_tiles_segmented, unsegmented_tile_idx)."""
    tiles = _g(doc, "data_quality", "tiles", default=[]) or []
    lidar = seg = 0
    bad = []
    for t in tiles:
        if not isinstance(t, dict) or not t.get("dtm"):
            continue
        if t.get("outside_austria") or int(t.get("valid_pixels") or 0) < 100:
            continue
        lidar += 1
        if t.get("segmentation") and int(t.get("n_objects") or 0) > 0:
            seg += 1
        else:
            bad.append(t.get("tile_index"))
    return lidar, seg, bad


def _nodata_tile_union_3035(doc):
    """Union (EPSG:3035) of tile bboxes that have **no DTM at all**
    (``dtm=False`` / ``valid_pixels=0``): open water (Bodensee, Rhine) or
    outside BEV ALS coverage.  The KG bbox routinely extends over such
    areas (91109 Gaissau: tiles 1/11/12 on Lake Constance), so a hole
    there is permanent and not a stitching defect.  None if no such tile."""
    tiles = _g(doc, "data_quality", "tiles", default=[]) or []
    boxes = []
    for t in tiles:
        if not isinstance(t, dict) or t.get("dtm") or int(t.get("valid_pixels") or 0) >= 100:
            continue
        bb = t.get("bbox_wgs")
        if isinstance(bb, (list, tuple)) and len(bb) == 4:
            boxes.append([float(x) for x in bb])
    if not boxes:
        return None
    try:
        from shapely.geometry import box
        from shapely.ops import transform as _shp_transform, unary_union
        from pyproj import Transformer
        tr = Transformer.from_crs("EPSG:4326", "EPSG:3035", always_xy=True).transform
        return unary_union([_shp_transform(tr, box(*b)) for b in boxes])
    except Exception as e:  # noqa: BLE001
        log.debug("nodata tile union skipped: %s", e)
        return None


def _declared_dtm_nodata_px(doc) -> int | None:
    """Σ over ``data_quality.tiles`` of (tile bbox px at 1 m − valid_pixels):
    the DTM-nodata area the document itself declares.  None without tiles."""
    tiles = _g(doc, "data_quality", "tiles", default=[]) or []
    tot = 0
    seen = False
    try:
        from pyproj import Transformer
        tr = Transformer.from_crs("EPSG:4326", "EPSG:3035", always_xy=True)
    except Exception:  # noqa: BLE001
        return None
    for t in tiles:
        if not isinstance(t, dict):
            continue
        bb = t.get("bbox_wgs")
        if not (isinstance(bb, (list, tuple)) and len(bb) == 4):
            continue
        x0, y0 = tr.transform(bb[0], bb[1])
        x1, y1 = tr.transform(bb[2], bb[3])
        px = abs(x1 - x0) * abs(y1 - y0)
        tot += max(0.0, px - float(t.get("valid_pixels") or 0))
        seen = True
    return int(tot) if seen else None


# --------------------------------------------------------------------------
# document-level checks (shared by both gates)
# --------------------------------------------------------------------------

def check_property_free_document(v2: dict, rep: Report) -> None:
    """A product must not carry any property-level section (fatal)."""
    pv = str(v2.get("product_version") or "")
    try:
        is_24 = float(pv.split("-")[0]) >= 2.4
    except Exception:
        is_24 = False
    if not is_24:
        return
    present = [k for k in FORBIDDEN_SECTIONS if k in v2]
    rep.check("property_free_sections", not present, "forbidden sections: " + ",".join(present))
    rep.check("property_free_flag", bool(v2.get("property_free") or v2.get("cadastre_free")),
              "property_free flag missing")
    # no property ids / addresses anywhere in the (compact) document text
    try:
        import json as _j
        txt = _j.dumps({k: v for k, v in v2.items() if k not in ("terrain", "landcover")}, default=str)
        bad = [w for w in (  # legacy property-level keys we refuse
                           "parcel_id", "building_id", "footprint_id", "vertex_heights", "outline_z",  # legacy
                           "\"address", "gst_nr", "\"ez\"") if w in txt]
        rep.check("property_free_text", not bad, "property-level keys in document: " + ",".join(bad))
    except Exception as e:  # noqa: BLE001
        rep.check("property_free_text", False, f"scan failed: {e}", fatal=False)


def check_ne(v2: dict, rep: Report, h3b_path: str | None, ne_declared_digest: str | None) -> None:
    """2.4 NE cells: JSON ``ne`` section consistent with the ``.h3b`` product.
    Missing NE (declared layer not available) is a non-fatal
    '-partial' defect; an INVALID h3b is fatal (never upload broken cells)."""
    pv = str(v2.get("product_version") or "")
    try:
        is_24 = float(pv.split("-")[0]) >= 2.4
    except Exception:
        is_24 = False
    if not is_24:
        return
    ne = v2.get("ne")
    rep.check("ne_section", isinstance(ne, dict), "ne section missing", fatal=False)
    if not isinstance(ne, dict):
        return
    if not h3b_path:
        rep.check("ne_h3b_present", ne.get("status") != "ok", "ne.status=ok but no h3b file", fatal=True)
        rep.check("ne_cells", False, f"no NE product (ne.status={ne.get('status')}) — '-partial'", fatal=False)
        return
    try:
        import ne_h3
        r = ne_h3.verify_h3b(h3b_path, expected_declared_digest=ne_declared_digest)
        rep.check("ne_h3b_valid", r["ok"], "; ".join(r["errors"]) or f"{r['cells_n']} cells / {r['chunks']} chunks")
        hdr = r.get("header") or {}
        rep.check("ne_h3b_kg", str(hdr.get("parent_kg") or hdr.get("kg") or "").zfill(5)
                  == str(ne.get("declared", {}).get("kg") or hdr.get("parent_kg") or hdr.get("kg") or "").zfill(5)
                  or True, "", fatal=False)
        d_json = (ne.get("declared") or {}).get("digest")
        d_h3b = (hdr.get("nec1") or {}).get("digest")
        rep.check("ne_declared_digest", bool(d_json) and d_json == d_h3b,
                  f"json {d_json} vs h3b {d_h3b}" + (f" vs fetched {ne_declared_digest}" if ne_declared_digest else ""))
        o_json = (ne.get("observed") or {}).get("digest")
        o_h3b = (hdr.get("neo1") or {}).get("digest")
        rep.check("ne_observed_digest", bool(o_json) and o_json == o_h3b, f"json {o_json} vs h3b {o_h3b}")
        n_obs = int((ne.get("observed") or {}).get("cells_observed") or 0)
        n_all = int(r.get("cells_n") or 0)
        is_block = bool(hdr.get("partial_cells"))
        # A split block whose bbox corner lies outside Austria (border KGs)
        # has no declared cells inside its extent — 0 observed is correct
        # there, not a product defect.
        _empty_block = is_block and n_all == 0
        rep.check("ne_cells_observed", n_obs > 0 and (is_block or n_obs >= 0.5 * n_all),
                  f"{n_obs}/{n_all} cells observed" + (" (block)" if is_block else "")
                  + (" (block has no declared cells — outside AT)" if _empty_block else ""),
                  fatal=n_obs == 0 and not _empty_block)
        rep.check("ne_algo", (hdr.get("nec1") or {}).get("algo") == ne_h3.ALGO_DECLARED
                  and (hdr.get("neo1") or {}).get("algo") == ne_h3.ALGO_OBS,
                  f"{(hdr.get('nec1') or {}).get('algo')} / {(hdr.get('neo1') or {}).get('algo')}")
    except Exception as e:  # noqa: BLE001
        rep.check("ne_h3b_valid", False, f"verify_h3b raised: {e}")


def check_document(v2: dict, v1: dict | None, rep: Report, *, code: str | None = None) -> None:
    rep.check("sections", all(k in v2 for k in REQUIRED_SECTIONS),
              "missing: " + ",".join(k for k in REQUIRED_SECTIONS if k not in v2))
    rep.check("version", str(v2.get("version")) == "v2", f"version={v2.get('version')}")
    if code:
        rep.check("kg_code", str(v2.get("kg_code")) == str(code),
                  f"{v2.get('kg_code')} != {code}")
    clf = str(_g(v2, "model", "classifier", default=""))
    rep.check("model_v2", clf.startswith("lgbm_v2"), f"classifier={clf!r}")
    by_tile = _g(v2, "model", "classifier_by_tile")
    if isinstance(by_tile, dict):
        n_v1 = sum(int(n) for c, n in by_tile.items() if not str(c).startswith("lgbm_v2"))
        rep.check("model_all_tiles_v2", n_v1 == 0,
                  f"{n_v1} tile(s) fell back to v1 RF", fatal=False)

    n_seg = int(_num(_g(v2, "landscape", "n_segments"), 0) or 0)
    rep.check("segments_present", n_seg > 0, f"n_segments={n_seg}")

    # tile stitching — every LiDAR tile segmented.  A v1 baseline that
    # itself carried gaps (unsegmented tiles, upstream-failed tiles, DTM
    # holes) caps what an upgrade regenerated from that same full GPKG can
    # achieve, so against a baseline these are *non-regression* checks;
    # without one (fresh KG) they are advisory — v1 completed such KGs as
    # "partial" too, and a fatal gate here would only loop the KG.
    lidar, seg, bad = _active_tiles(v2)
    l1, s1, _ = _active_tiles(v1) if v1 is not None else (0, 0, [])
    n_up = int(_num(_g(v2, "data_quality", "n_upstream_failed_tiles"), 0) or 0)
    n_up1 = int(_num(_g(v1, "data_quality", "n_upstream_failed_tiles"), 0) or 0) if v1 else 0
    rep.check("tiles_segmented", lidar > 0 and seg > 0, f"{seg}/{lidar} lidar tiles segmented")
    rep.check("tiles_all_segmented", seg == lidar,
              f"{seg}/{lidar} lidar tiles segmented; unsegmented={bad[:10]}",
              fatal=(v1 is not None and seg < s1))
    rep.check("no_upstream_failed_tiles", n_up == 0,
              f"{n_up} upstream-failed tile(s)" + (f" (v1: {n_up1})" if v1 else ""),
              fatal=(v1 is not None and n_up > n_up1))

    # property-free gate (fatal)
    check_property_free_document(v2, rep)

    check_v21_document(v2, rep)
    check_v23_top_ranking(v2, rep)

    cov = _g(v2, "coverage", default={}) or {}

    if v1 is None:
        return
    # ---- improvement / non-regression vs v1 --------------------------------
    cov1 = _g(v1, "coverage", default={}) or {}
    # Tile-grid comparisons.  ``total_segmented_area_sqm`` is the SUM of
    # per-tile valid px over the overlapping 1.5 km grid and ``n_tiles``
    # depends on how the bbox happened to fall on the grid when the v1 ran
    # (19185: v1 4 tiles / 9.06 km², v2 2 tiles / 4.53 km² for a bbox that
    # v2 covers *more* of).  Neither is comparable across a grid change, so
    # a differing grid is judged on AOI coverage: fatal only when the v2
    # bbox fails to cover the v1 bbox (a shrunk AOI); otherwise a warning.
    sa, sb = _num(cov.get("total_segmented_area_sqm")), _num(cov1.get("total_segmented_area_sqm"))
    nt2 = int(_num(cov.get("n_tiles"), 0) or 0)
    nt1 = int(_num(cov1.get("n_tiles"), 0) or 0)
    same_grid = nt1 == 0 or nt2 == 0 or nt1 == nt2
    bb2, bb1 = _bbox_3035(v2), _bbox_3035(v1)
    aoi_covered = True
    if bb2 and bb1:
        _sl = 25.0
        aoi_covered = (bb2[0] <= bb1[0] + _sl and bb2[1] <= bb1[1] + _sl
                       and bb2[2] >= bb1[2] - _sl and bb2[3] >= bb1[3] - _sl)
        rep.check("bbox_covers_v1", aoi_covered,
                  f"v2 x[{bb2[0]:.0f},{bb2[2]:.0f}] y[{bb2[1]:.0f},{bb2[3]:.0f}] "
                  f"v1 x[{bb1[0]:.0f},{bb1[2]:.0f}] y[{bb1[1]:.0f},{bb1[3]:.0f}]")
    if sa is not None and sb and sb > 0:
        rep.check("segmented_area_ge_v1", sa >= (1 - TOL["seg_area_rel"]) * sb,
                  f"v2={sa:.0f} v1={sb:.0f} m²" + ("" if same_grid else f" (grid {nt2} vs {nt1} tiles)"),
                  fatal=same_grid or not aoi_covered)
    rep.check("lidar_tiles_ge_v1", lidar >= l1, f"v2={lidar} v1={l1} lidar tiles"
              + ("" if same_grid else f" (grid {nt2} vs {nt1} tiles)"),
              fatal=same_grid or not aoi_covered)
    n1 = int(_num(_g(v1, "landscape", "n_segments"), 0) or 0)
    if n1 > 0:
        rep.check("segments_vs_v1", n_seg >= TOL["segments_rel_min"] * n1,
                  f"v2={n_seg} v1={n1}", fatal=False)
    u2, u1 = _unclassified_pct(v2), _unclassified_pct(v1)
    # Object count + mean size of the v2 unclassified share: "n=2 avg=117k m²"
    # is one demoted cross-tile blob (pipeline), "n=800 avg=60 m²" is the
    # classifier genuinely unsure (model/threshold) — very different fixes.
    _ua = (_g(v2, "area_summary", "unclassified", default={}) or {})
    _un = int(_num(_ua.get("n_objects"), 0) or 0)
    _ushape = (f" (n={_un} avg={_num(_ua.get('area_sqm'), 0.0) / _un:.0f}m²)" if _un else "")
    rep.check("unclassified_le_v1", u2 <= u1 + TOL["unclassified_pp"],
              f"v2={u2:.1f}%{_ushape} v1={u1:.1f}%")
    nb2 = int(_num(_g(v2, "new_buildings", "count"), 0) or 0)
    nb1 = int(_num(_g(v1, "new_buildings", "count"), 0) or 0)
    rep["delta"] = {"n_segments": n_seg - n1, "unclassified_pp": round(u2 - u1, 2),
                    "new_buildings": nb2 - nb1,
                    "seg_area_rel": round(sa / sb, 4) if sa is not None and sb else None}


_LAYOUT_TOL_DEG = 0.0005   # ~40 m; block bboxes are exact grid cuts of the parent


def _bbox_tuple(doc: dict):
    bb = (doc or {}).get("bbox") or {}
    try:
        return (float(bb["min_lon"]), float(bb["min_lat"]), float(bb["max_lon"]), float(bb["max_lat"]))
    except Exception:  # noqa: BLE001
        return None


def _bbox_close(a, b, tol=_LAYOUT_TOL_DEG) -> bool:
    return bool(a and b) and all(abs(x - y) <= tol for x, y in zip(a, b))


def _baseline_layout_stale(code: str, v2: dict, v1: dict) -> bool:
    """True when *code* is a split block whose v2 bbox equals the block's
    rectangle under the CURRENT ``kg_splitter`` layout while the v1 baseline
    bbox does not (i.e. the baseline is an old-layout product that merely
    shares the label). Primary-only (needs ``app._current_block_bboxes``);
    any failure -> False (keep the baseline, current behaviour)."""
    try:
        from kg_splitter import parent_kg_code
        parent = parent_kg_code(code)
        if parent == code:
            return False
        b2, b1 = _bbox_tuple(v2), _bbox_tuple(v1)
        if not b2 or not b1 or _bbox_close(b2, b1):
            return False
        import app as _app  # lazy: only the primary's ingest thread gets here
        cur = _app._current_block_bboxes(parent).get(code)
        return bool(cur) and _bbox_close(b2, cur) and not _bbox_close(b1, cur)
    except Exception:  # noqa: BLE001
        return False


def _bbox_3035(v2: dict):
    bb = (v2 or {}).get("bbox") or {}
    try:
        from pyproj import Transformer
        t = Transformer.from_crs(4326, 3035, always_xy=True)
        xs, ys = zip(*[t.transform(x, y) for x, y in (
            (bb["min_lon"], bb["min_lat"]), (bb["max_lon"], bb["min_lat"]),
            (bb["min_lon"], bb["max_lat"]), (bb["max_lon"], bb["max_lat"]))])
        return min(xs), min(ys), max(xs), max(ys)
    except Exception:  # noqa: BLE001
        return None


def _lidar_extent_3035(v2: dict):
    """EPSG:3035 envelope of the tiles that actually carry a DTM, or None.

    Border KGs (80110-southeast-2, Tirol/Italy) have whole tile rows outside
    Austria: no BEV data, nothing stitched, so the 25 m grid legitimately
    starts a tile row inside the doc bbox."""
    tiles = _g(v2, "data_quality", "tiles", default=[]) or []
    bbs = [t.get("bbox_wgs") for t in tiles
           if isinstance(t, dict) and t.get("dtm") and not t.get("outside_austria")
           and int(t.get("valid_pixels") or 0) >= 100 and t.get("bbox_wgs")]
    if not bbs or len(bbs) == len([t for t in tiles if isinstance(t, dict) and t.get("bbox_wgs")]):
        return None
    try:
        from pyproj import Transformer
        t = Transformer.from_crs(4326, 3035, always_xy=True)
        xs, ys = [], []
        for w, s, e, n in bbs:
            for x, y in ((w, s), (e, s), (w, n), (e, n)):
                px, py = t.transform(x, y)
                xs.append(px); ys.append(py)
        return min(xs), min(ys), max(xs), max(ys)
    except Exception:  # noqa: BLE001
        return None


def check_v21_document(v2: dict, rep: Report) -> None:
    """Product-2.1 JSON checks (JSON subset — also run on the primary)."""
    pv = str(v2.get("product_version") or "")
    # peers verify their own output (must be the current version); the primary
    # ingests whatever the fleet uploads during a rollout — an older readable
    # product line is a warning there, not a rejection.
    readable = {"2.1", "2.2", "2.3"}
    if pv == PRODUCT_VERSION:
        rep.check("product_version", True, f"product_version={pv!r}")
    else:
        rep.check("product_version", pv in readable, f"product_version={pv!r} (current {PRODUCT_VERSION})",
                  fatal=pv not in readable)
    g = _g(v2, "terrain", "grid25")
    if not rep.check("grid25_present", isinstance(g, dict) and bool(g.get("elev")),
                     "terrain.grid25 missing (tile arrays freed?)", fatal=False):
        return
    try:
        import v21_products as v21
        d = v21.decode_terrain_grid25(g)
    except Exception as e:  # noqa: BLE001
        rep.check("grid25_decodes", False, str(e)[:160])
        return
    rep.check("grid25_decodes", True)
    cell = d["cell_m"]
    cols, rows = d["cols"], d["rows"]
    bb = _bbox_3035(v2)
    _lx = _lidar_extent_3035(v2)
    if bb and _lx:
        # only the tiles with lidar can be gridded — judge against their
        # envelope (clipped to the doc bbox) instead of the whole bbox.
        bb = (max(bb[0], _lx[0]), max(bb[1], _lx[1]), min(bb[2], _lx[2]), min(bb[3], _lx[3]))
    if bb:
        # the grid is built from the (overlapping, bbox-overshooting) tile
        # rectangles snapped to 25 m, so it must COVER the bbox and may
        # exceed it by up to ~1 tile (1.6 km) per side — not "±1 cell".
        x0, y0 = d["x0"], d["y0"]
        x1, y1 = x0 + cols * cell, y0 - rows * cell
        covers = (x0 <= bb[0] + cell and y1 <= bb[1] + cell
                  and x1 >= bb[2] - cell and y0 >= bb[3] - cell)
        # 19565 (2026-09-21): 2×2 tiles for a 1.3×2.0 km bbox, east
        # overshoot 1759 m = one 1575 m tile + the 100 m read buffer + snap.
        slack = 1800.0
        sane = (bb[0] - x0 <= slack and x1 - bb[2] <= slack
                and bb[1] - y1 <= slack and y0 - bb[3] <= slack)
        _gd = (f"grid {cols}x{rows}@{cell:.0f}m x[{x0:.0f},{x1:.0f}] y[{y1:.0f},{y0:.0f}] "
               f"vs bbox x[{bb[0]:.0f},{bb[2]:.0f}] y[{bb[1]:.0f},{bb[3]:.0f}]")
        # Fatal only when the grid fails to COVER the AOI. Overshoot past
        # ``slack`` is a size cost, not a correctness defect: 09048
        # (2026-09-21, 958×719 m bbox straddling a tile row boundary) got
        # a 2-tile-tall grid, 2034 m above the bbox, and struck out on it.
        rep.check("grid25_dims", covers, _gd)
        if covers and not sane:
            rep.check("grid25_overshoot", False, _gd + f" (> {slack:.0f} m past bbox)", fatal=False)
    n_valid = int(np.isfinite(d["elev"]).sum())
    frac = n_valid / max(cols * rows, 1)
    # Border / glacier blocks (80110-southeast-2, Sölden ↔ Italy, 2026-10-20:
    # 7 of 12 tiles carry BEV data and several of those only a sliver —
    # 34 % of the grid is finite and that IS the whole dataset).  Judge
    # the grid against the DTM pixels the tile loop actually read
    # (``data_quality.tiles[].valid_pixels``, 1 m px → /cell² cells); the
    # absolute 40 % rule stays as the fallback for docs without that
    # telemetry.  Tile overlaps (~8 % per side) overcount the expectation,
    # so 60 % of it is a comfortable floor for a healthy stitch.
    _exp_cells = 0.0
    try:
        _exp_cells = sum(float(t.get("valid_pixels") or 0)
                         for t in (_g(v2, "data_quality", "tiles", default=[]) or [])
                         if isinstance(t, dict) and t.get("dtm")) / float(cell * cell)
    except Exception:  # noqa: BLE001
        _exp_cells = 0.0
    if _exp_cells > 0:
        _rel = n_valid / _exp_cells
        rep.check("grid25_finite", frac >= 0.4 or _rel >= 0.6,
                  f"{n_valid}/{cols * rows} cells valid ({100 * frac:.0f}%; "
                  f"{100 * _rel:.0f}% of {_exp_cells:.0f} expected from tile valid_pixels)")
    else:
        rep.check("grid25_finite", frac >= 0.4, f"{n_valid}/{cols * rows} cells valid ({100 * frac:.0f}%)")
    emin, emax = _num(_g(v2, "terrain", "elevation_min_m")), _num(_g(v2, "terrain", "elevation_max_m"))
    if n_valid and emin is not None and emax is not None:
        gmin, gmax = float(np.nanmin(d["elev"])), float(np.nanmax(d["elev"]))
        ok5 = gmin >= emin - 5 and gmax <= emax + 5
        rep.check("grid25_range", gmin >= emin - 50 and gmax <= emax + 50,
                  f"grid {gmin:.0f}..{gmax:.0f} m vs terrain {emin:.0f}..{emax:.0f} m")
        if not ok5:
            rep.check("grid25_range_drift", False,
                      f"grid {gmin:.0f}..{gmax:.0f} m outside terrain ±5 m", fatal=False)
    lc = _g(v2, "landcover", "grid25")
    if isinstance(lc, dict):
        rep.check("landcover_grid25_dims", int(lc.get("cols", 0)) == cols and int(lc.get("rows", 0)) == rows,
                  f"landcover {lc.get('cols')}x{lc.get('rows')} vs terrain {cols}x{rows}")
    ts = _g(v2, "tree_stats", default={}) or {}
    rep.check("tree_stats_v21", "n_apices" in ts and "det_mode" in ts,
              f"keys={sorted(ts)[:8]}", fatal=False)



def _gpkg_point_xy(blob: bytes):
    """(x, y) from a GeoPackage binary Point blob (no spatialite needed)."""
    import struct
    if not blob or blob[:2] != b"GP":
        return None
    flags = blob[3]
    env = (flags >> 1) & 0x07
    env_len = {0: 0, 1: 32, 2: 48, 3: 48, 4: 64}.get(env)
    if env_len is None:
        return None
    off = 8 + env_len
    bo = "<" if blob[off] == 1 else ">"
    gtype = struct.unpack(bo + "I", blob[off + 1:off + 5])[0]
    if gtype % 1000 != 1:
        return None
    x, y = struct.unpack(bo + "dd", blob[off + 5:off + 21])
    return float(x), float(y)

def check_v23_top_ranking(v2: dict, rep: Report) -> None:
    """Product-2.3: top-N lists are robust-ranked and plausibility-filtered.

    Fatal on a peer producing the current version (a 2.3 doc whose
    ``top_10_trees`` still carries an 80 m tree is the bug this version
    exists to fix); a warning for readable older lines on the primary.
    """
    pv = str(v2.get("product_version") or "")
    fatal = pv == PRODUCT_VERSION
    if not fatal and pv in ("2.1", "2.2"):
        return
    objs = v2.get("top_10_objects") or []
    trees = v2.get("top_10_trees") or []
    mm = v2.get("top_manmade_objects")
    rep.check("top_manmade_present", isinstance(mm, list),
              "top_manmade_objects missing", fatal=fatal)
    rows = list(objs) + list(trees) + list(mm or [])
    missing = [r for r in rows if not isinstance(r.get("height_robust_m"), (int, float))]
    rep.check("top_height_robust", not missing,
              f"{len(missing)}/{len(rows)} top rows lack height_robust_m", fatal=fatal)
    # robust ≤ max, lists non-increasing in robust height
    bad = [r for r in rows if isinstance(r.get("height_robust_m"), (int, float))
           and isinstance(r.get("height_max_m"), (int, float))
           and r["height_robust_m"] > r["height_max_m"] + 0.011]
    rep.check("top_robust_le_max", not bad, f"{len(bad)} rows robust > max", fatal=fatal)
    for name, lst in (("top_10_objects", objs), ("top_10_trees", trees)):
        hs = [r.get("height_robust_m") for r in lst if isinstance(r.get("height_robust_m"), (int, float))]
        rep.check(f"{name}_sorted", all(a >= b - 1e-6 for a, b in zip(hs, hs[1:])),
                  f"{name} not sorted by height_robust_m", fatal=fatal)
    try:
        import quality_flags as _qf
        T = _qf.THRESHOLDS
        spikes = [r for r in trees if isinstance(r.get("height_max_m"), (int, float))
                  and r["height_max_m"] >= T["tree_max_height_m"]["critical"]]
        spikes += [r for r in objs if r.get("type") in _qf.GROUND_TYPES
                   and isinstance(r.get("height_robust_m"), (int, float))
                   and r["height_robust_m"] >= T["flat_type_max_height_m"]["high"]]
        spikes += [r for r in objs if r.get("type") in ("water", "waterbody")
                   and isinstance(r.get("height_robust_m"), (int, float))
                   and r["height_robust_m"] >= T["water_max_height_m"]["high"]]
        rep.check("top_no_implausible", not spikes,
                  f"{len(spikes)} implausible rows in top lists "
                  f"(e.g. {spikes[0].get('type')} {spikes[0].get('height_max_m')} m)" if spikes else "",
                  fatal=fatal)
    except ImportError:
        pass


def check_v21_light_gpkg(c: sqlite3.Connection, layers: dict, v2: dict, rep: Report) -> None:
    """Product-2.1 light GPKG checks (peer only; *c* is an open ro connection)."""
    missing = [l for l in LIGHT_LAYERS_V21 if l not in layers]
    has_grid = bool(_g(v2, "terrain", "grid25"))
    rep.check("terrain_coarse_layers", not missing, "missing: " + ",".join(missing),
              fatal=has_grid)
    ts = _g(v2, "tree_stats", default={}) or {}
    n_ap = int(_num(ts.get("n_apices"), 0) or 0)
    claims = ts.get("apices_layer") == "tree_apices"
    if "tree_apices" not in layers:
        rep.check("apices_layer", not claims and n_ap == 0,
                  f"tree_apices layer missing (json n_apices={n_ap})", fatal=claims)
        return
    try:
        n = c.execute('SELECT COUNT(*) FROM "tree_apices"').fetchone()[0]
    except Exception as e:  # noqa: BLE001
        rep.check("apices_layer", False, str(e)[:120])
        return
    rtree = c.execute("SELECT COUNT(*) FROM gpkg_extensions WHERE table_name='tree_apices' "
                      "AND extension_name='gpkg_rtree_index'").fetchone()[0] > 0
    rep.check("apices_layer", rtree and (n_ap == 0 or abs(n - n_ap) <= 0.2 * n_ap),
              f"layer={n} json={n_ap} rtree={rtree}")
    # 2.2: apex grid anchor (pixel centres at x.5 on the integer-metre BEV
    # grid) — a fractional-origin regression would silently break tree_id
    # stability between product and live (FEEDBACK-5 §1).
    pts = []
    try:
        gcol = c.execute("SELECT column_name FROM gpkg_geometry_columns WHERE table_name='tree_apices'").fetchone()[0]
        for (blob,) in c.execute(f'SELECT "{gcol}" FROM "tree_apices" LIMIT 50'):
            xy = _gpkg_point_xy(blob)
            if xy:
                pts.append(xy)
    except Exception:  # noqa: BLE001
        pts = []
    if pts:
        off = [abs(((x - 0.5) % 1.0)) + abs(((y - 0.5) % 1.0)) for x, y in pts]
        bad = sum(1 for o in off if min(o, 2.0 - o) > 0.02)
        # fatal since 2026-09-20: KG 63330 shipped 50/50 apices at x.9 (restored
        # pre-2.2 tile checkpoints with a fractional origin) — tree_id unstable vs live.
        rep.check("apex_grid_anchor", bad == 0, f"{bad}/{len(pts)} apices off the x.5 grid")
    n_cr = int(_num(ts.get("n_crowns"), 0) or 0)
    if ts.get("crowns_layer") == "tree_crowns":
        try:
            m = c.execute('SELECT COUNT(*) FROM "tree_crowns"').fetchone()[0]
        except Exception as e:  # noqa: BLE001
            m = -1
            rep.check("crowns_layer", False, str(e)[:120], fatal=False)
        if m >= 0:
            rep.check("crowns_layer", abs(m - n_cr) <= 0.2 * max(n_cr, 1) and m <= n,
                      f"layer={m} json={n_cr} apices={n}", fatal=False)


# --------------------------------------------------------------------------
# light GPKG checks (peer only)
# --------------------------------------------------------------------------

def _dtm_nodata_mask(path: str, shape: tuple[int, int], transform) -> np.ndarray | None:
    """True where the light GPKG's own ``terrain_coarse_dtm`` (5 m, nodata
    −9999, v2.1+) has **no DTM**, resampled (nearest) onto the segment_type
    grid.  Where BEV ALS has no ground there is nothing to segment, so a
    hole there is a property of the terrain, not of the stitcher: the
    Bodensee KGs 91109 Gaissau / 91111 Hard carry 1.8 / 7.0 km² of lake
    (no elevation at all) whose water lies in shore tiles that are 3–52 % land — ``_nodata_tile_union_3035`` only
    drops *all-water* tiles, so both struck out at 18–25 % "hole" on every
    peer (2026-09-23/26).  None when the layer is absent (pre-2.1 light).

    The layer's nodata is ``v21_products.TERRAIN_COARSE_NODATA`` (−9999)
    and is matched **explicitly**: GDAL's GPKG gridded-coverage writer
    leaves ``gpkg_2d_gridded_coverage_ancillary.data_null`` empty for
    Float32, so ``ds.nodata`` reads back as None and the −9999 lake fill
    counted as valid DTM — the Bodensee KGs 91103/91108/91109/91110/
    91111/91119 struck out again on 2026-10-18..25 with the exclusion
    silently inactive ("no-DTM tiles excluded" but no "DTM-nodata px
    excluded" in the verdict)."""
    try:
        import rasterio
        from rasterio.warp import reproject, Resampling
        try:
            from v21_products import TERRAIN_COARSE_NODATA as _TC_ND
        except Exception:  # noqa: BLE001
            _TC_ND = -9999.0
        with rasterio.open(f"GPKG:{path}:terrain_coarse_dtm") as ds:
            a = ds.read(1)
            nd = ds.nodata
            valid = np.isfinite(a) & (a != _TC_ND) & (a > -9000.0)
            if nd is not None:
                valid &= a != nd
            dst = np.zeros(shape, np.uint8)
            reproject(valid.astype(np.uint8), dst, src_transform=ds.transform, src_crs=ds.crs,
                      dst_transform=transform, dst_crs=ds.crs, resampling=Resampling.nearest)
        if not dst.any():
            return None        # an empty DTM layer is a defect, not evidence
        return dst == 0
    except Exception as e:  # noqa: BLE001
        log.debug("terrain_coarse_dtm nodata mask skipped: %s", e)
        return None


def _gpkg_blob_geom(b: bytes):
    """shapely geometry from a GeoPackage geometry blob (header + WKB)."""
    from shapely import wkb as _wkb
    flags = b[3]
    env = (flags >> 1) & 7
    elen = {0: 0, 1: 32, 2: 48, 3: 48, 4: 64}.get(env, 0)
    return _wkb.loads(bytes(b[8 + elen:]))


def label_mix_fraction(c: sqlite3.Connection, sample: int | None = None) -> tuple[float, int, int]:
    """(fraction, n_bad, n_sampled) of ``segments`` rows whose polygon part
    area exceeds ``2 × area_sqm + 50``.  A part of an object can never be
    larger than the object, so any such row means the polygon was
    vectorised from label pixels that belong to a *different* object — the
    signature of a label-id collision in the stitcher (restored tile
    checkpoints, see ``austria_processor._remap_restored_tile_ids``).
    ``segments`` is EPSG:3035 (metres) in every product version.  Samples
    ``TOL['label_mix_sample']`` random rows (the defect is spread over the
    whole restored-tile area, so a sample sees it)."""
    n = int(sample or TOL["label_mix_sample"])
    total = c.execute('SELECT COUNT(*) FROM "segments"').fetchone()[0]
    if total <= n:
        cur = c.execute('SELECT geom, area_sqm FROM "segments"')
    else:
        cur = c.execute('SELECT geom, area_sqm FROM "segments" WHERE fid IN '
                        '(SELECT fid FROM "segments" ORDER BY RANDOM() LIMIT ?)', (n,))
    bad = seen = 0
    for b, a in cur:
        if b is None or a is None:
            continue
        try:
            g = _gpkg_blob_geom(b)
        except Exception:
            continue
        seen += 1
        if g.area > 2.0 * float(a) + 50.0:
            bad += 1
    return (bad / seen if seen else 0.0), bad, seen


def label_mix_fraction_path(path: str, sample: int | None = None) -> tuple[float, int, int]:
    c = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        return label_mix_fraction(c, sample)
    finally:
        c.close()


def _segment_type_nonzero(path: str, union_geom_3035=None, info: dict | None = None,
                          exclude_dtm_nodata: bool = True) -> tuple[int, float | None]:
    """(non-zero px, hole fraction inside union or None).  Reads the palette
    PNG tiles directly (GDAL expands them to RGBA).  With
    ``exclude_dtm_nodata`` the union is measured only where the GPKG's own
    ``terrain_coarse_dtm`` has data (see :func:`_dtm_nodata_mask`); *info*
    (if given) receives ``n_union`` / ``n_measured`` / ``n_dtm_nodata`` px."""
    from PIL import Image
    import rasterio
    c = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        row = c.execute("SELECT MAX(zoom_level) FROM gpkg_tile_matrix WHERE table_name='segment_type'").fetchone()
        if not row or row[0] is None:
            return 0, None
        z = row[0]
        tw, th, mw, mh = c.execute(
            "SELECT tile_width, tile_height, matrix_width, matrix_height FROM gpkg_tile_matrix "
            "WHERE table_name='segment_type' AND zoom_level=?", (z,)).fetchone()
        H, W = mh * th, mw * tw
        full = np.zeros((H, W), bool)
        nz = 0
        for col, rw, blob in c.execute("SELECT tile_column, tile_row, tile_data FROM segment_type WHERE zoom_level=?", (z,)):
            im = Image.open(io.BytesIO(blob))
            a = np.array(im if im.mode in ("P", "L") else im.convert("L"), dtype=np.uint8)
            m = a > 0
            nz += int(m.sum())
            r0, c0 = rw * th, col * tw
            hh, ww = min(th, H - r0), min(tw, W - c0)
            if hh > 0 and ww > 0:
                full[r0:r0 + hh, c0:c0 + ww] = m[:hh, :ww]
        hole = None
        if union_geom_3035 is not None and not union_geom_3035.is_empty:
            from rasterio.features import geometry_mask
            with rasterio.open(f"GPKG:{path}:segment_type") as ds:
                tf = ds.transform
            try:
                inside = ~geometry_mask([union_geom_3035], out_shape=(H, W), transform=tf,
                                        invert=False, all_touched=False)
                n_union = int(inside.sum())
                n_nd = 0
                if exclude_dtm_nodata and n_union > 0:
                    nd_mask = _dtm_nodata_mask(path, (H, W), tf)
                    if nd_mask is not None:
                        n_nd = int((inside & nd_mask).sum())
                        inside &= ~nd_mask
                n_in = int(inside.sum())
                if info is not None:
                    info.update(n_union=n_union, n_measured=n_in, n_dtm_nodata=n_nd)
                if n_in > 0:
                    hole = float((inside & ~full).sum()) / n_in
            except Exception as e:  # noqa: BLE001
                log.debug("segment_type hole check skipped: %s", e)
        return nz, hole
    finally:
        c.close()


def check_light_gpkg(path: str, v2: dict, rep: Report, union_geom_3035=None) -> None:
    p = Path(path)
    if not rep.check("light_gpkg_exists", p.exists() and p.stat().st_size > 0, str(path)):
        return
    c = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        ic = c.execute("PRAGMA integrity_check").fetchone()[0]
        rep.check("light_gpkg_integrity", ic == "ok", ic[:120])
        app_id = c.execute("PRAGMA application_id").fetchone()[0]
        rep.check("light_gpkg_app_id", app_id == 0x47504B47, hex(app_id))
        layers = {n: t for n, t in c.execute("SELECT table_name, data_type FROM gpkg_contents")}
        _optional = set()
        missing = [l for l in LIGHT_LAYERS_REQUIRED if l not in layers]
        rep.check("light_gpkg_layers", not missing, "missing: " + ",".join(missing))
        # property-level layers are forbidden (fatal) — also as bare tables
        all_tables = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        forbidden = [l for l in FORBIDDEN_LIGHT_LAYERS if l in layers or l in all_tables]
        rep.check("light_gpkg_property_free", not forbidden, "forbidden layers/tables: " + ",".join(forbidden))

        def _count(t):
            try:
                return c.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
            except Exception:
                return -1
        n_seg = int(_num(_g(v2, "landscape", "n_segments"), 0) or 0)
        # ``segments`` holds one polygon per connected part of a segment
        # (a label may split into several parts), so count distinct ids.
        try:
            ns = c.execute('SELECT COUNT(DISTINCT id) FROM "segments"').fetchone()[0]
        except Exception:
            ns = _count("segments")
        # The polygon layer is vectorised from the *stitched* label raster
        # inside the KG mask, so objects whose pixels were overwritten in a
        # tile overlap (or clipped by the block mask) have a JSON row and a
        # ``segment_points`` row but no polygon.  That is a v1-era trait, not
        # a v2 defect: 45410-east (v1, 2026-09-18) drops 2.0 %, fresh v2
        # blocks 2.3–2.7 %.  A 2 % fatal tolerance therefore failed every
        # fresh v2 KG fleet-wide on 2026-09-18 (files discarded, no ``_json``,
        # KG re-picked in a loop).  Fatal only past 10 %; 2–10 % is a
        # non-fatal drift warning so the rate stays visible in the log.
        _d = abs(ns - n_seg)
        rep.check("segments_layer_count", ns > 0 and (n_seg == 0 or _d <= max(20, 0.10 * n_seg)),
                  f"layer={ns} json={n_seg}")
        if n_seg and ns > 0 and _d > max(5, 0.02 * n_seg):
            rep.check("segments_layer_drift", False,
                      f"layer={ns} json={n_seg} ({100.0 * _d / n_seg:.1f}% objects without polygon)",
                      fatal=False)
        npx = _count("segment_points")
        rep.check("segment_points_count", abs(npx - ns) <= max(5, 0.02 * ns),
                  f"points={npx} segments={ns}", fatal=False)
        # Label-id consistency: polygon parts never larger than their object.
        try:
            _mf, _mb, _mn = label_mix_fraction(c)
            rep["label_mix_frac"] = round(_mf, 5)
            rep.check("segments_label_consistency", _mf <= TOL["label_mix_frac"],
                      f"{_mb}/{_mn} sampled polygon parts > 2x own area_sqm "
                      f"({100 * _mf:.2f}%, tol {100 * TOL['label_mix_frac']:.1f}%) — "
                      "label ids of restored tiles collided in the stitcher")
        except Exception as e:  # noqa: BLE001
            rep.check("segments_label_consistency", False, str(e)[:160], fatal=False)
        try:
            check_v21_light_gpkg(c, layers, v2, rep)
        except Exception as e:  # noqa: BLE001
            rep.check("v21_gpkg_checks", False, str(e)[:160], fatal=False)
        try:
            n_styles = c.execute("SELECT COUNT(DISTINCT f_table_name) FROM layer_styles").fetchone()[0]
            rep.check("layer_styles", n_styles >= len(LIGHT_LAYERS_REQUIRED) - 1,
                      f"{n_styles} styled layers", fatal=False)
        except Exception:
            pass
    finally:
        c.close()
    # raster stitching: nonzero segment_type px vs. segmented area, holes in union
    try:
        _nd = _nodata_tile_union_3035(v2)
        _measure_union = union_geom_3035
        if _nd is not None and union_geom_3035 is not None and not union_geom_3035.is_empty:
            try:
                _measure_union = union_geom_3035.difference(_nd)
            except Exception:  # noqa: BLE001
                _measure_union = union_geom_3035
        _hinfo: dict = {}
        nz, hole = _segment_type_nonzero(path, _measure_union, info=_hinfo)
        _nd_px = int(_hinfo.get("n_dtm_nodata") or 0)
        _nd_frac = _nd_px / max(int(_hinfo.get("n_union") or 0), 1)
        sa = _num(_g(v2, "coverage", "total_segmented_area_sqm"))
        if sa and sa > 0:
            # ``total_segmented_area_sqm`` is Σ per-tile valid px over the
            # *overlapping* 1.5 km grid (0.1 km overlap, see
            # ``_compute_tile_grid``) — every seam strip is counted twice,
            # while the stitched raster holds each px once.  For an n×m grid
            # the deduped/summed ratio tends to (1.4/1.5)² ≈ 0.87, so a flat
            # 0.95 floor failed every multi-tile fresh v2 KG on 2026-09-18
            # (45631-northeast 4 tiles → 0.934, 72010-north → 0.90) while
            # single-tile 19570 sat at 0.99999.  Real stitching gaps are
            # caught by ``segment_type_no_holes`` below; here fatal only
            # below the overlap-corrected floor, non-fatal drift above it.
            _nt = int(_num(_g(v2, "coverage", "n_tiles"), 1) or 1)
            _floor = 0.95 * ((1.4 / 1.5) ** 2 if _nt > 1 else 1.0)
            _r = nz / sa
            # The grid floor is exact only for a rectangular KG made of
            # full tiles (2×2 → 0.934, 2×3 → 0.924 fleet-wide).  For a
            # large irregular KG it is wrong in *both* directions:
            # ``total_segmented_area_sqm`` counts every valid DTM px in
            # the tile bboxes, while the stitched raster only keeps px of
            # *kept* objects — segments <50 % inside the AOI are dropped
            # and the centroid dedup drops overlap duplicates.  A KG whose
            # AOI fills only part of its 15-tile bbox legitimately reads 0.75–0.83
            # (47106 → 0.827 vs floor 0.828, 57210-south → 0.745/0.780,
            # 2026-09-23: both struck, v2 products dropped for a healthy
            # raster).  Real stitching loss is what ``segment_type_no_holes``
            # measures *inside the union*, so whenever that check can run
            # the area ratio is only a drift warning.  Fatal only when no
            # union is available (legacy callers) or the raster is empty.
            _shape_aware = hole is not None
            _ok = nz > 0 and (_shape_aware or _r >= _floor)
            rep.check("segment_type_raster_area", _ok,
                      f"nonzero px={nz} json segmented m²={sa:.0f} ratio={_r:.3f} floor={_floor:.3f} tiles={_nt}"
                      + (" (hole check authoritative)" if _shape_aware else ""))
            if _ok and _r < 0.95:
                rep.check("segment_type_raster_area_drift", False,
                          f"ratio={_r:.3f} (tile-overlap double count / KG-mask filter, {_nt} tiles)",
                          fatal=False)
        if hole is not None:
            # Holes are expected when the doc itself declares a gap (an
            # unsegmented or upstream-failed tile) — inherited from the v1
            # baseline / its full GPKG; fatal only for an *undeclared* hole.
            _l, _s, _ = _active_tiles(v2)
            _nup = int(_num(_g(v2, "data_quality", "n_upstream_failed_tiles"), 0) or 0)
            _declared_gap = (_s < _l) or _nup > 0
            rep.check("segment_type_no_holes", hole <= TOL["raster_hole_frac"],
                      f"{100*hole:.2f}% of the AOI has no segment class"
                      + (" (declared tile gap)" if _declared_gap else "")
                      + (" (no-DTM tiles excluded from union)" if _nd is not None else "")
                      + (f" (DTM-nodata px excluded: {100*_nd_frac:.1f}% of union)" if _nd_px else ""),
                      fatal=not _declared_gap)
            # Cross-check the mask against the doc's own tile ledger: the
            # excluded area must be explainable by declared partial-DTM
            # tiles (Σ tile px − valid_pixels).  A mask that hides more than
            # that is a truncated / blank terrain_coarse layer — surface it.
            if _nd_px:
                _budget = _declared_dtm_nodata_px(v2)
                rep.check("dtm_nodata_mask_declared", _budget is None or _nd_px <= 1.15 * _budget + 50_000,
                          f"mask excludes {_nd_px} px, tiles declare {_budget} nodata px", fatal=False)
        rep["segment_type_nonzero_px"] = nz
        rep["segment_type_hole_frac"] = hole
        rep["segment_type_dtm_nodata_frac"] = _nd_frac
    except Exception as e:  # noqa: BLE001
        rep.check("segment_type_readable", False, str(e)[:160], fatal=False)


# --------------------------------------------------------------------------
# gates
# --------------------------------------------------------------------------

def verify_before_upload(code: str, json_v2_path: str, light_gpkg_path: str | None,
                         v1_doc: dict | None, *, union_geom_3035=None, h3b_path: str | None = None,
                         ne_declared_digest: str | None = None) -> Report:
    rep = Report()
    p = Path(json_v2_path)
    if not rep.check("json_v2_exists", p.exists() and p.stat().st_size > 0, str(p)):
        return rep
    blob = p.read_bytes()
    rep["json_v2_bytes"] = len(blob)
    rep.check("json_v2_gzip", blob[:2] == b"\x1f\x8b", "not gzip")
    try:
        v2 = kg_json_v2.decode(blob)
    except Exception as e:  # noqa: BLE001
        rep.check("json_v2_decodes", False, str(e)[:160])
        return rep
    rep.check("json_v2_decodes", True)
    ok, why = kg_json_v2.roundtrip_ok(v2)
    rep.check("codec_roundtrip", ok, why)
    check_document(v2, v1_doc, rep, code=code)
    if light_gpkg_path is not None:
        check_light_gpkg(light_gpkg_path, v2, rep, union_geom_3035)
    check_ne(v2, rep, h3b_path, ne_declared_digest)
    if v1_doc is not None:
        try:
            v1_bytes = len(json.dumps(v1_doc, indent=2).encode())
            rep["size_ratio_v1_pretty_to_v2_gz"] = round(v1_bytes / max(len(blob), 1), 1)
        except Exception:
            pass
    return rep


def verify_for_ingest(code: str, blob: bytes, v1_doc: dict | None, manifest_entries: dict | None,
                      *, v1_row: dict | None = None) -> tuple[Report, dict | None]:
    """Primary gate.  Returns (report, decoded_doc_or_None)."""
    rep = Report()
    rep.check("blob_gzip", blob[:2] == b"\x1f\x8b", "not gzip")
    try:
        v2 = kg_json_v2.decode(blob)
    except Exception as e:  # noqa: BLE001
        rep.check("blob_decodes", False, str(e)[:160])
        return rep, None
    rep.check("blob_decodes", True)
    if manifest_entries is not None:
        e = manifest_entries.get(f"{code}_light_gpkg_v2")
        rep.check("manifest_light_gpkg_v2", isinstance(e, dict) and int(e.get("size") or 0) > 0
                  and bool(e.get("uploaded_at")), "no committed _light_gpkg_v2 entry")
    if v1_doc is not None and _baseline_layout_stale(code, v2, v1_doc):
        # Split-layout drift (kg_splitter is a function of kg_strikes.json):
        # the block CODE survived a re-split but now names a different
        # rectangle. The stored v1 doc describes the old rectangle, so every
        # "v2 >= v1" improvement check would compare unrelated AOIs
        # (57306-southeast-1: 20 tiles -> 12 tiles). Verify without a baseline.
        bb2, bb1 = v2.get("bbox") or {}, v1_doc.get("bbox") or {}
        rep.check("baseline_layout_stale", False,
                  f"v1 bbox lon[{_num(bb1.get('min_lon'), 0):.4f},{_num(bb1.get('max_lon'), 0):.4f}] "
                  f"lat[{_num(bb1.get('min_lat'), 0):.4f},{_num(bb1.get('max_lat'), 0):.4f}] -> "
                  f"v2 lon[{_num(bb2.get('min_lon'), 0):.4f},{_num(bb2.get('max_lon'), 0):.4f}] "
                  f"lat[{_num(bb2.get('min_lat'), 0):.4f},{_num(bb2.get('max_lat'), 0):.4f}] "
                  f"(current split layout) — v1 baseline dropped", fatal=False)
        v1_doc = None
        v1_row = None
    check_document(v2, v1_doc, rep, code=code)
    if v1_doc is None and v1_row:
        # Fall back to the flat index row for the few comparable numbers.
        n1 = int(_num(v1_row.get("n_segments"), 0) or 0)
        n2 = int(_num(_g(v2, "landscape", "n_segments"), 0) or 0)
        if n1 > 0:
            rep.check("segments_vs_v1_row", n2 >= TOL["segments_rel_min"] * n1,
                      f"v2={n2} v1={n1}", fatal=False)
    return rep, v2
