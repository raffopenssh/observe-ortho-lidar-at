"""v2.4 fast upgrade ("relabel") from an existing v2.3+ product pair.

The v2.4 product differs from v2.3 only by (a) being cadastre-free and
(b) carrying the NE cells layer (``<code>_ne.h3b`` + JSON section ``ne``).
Segments, trees, grid25, terrain — everything else is byte-identical.  So a
KG that already has a committed ``_light_gpkg_v2`` >= 2.3 and a ``_json_v2``
>= 2.3 (incl. ``-partial`` labels, e.g. ``v2.4-partial`` from the ``ne_cells``
defect) does NOT need the 1-2 h tile loop of the full upgrade path
(``process_one_kg(source_gpkg=…)`` re-reads the 2.5 GB full GPKG and
re-segments every tile).  Instead:

1. download the light GPKG (0.3-1 GB) + the gz JSON (<1 MB) + the v1 JSON
   (verify baseline), see :func:`fetch_inputs`;
2. copy the light GPKG and DROP the cadastre tables (``parcels``,
   ``buildings``, ``parcel_outline_z`` + their rtree / style / contents
   rows), VACUUM → ``<code>_light_v2.gpkg`` (2.4, cadastre-free);
3. rebuild the NE observed layer from the SAME arrays the fresh path uses,
   read back from the light GPKG: ``segment_type`` (1 m type raster),
   ``segment_height`` (1 m nDSM), ``segments`` polygons rasterised to a
   label raster, ``segment_points`` (one row per object, all attributes),
   ``tree_apices``, ``terrain_coarse_*`` (5 m dtm/slope/aspect); Hansen
   loss-year via the (cached) UMD tile, flight years from the JSON
   ``acquisition`` block;
4. ``strip_cadastre_from_doc`` on the v2.3 JSON + ``ne`` section +
   ``product_version`` 2.4 → ``<code>_v2.json.gz``.

The caller (``austria_processor._process_kg_v24_fast``) then runs the
normal 2.4 tail: ``v2_verify`` → upload light → h3b → json → NEO1 POST →
legacy clean-up.  Wall time: minutes instead of hours; traffic: the light
GPKG instead of the full one.  Quality: NEO1 from the light GPKG misses the
~5 % of objects that have no polygon (overwritten in tile overlaps) ONLY
in the label raster; per-object columns come from ``segment_points`` which
has every object, and the type / height rasters are the stitched ones, so
land-use mix, heights, trees, man-made rows and terrain are identical to
the fresh path.
"""
from __future__ import annotations

import json
import logging
import os
import shutil
import sqlite3
import time
from pathlib import Path
from types import SimpleNamespace

import numpy as np

log = logging.getLogger("v24_fast")

FAST_MIN_PRODUCT = 2.3            # light GPKG must carry tree_apices + terrain_coarse (2.1+) and trees v3.1 (2.3)
CADASTRE_TABLES = ("parcels", "buildings", "parcel_outline_z")
PATH_TAG = "v24_fast"


# === SECTION: eligibility ===

def _ver_num(v) -> float:
    try:
        return float(str(v or "").lstrip("v").split("-")[0])
    except Exception:
        return 0.0


def _ent(e, k, default=None):
    if e is None:
        return default
    if isinstance(e, dict):
        return e.get(k, default)
    return getattr(e, k, default)


def eligible(code: str, manifest) -> tuple[bool, str]:
    """(ok, reason).  *manifest* = ``zenodo_client.Manifest`` or raw map."""
    g2 = manifest.get(f"{code}_light_gpkg_v2")
    j2 = manifest.get(f"{code}_json_v2")
    if g2 is None or j2 is None:
        return False, "no v2 pair"
    if int(_ent(g2, "size", 0) or 0) <= 0:
        return False, "light_gpkg_v2 not committed"
    for e, name in ((g2, "light_gpkg_v2"), (j2, "json_v2")):
        st = str(_ent(e, "status", "") or "")
        if "error" in st or st == "removed":
            return False, f"{name} status={st}"
        if _ver_num(_ent(e, "version")) < FAST_MIN_PRODUCT:
            return False, f"{name} {_ent(e, 'version')} < {FAST_MIN_PRODUCT}"
    return True, f"light {_ent(g2, 'version')} + json {_ent(j2, 'version')}"


# === SECTION: inputs ===

def fetch_inputs(code: str, manifest, work_dir: Path, token: str, *, n_streams: int = 4,
                 min_free_gb: float = 3.0, step=None, doc_gate=None) -> dict:
    """Download light_v2 GPKG + json_v2 into *work_dir*.  Returns
    ``{"light_src", "json_v2_doc", "light_version", "json_version"}``.
    ``doc_gate(doc) -> bool``: evaluated right after the (tiny) json_v2 is
    decoded; when it returns False the light GPKG is NOT downloaded and
    ``{"json_v2_doc": doc, "declined": True}`` is returned (the caller
    takes the full path — e.g. a Copernicus-gap partial).
    Raises ``FileNotFoundError`` when a product is gone (404/403/410 →
    caller falls back to the full path), ``RuntimeError`` on transient /
    disk problems."""
    import sys as _sys
    _sv2 = str(Path(__file__).resolve().parent / "segv2")
    if _sv2 not in _sys.path:
        _sys.path.insert(0, _sv2)
    import gpkg_fetch as _gf
    import kg_json_v2
    import requests

    work_dir = Path(work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)
    g2 = manifest.get(f"{code}_light_gpkg_v2")
    j2 = manifest.get(f"{code}_json_v2")
    if g2 is None or j2 is None:
        raise FileNotFoundError(f"{code}: v2 pair missing from manifest")

    def _step(d):
        if step:
            step(d)

    # --- json_v2 (small, plain GET) ---
    jpath = work_dir / f"{code}_v2.src.json.gz"
    url = f"{_ent(j2, 'bucket_url')}/{_ent(j2, 'filename')}"
    _step(f"json_v2 {int(_ent(j2, 'size', 0) or 0) / 1e6:.1f} MB")
    r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=120)
    if r.status_code in (403, 404, 410):
        raise FileNotFoundError(f"{code}_json_v2 gone from Zenodo: HTTP {r.status_code}")
    if r.status_code != 200:
        raise RuntimeError(f"{code}_json_v2 download: HTTP {r.status_code}")
    jpath.write_bytes(r.content)
    try:
        doc = kg_json_v2.decode(r.content)
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"{code}_json_v2 undecodable: {e}") from e
    if str(doc.get("kg_code", code)).zfill(5) != str(code).split("-")[0].zfill(5) and "-" not in code:
        raise RuntimeError(f"{code}_json_v2: kg_code mismatch ({doc.get('kg_code')})")

    if doc_gate is not None and not doc_gate(doc):
        return {"json_v2_doc": doc, "declined": True,
                "json_version": str(_ent(j2, "version") or "")}

    # --- light GPKG (multi-stream range download) ---
    size = int(_ent(g2, "size", 0) or 0)
    gpath = work_dir / f"{code}_light_v2.src.gpkg"
    if gpath.exists() and gpath.stat().st_size == size:
        log.info("KG %s: reusing downloaded %s (%.0f MB)", code, gpath.name, size / 1e6)
    else:
        free = shutil.disk_usage("/").free / (1024 ** 3)
        need = 2 * size / (1024 ** 3) + min_free_gb      # src + cadastre-free copy
        if free < need:
            for old in work_dir.glob("*.src.gpkg*"):
                if old.name != gpath.name:
                    old.unlink(missing_ok=True)
            free = shutil.disk_usage("/").free / (1024 ** 3)
            if free < need:
                raise RuntimeError(f"{code}: disk {free:.1f} GB free < {need:.1f} GB for light GPKG x2")
        _step(f"light GPKG {size / 1e6:.0f} MB ({n_streams} streams)")
        entry = {"bucket_url": _ent(g2, "bucket_url"), "filename": _ent(g2, "filename"),
                 "size": size, "checksum": _ent(g2, "checksum")}
        t0 = time.time()
        try:
            rec = _gf.download(entry, gpath, n_streams=n_streams)
        except FileNotFoundError as e:
            raise FileNotFoundError(f"{code}_light_gpkg_v2 gone from Zenodo: {e}") from e
        except Exception as e:  # noqa: BLE001
            raise RuntimeError(f"{code}_light_gpkg_v2 download: {e}") from e
        want = str(_ent(g2, "checksum") or "").replace("md5:", "")
        if want and rec.get("md5") != want and not _gf.md5_acceptable(entry, rec.get("md5"), code):
            gpath.unlink(missing_ok=True)
            raise RuntimeError(f"{code}_light_gpkg_v2 md5 mismatch ({rec.get('md5')} != {want})")
        log.info("KG %s: v2.4 fast inputs ready — %s %.0f MB in %.0fs", code, gpath.name,
                 size / 1e6, time.time() - t0)
    try:
        c = sqlite3.connect(f"file:{gpath}?mode=ro", uri=True)
        qc = c.execute("PRAGMA quick_check").fetchone()[0]
        c.close()
        if qc != "ok":
            gpath.unlink(missing_ok=True)
            raise RuntimeError(f"{code}_light_gpkg_v2: quick_check {qc[:80]}")
    except sqlite3.DatabaseError as e:
        gpath.unlink(missing_ok=True)
        raise RuntimeError(f"{code}_light_gpkg_v2: {e}") from e
    return {"light_src": str(gpath), "json_v2_doc": doc,
            "light_version": _ent(g2, "version"), "json_version": _ent(j2, "version")}


def release_inputs(code: str, work_dir: Path) -> None:
    for f in Path(work_dir).glob(f"{code}_*.src.*"):
        try:
            f.unlink()
        except OSError:
            pass


# === SECTION: cadastre-free light GPKG ===

def make_cadastre_free_gpkg(src: str, dst: str) -> dict:
    """Copy *src* → *dst* and remove every cadastre-shaped table (+ its
    GeoPackage bookkeeping).  Returns {dropped: [...], bytes_before, bytes_after}."""
    src, dst = str(src), str(dst)
    if os.path.exists(dst):
        os.unlink(dst)
    shutil.copyfile(src, dst)
    before = os.path.getsize(dst)
    c = sqlite3.connect(dst)
    dropped = []
    try:
        tables = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        views = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='view'")}
        for t in CADASTRE_TABLES:
            if t not in tables and t not in views:
                continue
            # triggers + rtree virtual tables first (they reference the table)
            for (trg,) in c.execute("SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name=?", (t,)).fetchall():
                c.execute(f'DROP TRIGGER IF EXISTS "{trg}"')
            for (trg,) in c.execute("SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE ?",
                                    (f"rtree_{t}_%",)).fetchall():
                c.execute(f'DROP TRIGGER IF EXISTS "{trg}"')
            for (rt,) in c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE ?",
                                   (f"rtree_{t}_%",)).fetchall():
                c.execute(f'DROP TABLE IF EXISTS "{rt}"')
            if t in views:
                c.execute(f'DROP VIEW IF EXISTS "{t}"')
            else:
                c.execute(f'DROP TABLE IF EXISTS "{t}"')
            for bk, col in (("gpkg_contents", "table_name"), ("gpkg_geometry_columns", "table_name"),
                            ("gpkg_extensions", "table_name"), ("gpkg_tile_matrix", "table_name"),
                            ("gpkg_tile_matrix_set", "table_name"), ("gpkg_metadata_reference", "table_name"),
                            ("gpkg_data_columns", "table_name"), ("layer_styles", "f_table_name"),
                            ("gpkg_ogr_contents", "table_name")):
                if bk in tables:
                    try:
                        c.execute(f'DELETE FROM "{bk}" WHERE "{col}"=?', (t,))
                    except sqlite3.Error:
                        pass
            dropped.append(t)
        c.commit()
        c.execute("VACUUM")
    finally:
        c.close()
    # GPKG metadata: record the 2.4 relabel so a reader can tell
    try:
        c = sqlite3.connect(dst)
        if c.execute("SELECT 1 FROM sqlite_master WHERE name='gpkg_metadata'").fetchone():
            c.execute("INSERT INTO gpkg_metadata(md_scope, md_standard_uri, mime_type, metadata) VALUES (?,?,?,?)",
                      ("dataset", "https://example.invalid/docs/v2.4", "application/json",
                       json.dumps({"product_version": "2.4", "cadastre_free": True, "path": PATH_TAG,
                                   "dropped_tables": dropped,
                                   "relabelled_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())})))
            c.commit()
        c.close()
    except sqlite3.Error as e:
        log.debug("gpkg_metadata note skipped: %s", e)
    return {"dropped": dropped, "bytes_before": before, "bytes_after": os.path.getsize(dst)}


# === SECTION: arrays from the light GPKG ===

def _read_raster(gpkg: str, table: str, paletted: bool = False):
    """Band 1 of a GPKG raster table.  ``paletted=True`` for the uint8
    ``segment_type`` *tiles* table: GDAL stores it as a palette PNG and by
    default expands it to RGBA on read (band 1 = the RED of the type colour,
    NOT the type code); ``BAND_COUNT=1`` returns the palette index = code."""
    import rasterio
    opts = {"BAND_COUNT": 1} if paletted else {}
    with rasterio.open(f"GPKG:{gpkg}:{table}", **opts) as ds:
        if paletted and ds.count != 1:
            raise RuntimeError(f"{table}: expected 1 paletted band, got {ds.count}")
        arr = ds.read(1)
        nod = ds.nodata
        tf = ds.transform
    return arr, nod, tf


def read_arrays(gpkg: str) -> dict:
    """seg_type (uint8), ndsm (float32, NaN nodata), transform, shape."""
    st, _, tf = _read_raster(gpkg, "segment_type", paletted=True)
    if st.ndim != 2:
        st = st[0]
    nd, nod, tf2 = _read_raster(gpkg, "segment_height")
    nd = nd.astype(np.float32, copy=False)
    if nod is not None:
        nd = np.where(nd == nod, np.nan, nd)
    nd = np.where(nd < -1000, np.nan, nd)             # -9999 style nodata
    if nd.shape != st.shape:
        # same grid by construction; crop defensively to the overlap
        h = min(nd.shape[0], st.shape[0]); w = min(nd.shape[1], st.shape[1])
        nd, st = nd[:h, :w], st[:h, :w]
        log.warning("segment_height/segment_type shape mismatch %s vs %s — cropped", nd.shape, st.shape)
    if abs(tf.a - tf2.a) > 1e-6 or abs(tf.c - tf2.c) > 0.5 or abs(tf.f - tf2.f) > 0.5:
        log.warning("segment_type / segment_height transforms differ: %s vs %s", tf, tf2)
    return {"seg_type": st.astype(np.uint8, copy=False), "ndsm": nd, "transform": tf, "shape": st.shape}


def read_objects(gpkg: str) -> list:
    """One ``SimpleNamespace`` per object from ``segment_points`` (EPSG:4326
    centroids → ``centroid_e/n`` in 3035) with the attribute names
    ``ne_h3.build_observed`` reads off ``SegmentedObject``."""
    import fiona
    from pyproj import Transformer
    tx = Transformer.from_crs("EPSG:4326", "EPSG:3035", always_xy=True)
    out = []
    with fiona.open(gpkg, layer="segment_points") as src:
        crs_epsg = None
        try:
            crs_epsg = src.crs.to_epsg()
        except Exception:
            pass
        for f in src:
            p = f["properties"]
            g = f.get("geometry")
            if not g:
                continue
            x, y = g["coordinates"][:2]
            if crs_epsg == 3035:
                e, n = x, y
            else:
                e, n = tx.transform(x, y)
            oid = p.get("id")
            if oid is None:
                continue
            out.append(SimpleNamespace(
                obj_id=int(oid), obj_type=str(p.get("type") or ""),
                centroid_e=float(e), centroid_n=float(n),
                area_sqm=float(p.get("area_sqm") or 0.0),
                height_max=float(p.get("height_max_m") or 0.0),
                height_p90=float(p.get("height_p90_m") or 0.0),
                height_mean=float(p.get("height_mean_m") or 0.0),
                confidence=float(p.get("confidence") or 0.0),
                ndvi_fused=float(p.get("ndvi_fused") or 0.0),
                ndvi_mean=float(p.get("ndvi_mean") or 0.0),
                harm_amplitude=float(p.get("harm_amplitude") or 0.0),
                height_change=float(p.get("height_change_m") or 0.0),
                roughness=float(p.get("roughness") or 0.0),
                dtm_change=float(p.get("dtm_change_m") or 0.0),
                tri_mean=float(p.get("tri_mean") or 0.0),
                tpi_mean=float(p.get("tpi_mean") or 0.0),
                phenology_class=str(p.get("phenology_class") or ""),
                is_manmade=int(p.get("is_manmade") or 0),
            ))
    return out


def rasterize_labels(gpkg: str, shape: tuple, transform) -> np.ndarray:
    """int32 label raster (object id per pixel) from the ``segments`` polygons."""
    import fiona
    from rasterio.features import rasterize

    def _shapes():
        with fiona.open(gpkg, layer="segments") as src:
            for f in src:
                g = f.get("geometry")
                oid = (f["properties"] or {}).get("id")
                if g and oid is not None:
                    yield g, int(oid)
    try:
        return rasterize(_shapes(), out_shape=shape, transform=transform, fill=0,
                         dtype="int32", all_touched=False)
    except ValueError:
        return np.zeros(shape, np.int32)


def read_apices(gpkg: str) -> list:
    import fiona
    rows = []
    try:
        with fiona.open(gpkg, layer="tree_apices") as src:
            for f in src:
                g = f.get("geometry")
                if not g:
                    continue
                r = dict(f["properties"] or {})
                r["e"], r["n"] = g["coordinates"][:2]
                rows.append(r)
    except Exception as e:  # noqa: BLE001
        log.warning("tree_apices unreadable: %s", e)
    return rows


def read_coarse(gpkg: str) -> dict | None:
    out = {}
    for band in ("dtm", "slope", "aspect", "dsm", "ndsm"):
        try:
            a, nod, tf = _read_raster(gpkg, f"terrain_coarse_{band}")
            a = a.astype(np.float32, copy=False)
            if nod is not None:
                a = np.where(a == nod, np.nan, a)
            a = np.where(a < -1000, np.nan, a)
            out[band] = a
            out.setdefault("left", float(tf.c)); out.setdefault("top", float(tf.f))
            out.setdefault("res", float(abs(tf.a)))
            out.setdefault("rows", a.shape[0]); out.setdefault("cols", a.shape[1])
        except Exception:
            if band == "dtm":
                return None
    return out if out.get("dtm") is not None else None


def tile_meta_from_doc(doc: dict, bbox_wgs: tuple, full_tf, shape: tuple) -> list:
    """Pseudo ``tile_seg_results`` for ``ne_h3.build_observed``: one entry
    with the KG-wide Hansen loss-year raster (5 m, from the cached UMD
    tile) and one entry per JSON tile carrying its ALS flight years."""
    import rasterio.transform as rtf
    from pyproj import Transformer
    out = []
    H, W = shape
    res = abs(float(full_tf.a))
    full_bounds = rtf.array_bounds(H, W, full_tf)
    # Hansen at 5 m over the whole KG (30 m source → no information lost)
    try:
        from tile_cache import HansenTileCache
        f = max(1, int(round(5.0 / res)))
        tf5 = rasterio_affine(full_tf.c, full_tf.f, res * f)
        shp5 = ((H + f - 1) // f, (W + f - 1) // f)
        hd = HansenTileCache().get_forest_prior(tuple(bbox_wgs), tf5, shp5)
        ly = (hd or {}).get("loss_year")
        if ly is not None:
            b5 = rtf.array_bounds(shp5[0], shp5[1], tf5)
            out.append({"bounds_3035": tuple(b5), "hansen_loss_year": np.asarray(ly)})
    except Exception as e:  # noqa: BLE001
        log.warning("Hansen loss-year unavailable for NE cells: %s", e)
    # flight years per JSON tile (acquisition.tiles[i] ↔ data_quality.tiles[i])
    try:
        tx = Transformer.from_crs("EPSG:4326", "EPSG:3035", always_xy=True)
        dq_tiles = ((doc.get("data_quality") or {}).get("tiles") or [])
        by_idx = {int(t.get("tile_index", i + 1)) - 1: t for i, t in enumerate(dq_tiles)}
        acq_tiles = ((doc.get("acquisition") or {}).get("tiles") or [])
        n_fy = 0
        for a in acq_tiles:
            years = {int(a[k]) for k in ("dtm_year", "dsm_year") if a.get(k)}
            if not years:
                continue
            t = by_idx.get(int(a.get("tile", -1)))
            bb = (t or {}).get("bbox_wgs")
            if bb and len(bb) == 4:
                x0, y0 = tx.transform(bb[0], bb[1]); x1, y1 = tx.transform(bb[2], bb[3])
                bounds = (min(x0, x1), min(y0, y1), max(x0, x1), max(y0, y1))
            else:
                bounds = tuple(full_bounds)
            out.append({"bounds_3035": bounds, "flight_years": sorted(years)})
            n_fy += 1
        if acq_tiles and not n_fy:
            log.info("acquisition block carries no ALS years — flight_years stay 0")
    except Exception as e:  # noqa: BLE001
        log.warning("flight years from JSON failed: %s", e)
    return out


def rasterio_affine(left: float, top: float, res: float):
    from rasterio.transform import from_origin
    return from_origin(left, top, res, res)


# === SECTION: JSON relabel ===

def relabel_doc(doc: dict, ne_out: dict, *, product_version: str, strip_fn, path_note: str) -> dict:
    """v2.3 JSON → v2.4 JSON: cadastre-free + ``ne`` + version stamps.
    *ne_out* = ``{"summary": …}`` on success or ``{"error": …}``."""
    d = strip_fn(doc)
    prev = d.get("product_version")
    d["product_version"] = product_version
    d["version"] = d.get("version") or "v2"
    d["cadastre_free"] = True
    d.setdefault("coverage", {})
    if isinstance(d["coverage"], dict):
        d["coverage"]["note"] = ("Full KG tiled segmentation. Since 2.4 no parcel/footprint data is in this "
                                 "product; sub-KG detail lives in the NE cells layer (section 'ne', product <code>_ne.h3b).")
    if ne_out.get("summary"):
        d["ne"] = dict(ne_out["summary"], status="ok")
    else:
        d["ne"] = {"status": "missing", "error": ne_out.get("error") or "declared layer not available"}
    m = d.setdefault("methods", {})
    if isinstance(m, dict):
        m["ne_cells"] = (
            "H3 res-12 cells: declared land-use mix (NEC1, umfeld-at ne-cells-2, frozen epoch) vs our "
            "observed layer (NEO1: segment_type mix, nDSM heights, tree count/height/crown, NDVI, dh, "
            "verdict declared-vs-observed); NET1 = every tree apex >= 3 m, NEB1 = every man-made object "
            "with observed height; res-10 chunked .h3b product (manifest key _ne_h3b)")
        m["calibration"] = "Cadastre footprints as run-time model context only (not stored)"
        m["cadastre_source"] = ("run-time model context only (umfeld-at /k, cached <= 24 h); products carry "
                                "cadastre solely as NE cells (ne_h3, algo ne-cells-2) — no parcel / footprint / id")
        ds = m.get("data_sources")
        if isinstance(ds, list) and not any("NE cells" in str(x) for x in ds):
            ds.append("BEV Kataster as NE cells (umfeld-at ne-cells-2, H3 res 12)")
    up = d.get("upgraded_from") if isinstance(d.get("upgraded_from"), dict) else {}
    up = dict(up or {})
    up["relabel"] = {"path": PATH_TAG, "from_product_version": prev, "note": path_note,
                     "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    d["upgraded_from"] = up
    d.pop("cadastre_stripped_at", None)      # that stamp is for the stripped v1 JSON
    return d


# === SECTION: driver ===

def build(kg: dict, inputs: dict, ne_declared, *, out_gpkg: str, out_h3b: str, product_version: str,
          strip_fn, type_names: dict, step=None) -> dict:
    """Everything up to (not including) verify/upload.  Returns
    ``{light_gpkg, json_doc, ne, n_objects, n_apices, seconds, dropped}``;
    ``ne`` mirrors ``v21_out["ne"]`` of the fresh path (``h3b_path`` /
    ``summary`` / ``neo1_section`` / ``declared_digest`` or ``error``)."""
    import ne_h3
    code = kg["kg_code"]
    t0 = time.time()

    def _step(d):
        if step:
            step(d)

    _step("cadastre-free light GPKG (copy + drop tables + vacuum)")
    cf = make_cadastre_free_gpkg(inputs["light_src"], out_gpkg)
    log.info("KG %s: v2.4 fast — dropped %s, %.0f → %.0f MB", code, cf["dropped"],
             cf["bytes_before"] / 1e6, cf["bytes_after"] / 1e6)

    ne_out: dict = {}
    n_obj = n_ap = 0
    if ne_declared is None:
        ne_out = {"error": "declared layer not available"}
    else:
        try:
            _step("NE cells: reading segment_type / nDSM")
            arr = read_arrays(out_gpkg)
            H, W = arr["shape"]
            _step(f"NE cells: objects + label raster ({H * W / 1e6:.0f} Mpx)")
            objs = read_objects(out_gpkg)
            n_obj = len(objs)
            labels = rasterize_labels(out_gpkg, arr["shape"], arr["transform"])
            apex = read_apices(out_gpkg)
            n_ap = len(apex)
            coarse = read_coarse(out_gpkg)
            bb = kg.get("bbox") or (inputs["json_v2_doc"].get("bbox") or {})
            bbox_wgs = (bb["min_lon"], bb["min_lat"], bb["max_lon"], bb["max_lat"])
            meta = tile_meta_from_doc(inputs["json_v2_doc"], bbox_wgs, arr["transform"], arr["shape"])
            _step(f"NE cells: aggregating {ne_declared.header.get('cells_n')} res-12 cells")
            tne = time.time()
            obs = ne_h3.build_observed(ne_declared, arr["seg_type"], arr["ndsm"], labels, objs,
                                       arr["transform"], type_names, apex_rows=apex,
                                       tile_seg_results=meta, kg=ne_declared.header.get("kg", code),
                                       coarse=coarse)
            del labels, arr
            is_block = "-" in str(code)
            hdr = ne_h3.write_h3b(out_h3b, ne_declared, obs, code, product_version,
                                  extra_header={"block": code if is_block else None,
                                                "parent_kg": ne_declared.header.get("kg"),
                                                "partial_cells": is_block, "path": PATH_TAG})
            chk = ne_h3.verify_h3b(out_h3b, expected_declared_digest=ne_declared.digest)
            if not chk["ok"]:
                raise RuntimeError("h3b self-check failed: " + "; ".join(chk["errors"]))
            ne_out = {"h3b_path": out_h3b, "summary": ne_h3.json_section(obs, hdr),
                      "neo1_section": obs["neo1"] if not is_block else None,
                      "nec1_section": (ne_h3.nec1_bytes(ne_declared)
                                       if not is_block and ne_h3.is_bev_declared(ne_declared) else None),
                      "declared_digest": ne_declared.digest, "declared_source": ne_declared.header.get("source"),
                      "seconds": round(time.time() - tne, 1)}
            so = obs["summary"]["observed"]
            log.info("KG %s: v2.4 NE cells (fast) %d observed / %d, %d trees, %d man-made, %d chunks, %.0f KB, %.0fs",
                     code, so["cells_observed"], len(obs["cell_ids"]), so["trees_n"], so["manmade_n"],
                     len(hdr["chunks"]), os.path.getsize(out_h3b) / 1e3, time.time() - tne)
        except Exception as e:  # noqa: BLE001
            log.warning("KG %s: v2.4 NE cells build failed (fast path): %s", code, e, exc_info=True)
            ne_out = {"error": str(e)[:200]}
            try:
                os.unlink(out_h3b)
            except OSError:
                pass

    doc = relabel_doc(inputs["json_v2_doc"], ne_out, product_version=product_version, strip_fn=strip_fn,
                      path_note=f"relabelled from light {inputs.get('light_version')} + json {inputs.get('json_version')}")
    return {"light_gpkg": out_gpkg, "json_doc": doc, "ne": ne_out, "n_objects": n_obj, "n_apices": n_ap,
            "dropped": cf["dropped"], "seconds": round(time.time() - t0, 1)}
