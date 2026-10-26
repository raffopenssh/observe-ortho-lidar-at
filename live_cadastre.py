"""Run-time cadastre context for the *live* ``/api/v1/segment`` path from the
local ``bevdirect-serve`` (vtcseamless) — the same public BEV Katastralmappe
vector tiles the bev-mode processor uses (``austria_processor.
fetch_cadastre_data_bevdirect``), minus everything the KG pipeline adds on
top: **no NE declared layer, no ``/contrib`` report, no umfeld call**.

Why: umfeld's ``/k/api/v1/spatial/parcels|footprints`` fast paths answer
404 since the Kataster purge, so segv2's ``Context`` (dist_building /
dist_parcel_edge features, footprint-aware rules) and the v1
``building_footprints`` mask had silently degraded to empty on every live
run.  The processor already holds parcels + footprints per KG and hands
them to ``Context(parcels=…, footprints=…)``; this module gives the live
path the equivalent for an arbitrary AOI.

Contract
--------
``fetch(bbox_wgs) -> {'parcels': [geom3035], 'footprints': [geom3035],
'source': 'bevdirect@<tag>', 'cells': n, 'dt': s}`` or raises
:class:`Unavailable`.  Run-time model context only — nothing here is ever
written to a product or a response body (``CADASTRE_PRODUCTS=False`` rule).
Documents are parsed in RAM and dropped; a small per-cell LRU (geometry
lists, ≤ ``CELL_CACHE_MAX`` aligned 0.02° cells, ``CELL_CACHE_TTL_S``)
makes overlay / re-run requests for the same AOI free.
"""
from __future__ import annotations

import logging
import threading
import time
from collections import OrderedDict

from pyproj import Transformer
from shapely.geometry import shape
from shapely.ops import transform as shp_transform

log = logging.getLogger(__name__)

_T_4326_3035 = Transformer.from_crs("EPSG:4326", "EPSG:3035", always_xy=True)

#: upper bound on AOI size we serve from the viewport grid (cells of 0.02°
#: ≈ 1.5 × 2.2 km; the UI caps at 5 / 25 km²)
MAX_CELLS = 24
CELL_CACHE_MAX = 64
CELL_CACHE_TTL_S = 6 * 3600
DEADLINE_S = 240.0

_cache: "OrderedDict[tuple, tuple[float, dict]]" = OrderedDict()
_lock = threading.Lock()


class Unavailable(RuntimeError):
    """bevdirect-serve not installed / not running / upstream down / AOI too big."""


def available(probe_server: bool = False) -> bool:
    try:
        import ne_h3
        st = ne_h3.bevdirect_ready(probe_server=probe_server)
        return bool(st.get("binary")) and (not probe_server or bool(st.get("server")))
    except Exception:  # noqa: BLE001
        return False


def _to_3035(g):
    return shp_transform(_T_4326_3035.transform, g)


def _cell_geoms(doc: dict) -> dict:
    """One viewport document → EPSG:3035 geometry lists (RAM only)."""
    parcels, fps = {}, {}
    for p in doc.get("parcels") or []:
        g = p.get("geometry")
        if not g:
            continue
        pid = str(p.get("parcel_id") or f"{p.get('kg_code', '')}-{p.get('gnr', '')}")
        comp = bool(p.get("complete", True))
        cur = parcels.get(pid)
        if cur is None or (comp and not cur[1]) or \
                (comp == cur[1] and float(p.get("area_sqm") or 0) > cur[2]):
            parcels[pid] = (g, comp, float(p.get("area_sqm") or 0))
    for f in doc.get("footprints") or []:
        g = f.get("geometry")
        if not g:
            continue
        fid = str(f.get("id") or "")
        cur = fps.get(fid)
        if cur is None or float(f.get("area_sqm") or 0) > cur[1]:
            fps[fid] = (g, float(f.get("area_sqm") or 0))

    def _geoms(items):
        out = []
        for g in items:
            try:
                s = shape(g)
                if s.is_empty:
                    continue
                if not s.is_valid:
                    s = s.buffer(0)
                out.append(_to_3035(s))
            except Exception:  # noqa: BLE001
                continue
        return out

    return {"parcels": _geoms(g for g, _, _ in parcels.values()),
            "footprints": _geoms(g for g, _ in fps.values()),
            "source": str(doc.get("source") or "bevdirect")}


def fetch(bbox_wgs, *, deadline_s: float = DEADLINE_S) -> dict:
    """Parcels + footprints (EPSG:3035) for a WGS84 ``(W, S, E, N)`` bbox."""
    import vct_pilot
    W, S, E, N = (float(v) for v in bbox_wgs)
    cells = vct_pilot._cells(W, S, E, N)
    if len(cells) > MAX_CELLS:
        raise Unavailable(f"AOI spans {len(cells)} bevdirect cells > {MAX_CELLS}")
    if not available(probe_server=False):
        raise Unavailable("bevdirect-serve tooling not installed on this host")
    try:
        import ne_h3
        ne_h3._ensure_bevdirect_server()
    except Exception as e:  # noqa: BLE001
        raise Unavailable(f"bevdirect-serve not running: {e}") from e
    t0 = time.time()
    deadline = t0 + deadline_s
    parcels, fps, src = [], [], "bevdirect"
    now = time.time()
    for ix, iy in cells:
        key = (ix, iy)
        with _lock:
            hit = _cache.get(key)
            if hit and now - hit[0] < CELL_CACHE_TTL_S:
                _cache.move_to_end(key)
                cg = hit[1]
            else:
                cg = None
        if cg is None:
            try:
                doc = vct_pilot._viewport(vct_pilot._cell_bbox(ix, iy), deadline)
            except vct_pilot.UpstreamDownError as e:
                raise Unavailable(str(e)) from e
            except Exception as e:  # noqa: BLE001
                raise Unavailable(f"viewport {ix},{iy}: {str(e)[:160]}") from e
            cg = _cell_geoms(doc)
            del doc
            with _lock:
                _cache[key] = (time.time(), cg)
                while len(_cache) > CELL_CACHE_MAX:
                    _cache.popitem(last=False)
        parcels.extend(cg["parcels"])
        fps.extend(cg["footprints"])
        src = cg["source"]
    # a parcel / footprint straddling two cells arrives twice — the cell
    # documents are independent, so dedupe on WKB here
    def _dedupe(gs):
        seen, out = set(), []
        for g in gs:
            k = g.wkb
            if k in seen:
                continue
            seen.add(k)
            out.append(g)
        return out
    parcels, fps = _dedupe(parcels), _dedupe(fps)
    dt = time.time() - t0
    log.info("live_cadastre: %d cells → %d parcels, %d footprints (%s) in %.1fs",
             len(cells), len(parcels), len(fps), src, dt)
    return {"parcels": parcels, "footprints": fps, "source": src, "cells": len(cells), "dt": round(dt, 1)}


def building_mask(ctx: dict, transform, shape_hw):
    """bool raster (True = footprint) on the AOI grid — the v1 ``building_footprints`` input."""
    import cadastre
    return cadastre.rasterize_buildings(ctx["footprints"], transform, shape_hw)
