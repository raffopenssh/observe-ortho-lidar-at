"""Cadastre building footprint fetcher and ground-truth evaluator.

Building polygons come from the **local bevdirect-serve** (vtcseamless, BEV
Katastralmappe vector tiles) via :mod:`live_cadastre` — the same source the
bev-mode KG pipeline uses (``austria_processor.fetch_cadastre_data_bevdirect``).
They are transformed to EPSG:3035, rasterised onto our 1 m LIDAR grid and
used as run-time model context / ground truth for the classifier.

History: until 2026-10 this module fetched from the cadastre API
(``example.invalid/k/api/v1/spatial/points|footprints`` and
``/export/geojson``).  Those parcel-keyed routes are gone (plain 404 since the
umfeld-at publication, 410 before) — **no HTTP call to umfeld is made here
any more**.  Run-time context only: nothing from this module is written to a
product (``austria_processor.CADASTRE_PRODUCTS = False``).
"""
from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Optional

import numpy as np
from pyproj import Transformer
from shapely.geometry import shape as shapely_shape, mapping
from shapely.ops import transform as shapely_transform

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CACHE_DIR = Path("/tmp/cadastre_cache")  # legacy on-disk GeoJSON cache (unused; live_cadastre keeps a RAM LRU)
MAX_PER_REQUEST = 100_000

# Reusable CRS transformer (thread-safe after creation)
_TRANSFORMER_4326_TO_3035 = Transformer.from_crs(
    "EPSG:4326", "EPSG:3035", always_xy=True
)


class CadastreRetired(RuntimeError):
    """The umfeld ``/k`` cadastre routes (``spatial/*``, ``export/geojson``)
    are retired (404).  Kept for callers that still ``except CadastreRetired``;
    nothing raises it on a network path any more."""


def _transform_geom_to_3035(geom):
    """Transform a shapely geometry from EPSG:4326 → EPSG:3035."""
    return shapely_transform(_TRANSFORMER_4326_TO_3035.transform, geom)


# ---------------------------------------------------------------------------
# 1. Fetch building footprints (local bevdirect via live_cadastre)
# ---------------------------------------------------------------------------

def fetch_building_footprints(
    bbox_wgs84: tuple[float, float, float, float],
    *,
    use_cache: bool = True,
    limit: int = MAX_PER_REQUEST,
    fast_path: bool = True,
) -> Optional[list]:
    """Building footprint POLYGONS (EPSG:3035, clipped to *bbox_wgs84*) from
    the local bevdirect-serve.

    ``use_cache`` / ``limit`` / ``fast_path`` are accepted for call-site
    compatibility (the RAM cell cache in :mod:`live_cadastre` is always on).

    Returns ``[]`` for a rural AOI without buildings, ``None`` when bevdirect
    is unavailable on this host (tooling missing, server down, AOI too big).
    """
    west, south, east, north = bbox_wgs84
    try:
        import live_cadastre as _lc
    except ImportError:
        return None
    t0 = time.time()
    try:
        ctx = _lc.fetch((west, south, east, north))
    except _lc.Unavailable as e:
        log.warning("cadastre footprints unavailable (bevdirect): %s", e)
        return None
    except Exception as e:  # noqa: BLE001
        log.warning("cadastre footprints failed (bevdirect): %s", e)
        return None
    from shapely.geometry import box as shapely_box
    clip_box = _transform_geom_to_3035(shapely_box(west, south, east, north))
    polygons_3035 = []
    n_skipped = 0
    for g in ctx.get("footprints") or []:
        try:
            if g.is_empty or g.geom_type == "Point" or not g.intersects(clip_box):
                continue
            gc = g.intersection(clip_box)
            if not gc.is_empty:
                polygons_3035.append(gc)
        except Exception as exc:  # noqa: BLE001
            n_skipped += 1
            log.debug("Skipping invalid footprint geometry: %s", exc)
        if len(polygons_3035) >= limit:
            break
    log.info("cadastre: %d building footprints (%d skipped, %s) for bbox "
             "[%.4f, %.4f, %.4f, %.4f] in %.1fs", len(polygons_3035), n_skipped,
             ctx.get("source"), west, south, east, north, time.time() - t0)
    return polygons_3035


def _extract_features(geojson: dict) -> list[dict]:
    """Robustly extract feature list from various GeoJSON shapes."""
    if not isinstance(geojson, dict):
        return []
    gtype = geojson.get("type", "")
    if gtype == "FeatureCollection":
        return geojson.get("features", [])
    if gtype == "Feature":
        return [geojson]
    # Some APIs return a bare geometry or a dict of layers
    if "features" in geojson:
        val = geojson["features"]
        if isinstance(val, list):
            return val
    # Nested layer dict? e.g. {"buildings": {"type": "FeatureCollection", ...}}
    for key in ("buildings", "building_footprints"):
        sub = geojson.get(key)
        if isinstance(sub, dict):
            return _extract_features(sub)
        if isinstance(sub, list):
            return sub
    return []


# ---------------------------------------------------------------------------
# 2. Rasterize footprints onto a grid
# ---------------------------------------------------------------------------

def rasterize_buildings(
    footprints_3035: list,
    transform,
    shape: tuple[int, int],
) -> np.ndarray:
    """Create a boolean raster where True = building pixel.

    Parameters
    ----------
    footprints_3035 : list of shapely Polygon/MultiPolygon in EPSG:3035
    transform : rasterio Affine transform for the output grid
    shape : (rows, cols) of the output grid

    Returns
    -------
    np.ndarray of bool, shape = *shape*
    """
    from rasterio.features import rasterize

    if not footprints_3035:
        log.info("No footprints to rasterize – returning empty mask")
        return np.zeros(shape, dtype=bool)

    # rasterio.features.rasterize expects (geometry, value) pairs
    geom_value_pairs = [(geom, 1) for geom in footprints_3035 if not geom.is_empty]

    if not geom_value_pairs:
        return np.zeros(shape, dtype=bool)

    log.info("Rasterizing %d building footprints onto %s grid", len(geom_value_pairs), shape)
    raster = rasterize(
        geom_value_pairs,
        out_shape=shape,
        transform=transform,
        fill=0,
        default_value=1,
        dtype=np.uint8,
        all_touched=True,
    )
    mask = raster.astype(bool)
    n_px = int(mask.sum())
    log.info(
        "Building mask: %d pixels (%.2f%% of grid)",
        n_px, 100 * n_px / (shape[0] * shape[1]) if shape[0] * shape[1] > 0 else 0,
    )
    return mask


# ---------------------------------------------------------------------------
# 3. Convenience: fetch + rasterize
# ---------------------------------------------------------------------------

def get_building_mask(
    bbox_wgs84: tuple[float, float, float, float],
    transform,
    shape: tuple[int, int],
    *,
    use_cache: bool = True,
) -> Optional[np.ndarray]:
    """Fetch building footprints and rasterize them in one call.

    Parameters
    ----------
    bbox_wgs84 : (west, south, east, north)
    transform : rasterio Affine (EPSG:3035)
    shape : (rows, cols)
    use_cache : bool

    Returns
    -------
    np.ndarray of bool, or None if the fetch failed.
    """
    footprints = fetch_building_footprints(bbox_wgs84, use_cache=use_cache)
    if footprints is None:
        return None
    return rasterize_buildings(footprints, transform, shape)


# ---------------------------------------------------------------------------
# 4. Evaluate classifier against cadastre ground truth
# ---------------------------------------------------------------------------

# Building-related type codes from object_classifier
_BUILDING_CODES = {9, 10, 17, 18}  # building, structure, solar_panel, greenhouse


def evaluate_classification(
    predicted_types: np.ndarray,
    building_mask: np.ndarray,
    *,
    building_codes: set[int] | None = None,
) -> dict:
    """Compare predicted object-type raster against cadastre building mask.

    Parameters
    ----------
    predicted_types : np.ndarray uint8  (type_code per pixel)
        From ``object_classifier.make_type_raster()``.
    building_mask : np.ndarray bool
        Ground truth from ``get_building_mask()`` or ``rasterize_buildings()``.
    building_codes : set[int], optional
        Type codes that count as "building" in the prediction.
        Default: {9 (building), 10 (structure), 17 (solar_panel), 18 (greenhouse)}.

    Returns
    -------
    dict with keys:
        tp, fp, fn, tn   – confusion matrix counts
        precision         – TP / (TP + FP)  (what fraction of predicted buildings
                            are actually buildings)
        recall            – TP / (TP + FN)  (what fraction of real buildings
                            we detected)
        f1                – harmonic mean of precision & recall
        iou               – intersection over union (Jaccard index)
        accuracy          – (TP + TN) / total
        building_pixels_truth  – total building pixels in ground truth
        building_pixels_pred   – total building pixels in prediction
    """
    if building_codes is None:
        building_codes = _BUILDING_CODES

    if predicted_types.shape != building_mask.shape:
        raise ValueError(
            f"Shape mismatch: predicted {predicted_types.shape} "
            f"vs building_mask {building_mask.shape}"
        )

    pred_bld = np.isin(predicted_types, list(building_codes))
    truth = building_mask.astype(bool)

    tp = int(np.sum(pred_bld & truth))
    fp = int(np.sum(pred_bld & ~truth))
    fn = int(np.sum(~pred_bld & truth))
    tn = int(np.sum(~pred_bld & ~truth))

    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) > 0 else 0.0
    iou = tp / (tp + fp + fn) if (tp + fp + fn) > 0 else 0.0
    total = tp + fp + fn + tn
    accuracy = (tp + tn) / total if total > 0 else 0.0

    result = {
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "tn": tn,
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1": round(f1, 4),
        "iou": round(iou, 4),
        "accuracy": round(accuracy, 4),
        "building_pixels_truth": int(truth.sum()),
        "building_pixels_pred": int(pred_bld.sum()),
    }

    log.info(
        "Evaluation: P=%.3f R=%.3f F1=%.3f IoU=%.3f  "
        "(TP=%d FP=%d FN=%d TN=%d)",
        precision, recall, f1, iou, tp, fp, fn, tn,
    )
    return result


# ---------------------------------------------------------------------------
# 5. Bonus: per-class breakdown
# ---------------------------------------------------------------------------

def classification_confusion_by_type(
    predicted_types: np.ndarray,
    building_mask: np.ndarray,
) -> dict[str, dict]:
    """For each predicted type code, count pixels inside/outside buildings.

    Returns dict mapping type_name → {"inside_building": int, "outside_building": int}.
    Useful for seeing which types are confused with buildings.
    """
    # Import locally to avoid circular dependency
    try:
        from object_classifier import OBJECT_TYPE_NAMES
    except ImportError:
        OBJECT_TYPE_NAMES = {}

    truth = building_mask.astype(bool)
    codes = np.unique(predicted_types)
    result = {}
    for code in codes:
        code_mask = predicted_types == code
        name = OBJECT_TYPE_NAMES.get(int(code), f"type_{code}")
        result[name] = {
            "inside_building": int(np.sum(code_mask & truth)),
            "outside_building": int(np.sum(code_mask & ~truth)),
        }
    return result
