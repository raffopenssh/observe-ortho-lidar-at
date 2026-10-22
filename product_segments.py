"""product_segments — serve ``/api/v1/segment`` from the frozen v2.x products.

The fleet has already segmented every finished KG; its ``<code>_light_v2.gpkg``
carries the ``segments`` polygon layer (EPSG:3035, 55 properties — the same
feature vector ``object_segmentation.SegmentedObject`` holds).  Instead of
re-running Felzenszwalb on live BEV rasters for an AOI that is already
covered, :func:`build` reconstructs what ``_segment_core`` needs from those
products:

* AOI → product codes via ``trees_v3.resolve_coverage`` (same oracle the v3
  tree service uses: parent R-tree → ``kg_v2_store.codes_for_parent`` →
  readable ``_light_gpkg_v2`` manifest entry → grid25 footprint).
* light GPKGs through ``search_index.GpkgCache`` (1 GB LRU, Zenodo download
  with ``zenodo_fetch`` progress — the async segment worker binds it to the
  task so the dashboard shows ``k/N files, MB, ETA``).
* ``segments`` rows inside the AOI bbox → ``SegmentedObject`` list + a 1 m
  label raster (``rasterio.features.rasterize``), mask, transform and an
  nDSM proxy (per-object ``height_mean``) so the existing overlay / export /
  share machinery works unchanged.

Mode ``auto`` (the public default) only answers when ≥ ``MIN_PRODUCT_FRAC``
of the AOI is product-covered — otherwise the caller falls back to the live
("bevdirect") pipeline.  Mode ``product`` answers whatever is covered and
raises when nothing is.  Nothing cadastre-derived exists in the 2.4 light
product (``v2_verify.FORBIDDEN_LIGHT_LAYERS``), so the result is clean by
construction; ``_PROP_MAP`` is an explicit allow-list anyway.
"""
from __future__ import annotations

import logging
import math
import time

import numpy as np

log = logging.getLogger(__name__)

#: ``auto`` mode needs every intersecting parent KG to have a readable
#: product (``live_parents`` empty) *and* at least this share of the AOI inside
#: a product footprint (grid25 cells are unclassified over no-data / water, so
#: the footprint fraction is rarely exactly 1).
MIN_PRODUCT_FRAC = 0.5
SEGMENTS_LAYER = 'segments'
#: hard cap on polygons read per request (25 km² of dense forest ≈ 250k)
MAX_OBJECTS = 40_000       # memory guard: ~1 GB RSS at 48k objects / 12 km²

# GPKG column -> SegmentedObject attribute (allow-list; everything else is dropped)
_PROP_MAP = {
    'type': 'obj_type', 'type_code': 'type_code', 'group_type': 'group_type',
    'area_sqm': 'area_sqm', 'perimeter_m': 'perimeter_m', 'compactness': 'compactness',
    'elongation': 'elongation', 'solidity': 'solidity', 'extent': 'extent',
    'height_max_m': 'height_max', 'height_mean_m': 'height_mean', 'height_p90_m': 'height_p90',
    'height_std_m': 'height_std', 'slope_mean_deg': 'slope_mean', 'aspect_mean_deg': 'aspect_mean',
    'aspect_dominant': 'aspect_dominant', 'roughness': 'roughness',
    'dsm_edge_strength': 'dsm_edge_strength', 'elevation_mean_m': 'elevation_mean',
    'elevation_min_m': 'elevation_min', 'elevation_max_m': 'elevation_max',
    'tri_mean': 'tri_mean', 'tpi_mean': 'tpi_mean', 'curvature_mean': 'curvature_mean',
    'terrain_class': 'terrain_class', 'ndvi_mean': 'ndvi_mean', 'ndvi_std': 'ndvi_std',
    'ndvi_fused': 'ndvi_fused', 'brightness_mean': 'brightness_mean', 'nir_mean': 'nir_mean',
    'height_change_m': 'height_change', 'dtm_change_m': 'dtm_change',
    'temporal_stability': 'temporal_stability', 'volume_change_m3': 'volume_change_m3',
    'volume_change_abs_m3': 'volume_change_abs_m3', 'dtm_change_max_m': 'dtm_change_max',
    'glcm_entropy': 'glcm_entropy', 'glcm_homogeneity': 'glcm_homogeneity',
    'texture_complexity': 'texture_complexity', 'sar_vv': 'sar_vv', 'sar_vh': 'sar_vh',
    'harm_amplitude': 'harm_amplitude', 'harm_phase': 'harm_phase',
    'phenology_class': 'phenology_class', 'confidence': 'confidence', 'is_manmade': 'is_manmade',
    'classifier': 'classifier_source', 'rf_model': 'rf_model_hash', 'rf_type': 'rf_type',
    'rf_confidence': 'rf_confidence',
}
_FLOAT_ATTRS = {
    'area_sqm', 'perimeter_m', 'compactness', 'elongation', 'solidity', 'extent', 'height_max',
    'height_mean', 'height_p90', 'height_std', 'slope_mean', 'aspect_mean', 'roughness',
    'dsm_edge_strength', 'elevation_mean', 'elevation_min', 'elevation_max', 'tri_mean',
    'tpi_mean', 'curvature_mean', 'ndvi_mean', 'ndvi_std', 'ndvi_fused', 'brightness_mean',
    'nir_mean', 'height_change', 'dtm_change', 'temporal_stability', 'volume_change_m3',
    'volume_change_abs_m3', 'dtm_change_max', 'glcm_entropy', 'glcm_homogeneity',
    'texture_complexity', 'sar_vv', 'sar_vh', 'harm_amplitude', 'harm_phase', 'confidence',
    'rf_confidence',
}


class NoProductCoverage(ValueError):
    pass


def _grid(geom_3035):
    """Integer-metre raster grid (BEV pixel edges are on integer metres)."""
    from rasterio.transform import Affine
    minx, miny, maxx, maxy = geom_3035.bounds
    x0, y1 = math.floor(minx), math.ceil(maxy)
    w, h = max(1, math.ceil(maxx) - x0), max(1, y1 - math.floor(miny))
    return Affine(1.0, 0.0, float(x0), 0.0, -1.0, float(y1)), (h, w)


def _row_to_object(obj_id: int, props: dict, geom) -> 'SegmentedObject':
    from object_segmentation import SegmentedObject
    kw = {}
    for col, attr in _PROP_MAP.items():
        v = props.get(col)
        if v is None:
            continue
        if attr in _FLOAT_ATTRS:
            try:
                v = float(v)
            except (TypeError, ValueError):
                continue
            if not math.isfinite(v):
                continue
        elif attr == 'is_manmade':
            v = bool(v)
        elif attr == 'type_code':
            try:
                v = int(v)
            except (TypeError, ValueError):
                v = 0
        else:
            v = str(v)
        kw[attr] = v
    c = geom.representative_point() if geom.geom_type != 'Point' else geom
    b = geom.bounds
    kw.setdefault('obj_type', 'unknown')
    kw.setdefault('type_code', 0)
    kw.setdefault('area_sqm', float(geom.area))
    kw.setdefault('perimeter_m', float(geom.length))
    kw.setdefault('compactness', 0.0)
    kw.setdefault('elongation', 1.0)
    kw.setdefault('height_max', 0.0)
    kw.setdefault('height_mean', 0.0)
    kw.setdefault('height_p90', 0.0)
    kw.setdefault('height_std', 0.0)
    kw.setdefault('slope_mean', 0.0)
    kw.setdefault('classifier_source', 'product')
    return SegmentedObject(obj_id=obj_id, centroid_e=float(c.x), centroid_n=float(c.y),
                           bbox=(float(b[0]), float(b[1]), float(b[2]), float(b[3])), **kw)


def build(geom_3035, mode: str = 'auto', progress=None, *, min_frac: float = MIN_PRODUCT_FRAC,
          max_objects: int = MAX_OBJECTS) -> dict | None:
    """Reconstruct a segmentation of *geom_3035* from the v2.x products.

    Returns ``None`` when *mode* is ``auto`` and the products cover less than
    *min_frac* of the AOI (caller runs the live pipeline), raises
    :class:`NoProductCoverage` when *mode* is ``product`` and nothing is
    covered.  Otherwise ``{objects, labels, mask, transform, shape, ndsm,
    stats, codes, version, product_frac, obs_year, warnings}``.
    """
    import trees_v3
    from rasterio.features import rasterize
    from shapely.geometry import shape as shp_shape
    from shapely.prepared import prep
    import fiona

    t0 = time.time()
    _p = progress or (lambda *_a: None)
    _p('Looking up products', 'coverage oracle')
    cov = trees_v3.resolve_coverage(geom_3035)
    frac = cov['product_frac']
    products = cov['product']
    uncovered = not products or bool(cov['live_parents']) or frac < min_frac
    if uncovered:
        if mode == 'product' and not products:
            raise NoProductCoverage('no finished v2.x product covers this area yet — '
                                    'use source=bev (live) or wait for the fleet')
        log.info('product_segments: %.0f%% covered (%d products, live parents %s) → %s', frac * 100,
                 len(products), cov['live_parents'], 'live fallback' if mode == 'auto' else 'partial')
        if mode == 'auto':
            return None

    failed = trees_v3.prefetch_gpkgs(products, progress=lambda d, st: _p('Fetching products', d))
    transform, shape = _grid(geom_3035)
    h, w = shape
    labels = np.zeros(shape, dtype=np.int32)
    pg = prep(geom_3035)
    bounds = geom_3035.bounds
    objects: list = []
    warnings: list[str] = []
    codes, versions, years = [], set(), []
    next_id = 1
    for prod in sorted(products, key=lambda p: p['code']):
        code, entry = prod['code'], prod['entry']
        if code in failed:
            warnings.append(f'{code}: product download failed ({failed[code]})')
            continue
        _p('Reading product', f'{code} segments')
        path = trees_v3.gpkg_path(code, entry)
        if not path:
            warnings.append(f'{code}: light GPKG unavailable')
            continue
        shapes = []
        new_objs = []
        row_years = {}
        n_read = 0
        try:
            with fiona.open(path, layer=SEGMENTS_LAYER) as src:
                for f in src.filter(bbox=bounds):
                    g = f.get('geometry')
                    if not g:
                        continue
                    sg = shp_shape(g)
                    if sg.is_empty or not pg.intersects(sg):
                        continue
                    props = dict(f.get('properties') or {})
                    new_objs.append(_row_to_object(next_id, props, sg))
                    shapes.append((sg, next_id))
                    if props.get('obs_year'):
                        row_years[next_id] = int(props['obs_year'])
                    next_id += 1
                    n_read += 1
                    if len(objects) + len(new_objs) > 2 * max_objects:
                        raise ValueError(f'more than {max_objects} product objects in the area — '
                                         'choose a smaller region')
        except ValueError:
            raise
        except Exception as e:  # noqa: BLE001
            log.warning('product_segments: %s read failed: %s', code, e)
            warnings.append(f'{code}: read failed ({e})')
            continue
        if shapes:
            # later products never overwrite pixels an earlier one already claimed
            tmp = rasterize(shapes, out_shape=shape, transform=transform, fill=0,
                            dtype=np.int32, all_touched=False)
            free = labels == 0
            labels[free] = tmp[free]
            del tmp, free, shapes
            # keep only objects that actually own a pixel (neighbouring products spill
            # over KG borders, so a large share of each read is overlap) — this is
            # what bounds memory, the raw read count is transient
            owned = np.bincount(labels.ravel(), minlength=next_id) > 0
            kept = [o for o in new_objs if owned[o.obj_id]]
            objects.extend(kept)
            years.extend(row_years[o.obj_id] for o in kept if o.obj_id in row_years)
            del new_objs, kept, owned, row_years
            if len(objects) > max_objects:
                raise ValueError(f'more than {max_objects} product objects in the area — '
                                 'choose a smaller region')
        codes.append(code)
        v = (entry.get('version') or '')
        if v:
            versions.add(str(v))
        log.info('product_segments: %s → %d segments in AOI', code, n_read)

    if not objects:
        if mode == 'auto':
            return None
        raise NoProductCoverage('products cover this area but hold no segments here')

    mask = rasterize([(geom_3035, 1)], out_shape=shape, transform=transform, fill=0,
                     dtype=np.uint8, all_touched=False).astype(bool)
    labels[~mask] = 0
    # drop objects that own no pixel inside the AOI (fully overwritten / clipped)
    counts = np.bincount(labels.ravel(), minlength=next_id)
    objects = [o for o in objects if counts[o.obj_id] > 0]
    # nDSM proxy: per-pixel height_mean of the owning object (height colour mode)
    hm = np.zeros(next_id, dtype=np.float32)
    for o in objects:
        hm[o.obj_id] = o.height_mean
    ndsm = hm[labels]
    ndsm[~mask] = np.nan

    import collections
    obs_year = collections.Counter(years).most_common(1)[0][0] if years else None
    n_mm = sum(1 for o in objects if o.is_manmade)
    stats = {
        'total_objects': len(objects), 'manmade': n_mm, 'natural': len(objects) - n_mm,
        'model': 'product', 'classifier': 'product:' + ','.join(sorted(versions)) if versions else 'product',
        'product_codes': codes, 'product_frac': frac,
    }
    if mode == 'product' and (cov['live_parents'] or frac < min_frac):
        warnings.append(f'only {frac * 100:.0f}% of the area has a finished product — the rest is blank'
                        + (f" (no product yet for KG {', '.join(cov['live_parents'])})" if cov['live_parents'] else ''))
    log.info('product_segments: %d objects from %d products (%.0f%% coverage) in %.1fs',
             len(objects), len(codes), frac * 100, time.time() - t0)
    return {'objects': objects, 'labels': labels, 'mask': mask, 'transform': transform,
            'shape': shape, 'ndsm': ndsm, 'stats': stats, 'codes': codes,
            'version': max(versions) if versions else None, 'product_frac': frac,
            'obs_year': obs_year, 'warnings': warnings}
