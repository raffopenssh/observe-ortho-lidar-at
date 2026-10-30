"""v2 ground-truth label raster for one AOI (EPSG:3035, 1 m).

Sources, in *decreasing* priority (later sources never overwrite earlier ones):

  1. building footprints (in-RAM building context, optional)  → roof   (needs nDSM ≥ 2 m)
  2. OSM road centrelines buffered by fclass              → road / path
  3. OSM rail centrelines buffered                        → rail
  4. OSM water areas + waterway lines buffered            → water
  5. INVEKOS Schläge (AMA) crop-type polygons             → crop / grass / vineyard /
                                                            orchard / hedge / shrub /
                                                            water / garden …

Every pixel also gets a *source id* (LABEL_SOURCE) so the training harness can
weight / ablate sources, and the per-source height + NDVI vetoes from v1 are
kept: e.g. a 'grass' pixel with nDSM > 2 m is unlabelled (tree canopy over a
legal meadow), a 'road' pixel with NDVI > 0.3 is unlabelled.

Public release
--------------
The original training stack also painted BEV land-use (NS code) polygons
(``SOURCES`` 6/7).  Those are property-level layers
and were removed; the ``SOURCES`` ids are kept so existing training
artefacts still decode.  Building footprints are only used when the caller
passes them (``LabelContext(footprints=...)``, in RAM, from the optional
``config.BEV_VECTOR_CONTEXT`` path); OSM geometry needs ``config.NE_SERVER_URL``
and INVEKOS needs ``config.INVEKOS_API_URL`` or the local GPKG
(``data/invekos/INSPIRE_SCHLAEGE_2024-1_POLYGON.gpkg``, EPSG:31287).  With
none configured the label raster is empty and the ``dist_*`` context
features fall back to their 500 m cap.
"""
from __future__ import annotations

import logging
import re
import sys as _sys
import time
from pathlib import Path

import numpy as np
import requests
from pyproj import Transformer
from rasterio import features as rfeatures
from shapely.geometry import shape, box
from shapely.ops import transform as shp_transform

log = logging.getLogger("segv2.labels")

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in _sys.path:
    _sys.path.insert(0, str(ROOT))
import config as _config  # noqa: E402
import http_pool as _http_pool  # noqa: E402

# OSM geometry service: the NE server's public, key-less ``GET /api/v1/osm/geometry
# ?bbox=W,S,E,N&cat=road,rail,water,water_area&limit=`` (ODbL, see
# ``/api/v1/docs/llm.txt`` on that host); '' = no OSM context
OSM_GEOMETRY_URL = f"{_config.NE_SERVER_URL}/api/v1/osm/geometry" if _config.NE_SERVER_URL else ""
INVEKOS_GPKG = ROOT / "data/invekos/INSPIRE_SCHLAEGE_2024-1_POLYGON.gpkg"
# INVEKOS Schläge API (years 2022-2026, EPSG:3035, ``nearest_to=<flight year>``
# picks the release closest to the LiDAR flight); '' = disabled.  Spec:
# segv2/INVEKOS_API_SPEC.md.  Local GPKG above is the offline fallback.
INVEKOS_API = _config.INVEKOS_API_URL

_T_4326_3035 = Transformer.from_crs(4326, 3035, always_xy=True)
_T_31287_3035 = Transformer.from_crs(31287, 3035, always_xy=True)
_T_3035_4326 = Transformer.from_crs(3035, 4326, always_xy=True)

# ---------------------------------------------------------------------------
# Taxonomy
# ---------------------------------------------------------------------------
# v1 types kept, plus new ones the data clearly shows (rail, wetland, glacier).
TYPES = [
    "tree", "shrub", "grass", "hedge", "water", "wetland",
    "roof", "greenhouse",
    "road", "path", "parking", "rail",
    "crop", "orchard", "vineyard", "garden",
    "bare_soil", "rock", "glacier", "earthwork",
]
TYPE_ID = {t: i + 1 for i, t in enumerate(TYPES)}
ID_TYPE = {v: k for k, v in TYPE_ID.items()}

SOURCES = ["footprint", "osm_road", "osm_rail", "osm_water", "invekos", "landuse", "landuse_verge"]  # 6/7 unused (legacy ids)
SOURCE_ID = {s: i + 1 for i, s in enumerate(SOURCES)}

# BEV land-use symbol (Nutzungssymbol, Tabelle 8) → type.  Kept as a reference
# table for the v1 LANDUSE_CODE_TO_TYPE cross-checks; no label source paints
# it in the public release (property-level layer removed).
NS_CODE_TO_TYPE = {
    40: "orchard", 41: "roof", 42: "parking", 48: "grass", 52: "garden", 53: "vineyard",
    54: "grass", 55: "shrub", 56: "tree", 57: "shrub", 58: "path", 59: "water", 60: "water",
    61: "wetland", 62: "bare_soil", 63: "parking", 64: "grass", 65: "grass", 72: "grass",
    83: "parking", 84: "earthwork", 87: "rock", 88: "glacier", 92: "rail", 95: "road", 96: "grass",
}

# Height vetoes (m nDSM) — above this the footprint/OSM/INVEKOS label is probably
# under a canopy or structure and gets unlabelled.
MAX_H = {
    "road": 1.5, "path": 1.5, "parking": 1.5, "rail": 1.5, "water": 1.0, "wetland": 1.5,
    "grass": 2.0, "crop": 2.5, "vineyard": 3.5, "garden": 3.0, "bare_soil": 1.5,
    "glacier": 2.0, "earthwork": 3.0, "hedge": 8.0, "orchard": 8.0, "shrub": 6.0,
}
MIN_H = {"roof": 2.0, "tree": 3.0}
# NDVI vetoes — sealed surfaces must not be green (BEV ortho NDVI, 1 m)
MAX_NDVI = {"road": 0.30, "path": 0.35, "parking": 0.30, "rail": 0.35, "bare_soil": 0.30,
            "roof": 0.35, "water": 0.20, "glacier": 0.15, "rock": 0.30}
# Vegetation must be green. Alpine INVEKOS "Almfutterfläche" / BEV NS 52
# (Alpen) polygons legally cover scree, snow beds and bare ridges; without this
# veto ~5 % of "grass" rows sit on bare rock (BEV NDVI < 0.15, elev > 1500 m) and
# the model learns rock→grass.  Thresholds are the ~5th percentile of the BEV
# ortho NDVI per class in the 143-KG build (leaf-off spring flights are real, so
# keep them low).  Same rule is applied segment-wise in train.py for parquets
# built before this veto existed (MIN_NDVI_SEG).
MIN_NDVI = {"grass": 0.15, "tree": 0.20, "shrub": 0.15, "hedge": 0.20, "orchard": 0.10, "wetland": 0.10}
# NOTE: these are the label-time (pixel) floors.  On the real-NIR 171-KG build
# (2026-09-15) the per-class segment-mean NDVI distribution sits well above them
# (5th pct: grass .17, tree .24, shrub .19, orchard .13; the histogram tails off
# into the floor rather than piling up at it), so they are left as-is — a rebuild
# is not needed.  train.py derives the tighter *segment-level* veto (vegetation
# q05 floors + sealed-class q95 ceilings) from the data at train time.

# Label sources are *current* (INVEKOS 2024, footprints + OSM live) while the
# LiDAR behind a KG may be 2006-2024.  Classes whose surface changes on a
# few-year horizon get their weight decayed with DSM age:
#   w *= 1 / (1 + AGE_DECAY[ty] * dsm_age_years)
AGE_DECAY = {"orchard": 0.12, "garden": 0.08, "earthwork": 0.15, "shrub": 0.10, "hedge": 0.10,
             "crop": 0.03, "vineyard": 0.05, "parking": 0.05, "road": 0.03, "roof": 0.05}

# OSM highway fclass → (type, half-width m). Widths are *pavement* half widths.
OSM_ROAD = {
    "motorway": ("road", 12.0), "motorway_link": ("road", 5.0),
    "trunk": ("road", 8.0), "trunk_link": ("road", 4.5),
    "primary": ("road", 4.5), "primary_link": ("road", 3.5),
    "secondary": ("road", 3.8), "secondary_link": ("road", 3.2),
    "tertiary": ("road", 3.2), "tertiary_link": ("road", 3.0),
    "unclassified": ("road", 2.8), "residential": ("road", 2.8),
    "living_street": ("road", 2.5), "service": ("road", 2.0),
    "pedestrian": ("path", 2.0), "track": ("path", 1.5), "track_grade1": ("path", 1.5),
    "track_grade2": ("path", 1.4), "track_grade3": ("path", 1.2), "track_grade4": ("path", 1.0),
    "track_grade5": ("path", 0.8), "path": ("path", 0.7), "footway": ("path", 0.9),
    "cycleway": ("path", 1.2), "bridleway": ("path", 0.9), "steps": ("path", 0.8),
}
OSM_RAIL = {"rail": 2.0, "light_rail": 1.8, "tram": 1.6, "narrow_gauge": 1.5,
            "subway": 1.8, "monorail": 1.5, "funicular": 1.5, "miniature": 0.8}
OSM_WATER_LINE = {"river": 8.0, "canal": 4.0, "stream": 1.2, "drain": 0.8, "ditch": 0.6}

# INVEKOS SNAR_BEZEICHNUNG → type. Regex on the (upper-case) crop name; first
# match wins, ordered specific → generic.
INVEKOS_RULES: list[tuple[str, str]] = [
    (r"GLÖZ HECKE", "hedge"),
    (r"GLÖZ FELDGEHÖLZ|GLÖZ BAUM|GEBÜSCH", "shrub"),
    (r"GLÖZ TEICH|TÜMPEL", "water"),
    (r"GLÖZ GRABEN|UFERRANDSTREIFEN", "grass"),
    (r"GLÖZ RAIN|BÖSCHUNG|TROCKENSTEINMAUER|STEINRIEGEL|STEINHAGE", "grass"),
    (r"NATURDENKMAL", "tree"),
    (r"^WEIN|REBSCHUL|WEINGARTEN", "vineyard"),
    (r"TAFELÄPFEL|TAFELBIRNEN|MARILLEN|ZWETSCHKEN|KIRSCHEN|PFIRSICHE|WALNÜSSE|EDELKASTANIEN|"
     r"HASELNÜSSE|HOLUNDER|ANDERES OBST|STREUOBST|OBSTANLAGE|QUITTEN|WEICHSELN|BAUMSCHULE", "orchard"),
    (r"STRAUCHBEEREN|ERDBEEREN|HOPFEN", "crop"),
    (r"GEWÄCHSHAUS|FOLIENTUNNEL", "greenhouse"),
    (r"ENERGIEHOLZ|KURZUMTRIEB|CHRISTBAUM|FORST", "tree"),
    (r"ALMWEIDE|ALM|BERGMÄHDER", "grass"),
    (r"WIESE|WEIDE|GRÜNBRACHE|GRÜNLANDBRACHE|GRÜNLAND|STREUWIESE|HUTWEIDE|KLEEGRAS|"
     r"FUTTERGRÄSER|WECHSELWIESE|DAUERWEIDE|ACKERWEIDE|MÄHWIESE|RASEN", "grass"),
    (r"BRACHE|STILLLEGUNG", "grass"),
    (r"HAUS- UND NUTZGARTEN|HAUSGARTEN|NUTZGARTEN", "garden"),
    (r"BLUMEN|ZIERPFLANZEN", "crop"),
    (r".", "crop"),  # everything else in INVEKOS is an arable crop
]
_INVEKOS_RE = [(re.compile(p), t) for p, t in INVEKOS_RULES]


def invekos_type(name: str | None) -> str | None:
    if not name:
        return None
    u = name.upper()
    for rx, t in _INVEKOS_RE:
        if rx.search(u):
            return t
    return None


# ---------------------------------------------------------------------------
# Fetchers
# ---------------------------------------------------------------------------
_SESSION = requests.Session()
_SESSION.headers["Accept-Encoding"] = "gzip"


class UpstreamUnavailable(RuntimeError):
    """An OSM / INVEKOS fetch failed after every retry.  Raised by the
    strict fetchers so the processor can DEFER the KG instead of baking a
    product on an empty OSM context (dist_road=500 m everywhere, no
    road/rail/water masks)."""


# retry ladder for the strict fetchers: ~4 min total, enough to ride out a
# restart of the upstream service (503 for 30-90 s) without deferring the KG
STRICT_BACKOFF_S = (5, 10, 20, 40, 60, 60, 60)


def _get_json(url, params, timeout=180, retries=3, backoff=None):
    if not url:
        return None
    last = None
    for a in range(retries):
        if a and backoff:
            time.sleep(backoff[min(a - 1, len(backoff) - 1)])
        try:
            r = _SESSION.get(url, params=params, timeout=timeout,
                             headers={"User-Agent": _http_pool.USER_AGENT})
            r.raise_for_status()
            return r.json()
        except Exception as e:  # noqa: BLE001
            last = e
            log.warning("GET %s failed (%d): %s", url, a, e)
    if backoff:
        raise UpstreamUnavailable(f"{url.split('?')[0]} unavailable after {retries} attempts: {last}")
    return None


def _to_3035(geom):
    return shp_transform(_T_4326_3035.transform, geom)


def fetch_osm(bbox_wgs: tuple[float, float, float, float], strict: bool = False) -> dict:
    """{'road': [(geom3035,fclass)], 'rail': [...], 'water_line': [...], 'water_area': [...]}

    ``strict`` (the processor): retry over STRICT_BACKOFF_S and raise
    :class:`UpstreamUnavailable` instead of returning an empty context."""
    w, s, e, n = bbox_wgs
    out = {"road": [], "rail": [], "water_line": [], "water_area": []}
    if not OSM_GEOMETRY_URL:
        # no OSM geometry service configured (config.NE_SERVER_URL empty): empty
        # context, never an upstream error — strict callers would otherwise
        # defer every KG forever
        return out
    # bbox-keyed; ``cat=water_area`` gives fillable (Multi)Polygons,
    # ``cat=water`` the centreline/outline chunks.
    d = _get_json(OSM_GEOMETRY_URL,
                  {"bbox": f"{w},{s},{e},{n}", "cat": "road,rail,water,water_area",
                   "limit": 100000},
                  retries=len(STRICT_BACKOFF_S) + 1 if strict else 3,
                  backoff=STRICT_BACKOFF_S if strict else None)
    if d is None:
        if strict:
            raise UpstreamUnavailable("osm/geometry returned no document")
        return out
    if not d:
        return out
    for f in d.get("features", []):
        p = f.get("properties", {})
        try:
            g = _to_3035(shape(f["geometry"]))
        except Exception:  # noqa: BLE001
            continue
        cat, fc = p.get("cat"), p.get("fclass", "")
        if cat == "road":
            out["road"].append((g, fc))
        elif cat == "rail":
            out["rail"].append((g, fc))
        elif cat == "water_area":
            out["water_area"].append((g, fc))
        elif cat == "water":
            if g.geom_type.startswith("Line"):
                out["water_line"].append((g, fc))
    return out


INVEKOS_MAX_SIDE_M = 20_000.0      # API refuses > 25 km per side (400)
INVEKOS_MIN_SIDE_M = 500.0         # subdivision floor on ``truncated``


def _invekos_get(bbox_3035, params, strict):
    p = dict(params, bbox=",".join(f"{v:.1f}" for v in bbox_3035))
    return _get_json(f"{INVEKOS_API}/schlaege", p, timeout=120,
                     retries=len(STRICT_BACKOFF_S) + 1 if strict else 3,
                     backoff=STRICT_BACKOFF_S if strict else None)


def _invekos_cells(bbox_3035, params, strict, depth=0):
    """Yield feature-collection documents covering ``bbox_3035``: split into
    ≤INVEKOS_MAX_SIDE_M cells up front and subdivide any cell the API
    reports ``truncated`` (quadtree down to INVEKOS_MIN_SIDE_M) — a
    truncated answer used to empty the whole INVEKOS layer silently."""
    x0, y0, x1, y1 = bbox_3035
    w, h = x1 - x0, y1 - y0
    if depth == 0 and (w > INVEKOS_MAX_SIDE_M or h > INVEKOS_MAX_SIDE_M):
        import math
        nx, ny = max(1, math.ceil(w / INVEKOS_MAX_SIDE_M)), max(1, math.ceil(h / INVEKOS_MAX_SIDE_M))
        for i in range(nx):
            for j in range(ny):
                yield from _invekos_cells((x0 + i * w / nx, y0 + j * h / ny,
                                           x0 + (i + 1) * w / nx, y0 + (j + 1) * h / ny), params, strict, 1)
        return
    d = _invekos_get(bbox_3035, params, strict)
    if d is None:
        if strict:
            raise UpstreamUnavailable("invekos/schlaege returned no document")
        yield None
        return
    if d.get("truncated") and min(w, h) > INVEKOS_MIN_SIDE_M:
        xm, ym = (x0 + x1) / 2, (y0 + y1) / 2
        for q in ((x0, y0, xm, ym), (xm, y0, x1, ym), (x0, ym, xm, y1), (xm, ym, x1, y1)):
            yield from _invekos_cells(q, params, strict, depth + 1)
        return
    if d.get("truncated"):
        log.warning("invekos: cell %.0f,%.0f still truncated at %.0f m — partial", x0, y0, min(w, h))
    yield d


def fetch_invekos_api(bbox_3035, nearest_to: int | None = None, year: int | None = None,
                      strict: bool = False) -> tuple[list[tuple], int] | None:
    """([(geom3035, type, snar_name)], year_used) from the INVEKOS API, or None on failure.

    The request is tiled (≤20 km cells, API limit 25 km) and any
    ``truncated`` cell is subdivided, so a big KG never loses the layer.
    ``strict``: transport failure (5xx / timeout after STRICT_BACKOFF_S)
    raises :class:`UpstreamUnavailable`."""
    if not INVEKOS_API:
        return None
    params = {"crs": "3035"}
    if year:
        params["year"] = int(year)
    elif nearest_to:
        params["nearest_to"] = int(nearest_to)
    out, seen, yr = [], set(), 0
    for d in _invekos_cells(tuple(float(v) for v in bbox_3035), params, strict):
        if d is None or "features" not in d:
            return None
        yr = yr or int(d.get("year") or 0)
        for f in d["features"]:
            g = f.get("geometry")
            if not g:
                continue
            name = (f.get("properties") or {}).get("snar")
            ty = invekos_type(name)
            if not ty:
                continue
            # a cell boundary cuts nothing (API returns whole Schläge) but a
            # polygon straddling two cells comes back twice → dedupe by id/wkb
            fid = (f.get("id") or (f.get("properties") or {}).get("id"))
            geom = shape(g)
            if geom.is_empty:
                continue
            key = fid if fid is not None else geom.wkb
            if key in seen:
                continue
            seen.add(key)
            out.append((geom, ty, name))
    return out, yr


def fetch_invekos(bbox_3035: tuple[float, float, float, float], nearest_to: int | None = None) -> list[tuple]:
    """[(geom3035, type, snar_name)] — INVEKOS API (year nearest the flight) first,
    local 2024-1 GPKG as fallback."""
    r = fetch_invekos_api(bbox_3035, nearest_to=nearest_to)
    if r is not None:
        log.info("invekos api: %d polys, year %s (nearest_to=%s)", len(r[0]), r[1], nearest_to)
        return r[0]
    log.warning("invekos api unavailable → local GPKG 2024-1")
    if not INVEKOS_GPKG.exists():
        return []
    import pyogrio
    t = Transformer.from_crs(3035, 31287, always_xy=True)
    x0, y0, x1, y1 = bbox_3035
    xs, ys = zip(*[t.transform(x, y) for x, y in ((x0, y0), (x1, y0), (x1, y1), (x0, y1))])
    df = pyogrio.read_dataframe(str(INVEKOS_GPKG), bbox=(min(xs), min(ys), max(xs), max(ys)),
                                columns=["SNAR_BEZEICHNUNG"])
    out = []
    for geom, name in zip(df.geometry, df["SNAR_BEZEICHNUNG"]):
        if geom is None or geom.is_empty:
            continue
        ty = invekos_type(name)
        if ty:
            out.append((shp_transform(_T_31287_3035.transform, geom), ty, name))
    return out


# ---------------------------------------------------------------------------
# Rasteriser
# ---------------------------------------------------------------------------
def _burn(shapes, transform, shape_hw, dtype=np.uint8):
    if not shapes:
        return np.zeros(shape_hw, dtype)
    return rfeatures.rasterize(shapes, out_shape=shape_hw, transform=transform,
                               fill=0, dtype=dtype, all_touched=False)


def _paint(label, source, weight, mask, ty, src, w=1.0):
    free = mask & (label == 0)
    label[free] = TYPE_ID[ty]
    source[free] = SOURCE_ID[src]
    weight[free] = w


class LabelContext:
    """Fetched vector inputs for one KG; can rasterise any tile window."""

    def __init__(self, kg_code: str, bbox_3035: tuple[float, float, float, float],
                 flight_year: int | None = None, footprints=None):
        self.kg = kg_code
        self.bbox_3035 = bbox_3035
        self.flight_year = flight_year
        x0, y0, x1, y1 = bbox_3035
        pts = [_T_3035_4326.transform(x, y) for x, y in ((x0, y0), (x1, y1), (x0, y1), (x1, y0))]
        lons, lats = zip(*pts)
        pad = 0.002
        self.bbox_wgs = (min(lons) - pad, min(lats) - pad, max(lons) + pad, max(lats) + pad)
        # in-RAM building footprints (EPSG:3035) from the caller, or none
        self.footprints = list(footprints or [])
        self.osm = fetch_osm(self.bbox_wgs)
        self.inv = fetch_invekos(bbox_3035, nearest_to=flight_year)
        log.info("KG %s osm: %d road, %d rail, %d water lines, %d water areas; invekos: %d",
                 kg_code, len(self.osm["road"]), len(self.osm["rail"]),
                 len(self.osm["water_line"]), len(self.osm["water_area"]), len(self.inv))
        # pre-buffered OSM surfaces (3035 metres)
        self.road_polys, self.path_polys = [], []
        for g, fc in self.osm["road"]:
            ty, hw = OSM_ROAD.get(fc, ("road", 2.5))
            (self.road_polys if ty == "road" else self.path_polys).append(g.buffer(hw, cap_style=2))
        self.rail_polys = [g.buffer(OSM_RAIL.get(fc, 1.8), cap_style=2) for g, fc in self.osm["rail"]]
        self.water_polys = [g for g, _ in self.osm["water_area"]]
        self.water_polys += [g.buffer(OSM_WATER_LINE.get(fc, 1.5), cap_style=2)
                             for g, fc in self.osm["water_line"]]

    def rasterize(self, transform, shape_hw, ndsm: np.ndarray | None, ndvi: np.ndarray | None,
                  dsm_age: np.ndarray | None = None):
        """Returns (label u8, source u8, weight f32) arrays for the window.

        ``ndvi`` must be a *real* NIR-derived index (gpkg_raster.nir_years);
        ``dsm_age`` (years, per pixel) decays the weight of time-sensitive
        classes (AGE_DECAY)."""
        h, w = shape_hw
        label = np.zeros((h, w), np.uint8)
        source = np.zeros((h, w), np.uint8)
        weight = np.zeros((h, w), np.float32)
        win = box(transform.c, transform.f + transform.e * h,
                  transform.c + transform.a * w, transform.f)

        def _clip(geoms):
            return [g for g in geoms if g.intersects(win)]

        # 1. footprints → roof
        fp = _burn([(g, 1) for g in _clip(self.footprints)], transform, shape_hw).astype(bool)
        _paint(label, source, weight, fp, "roof", "footprint")
        # 2. OSM roads / paths
        _paint(label, source, weight, _burn([(g, 1) for g in _clip(self.road_polys)], transform, shape_hw).astype(bool), "road", "osm_road")
        _paint(label, source, weight, _burn([(g, 1) for g in _clip(self.path_polys)], transform, shape_hw).astype(bool), "path", "osm_road")
        # 3. rail
        _paint(label, source, weight, _burn([(g, 1) for g in _clip(self.rail_polys)], transform, shape_hw).astype(bool), "rail", "osm_rail")
        # 4. water
        _paint(label, source, weight, _burn([(g, 1) for g in _clip(self.water_polys)], transform, shape_hw).astype(bool), "water", "osm_water")
        # 5. INVEKOS (shrink by 1 m to avoid boundary bleed)
        inv_by_type: dict[str, list] = {}
        for g, ty, _ in self.inv:
            if g.intersects(win):
                inv_by_type.setdefault(ty, []).append(g.buffer(-1.0))
        for ty, geoms in inv_by_type.items():
            m = _burn([(g, 1) for g in geoms if not g.is_empty], transform, shape_hw).astype(bool)
            _paint(label, source, weight, m, ty, "invekos")
        # --- physical vetoes ---
        if ndsm is not None:
            for ty, mx in MAX_H.items():
                bad = (label == TYPE_ID[ty]) & (ndsm > mx)
                label[bad] = 0
            for ty, mn in MIN_H.items():
                bad = (label == TYPE_ID[ty]) & (ndsm < mn)
                label[bad] = 0
        if ndvi is not None:
            for ty, mx in MAX_NDVI.items():
                bad = (label == TYPE_ID[ty]) & (ndvi > mx)
                label[bad] = 0
            for ty, mn in MIN_NDVI.items():
                bad = (label == TYPE_ID[ty]) & (ndvi < mn)
                label[bad] = 0
        if dsm_age is not None:
            age = np.clip(np.nan_to_num(dsm_age, nan=0.0), 0, 20).astype(np.float32)
            for ty, k in AGE_DECAY.items():
                m = label == TYPE_ID[ty]
                weight[m] /= (1.0 + k * age[m])
        source[label == 0] = 0
        weight[label == 0] = 0
        return label, source, weight


def summarize(label: np.ndarray, source: np.ndarray) -> dict:
    out = {}
    tot = int((label > 0).sum())
    out["labelled_frac"] = round(tot / label.size, 3)
    ids, cnt = np.unique(label[label > 0], return_counts=True)
    out["by_type"] = {ID_TYPE[int(i)]: int(c) for i, c in zip(ids, cnt)}
    ids, cnt = np.unique(source[source > 0], return_counts=True)
    out["by_source"] = {SOURCES[int(i) - 1]: int(c) for i, c in zip(ids, cnt)}
    return out
