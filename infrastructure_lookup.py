"""Infrastructure spatial lookup for rule-based classification.

Fetches energy/telecom infrastructure locations from an optional
infrastructure API (``config.POWER_API_URL``; feature disabled when empty),
builds a cKDTree spatial index in EPSG:3035, and provides fast per-segment
proximity queries.

Used by classify_object() in object_segmentation.py to detect:
  - wind_turbine (known location + height signature)
  - solar_panel (known location + NDVI/roughness signature)
  - substation (known location + area/NDVI signature)
  - mast (known telecom/structure location + height/area signature)

Usage::

    lookup = InfrastructureLookup.for_bbox(west, south, east, north)
    nearby = lookup.find_nearby(centroid_e, centroid_n, radius_m=150,
                                categories=["wind_energy", "telecom"])
"""
from __future__ import annotations

import hashlib
import json
import logging
import time
from pathlib import Path
from typing import Optional

import numpy as np
import requests
from pyproj import Transformer

log = logging.getLogger("infrastructure_lookup")

import config

POWER_API_URL = config.POWER_API_URL  # "" => lookup disabled (empty index)
CACHE_DIR = Path("/tmp/infrastructure_cache")
CACHE_MAX_AGE_S = 24 * 3600  # 24 hours
USER_AGENT = "observe-ortho-lidar-at/1.0 (+https://github.com/raffopenssh/observe-ortho-lidar-at)"
REQUEST_TIMEOUT = 60
BUFFER_M_WGS84 = 0.005  # ~500m buffer in degrees

_T_4326_TO_3035 = Transformer.from_crs("EPSG:4326", "EPSG:3035", always_xy=True)


class InfrastructureLookup:
    """Spatial index of infrastructure features for fast proximity queries."""

    def __init__(self, features: list[dict]):
        """Build from a list of parsed feature dicts.
        
        Each dict has: e3035, n3035, category, type, height_agl_m, area_sqm,
        rotor_diameter_m, hub_height_m, capacity_mw, properties (raw).
        """
        self._features = features
        self._tree = None
        self._coords = None
        
        if features:
            coords = np.array([[f["e3035"], f["n3035"]] for f in features])
            self._coords = coords
            try:
                from scipy.spatial import cKDTree
                self._tree = cKDTree(coords)
            except ImportError:
                log.warning("scipy not available \u2014 falling back to brute-force search")

    def __len__(self):
        return len(self._features)

    def find_nearby(
        self,
        e3035: float,
        n3035: float,
        radius_m: float = 150.0,
        categories: list[str] | None = None,
    ) -> list[dict]:
        """Find infrastructure features within radius_m of a point.
        
        Parameters
        ----------
        e3035, n3035 : EPSG:3035 coordinates
        radius_m : search radius in metres
        categories : filter by category (e.g. ["wind_energy", "telecom"]).
                     None = all categories.
        
        Returns
        -------
        List of feature dicts sorted by distance (closest first).
        Each dict includes a 'distance_m' key.
        """
        if not self._features:
            return []

        if self._tree is not None:
            indices = self._tree.query_ball_point([e3035, n3035], radius_m)
        else:
            # Brute force fallback
            dists = np.sqrt((self._coords[:, 0] - e3035)**2 +
                           (self._coords[:, 1] - n3035)**2)
            indices = list(np.where(dists <= radius_m)[0])

        results = []
        pt = np.array([e3035, n3035])
        for idx in indices:
            f = self._features[idx]
            if categories and f["category"] not in categories:
                continue
            dist = float(np.sqrt((f["e3035"] - e3035)**2 + (f["n3035"] - n3035)**2))
            results.append({**f, "distance_m": dist})

        results.sort(key=lambda x: x["distance_m"])
        return results

    @classmethod
    def for_bbox(
        cls,
        west: float,
        south: float,
        east: float,
        north: float,
        buffer_deg: float = BUFFER_M_WGS84,
    ) -> "InfrastructureLookup":
        """Fetch infrastructure for a WGS84 bbox (+buffer) and build index.
        
        Caches API responses for 24h.
        """
        # Add buffer
        w = west - buffer_deg
        s = south - buffer_deg
        e = east + buffer_deg
        n = north + buffer_deg

        features = _fetch_and_parse(w, s, e, n)
        lookup = cls(features)
        log.info("InfrastructureLookup: %d features for bbox "
                 "(%.4f,%.4f,%.4f,%.4f)+buffer",
                 len(features), west, south, east, north)
        return lookup


def _cache_path(bbox: tuple) -> Path:
    raw = f"infra_{bbox[0]:.5f},{bbox[1]:.5f},{bbox[2]:.5f},{bbox[3]:.5f}"
    h = hashlib.md5(raw.encode()).hexdigest()[:16]
    return CACHE_DIR / f"infra_{h}.json"


def _read_cache(bbox: tuple) -> Optional[list[dict]]:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    p = _cache_path(bbox)
    if p.exists():
        try:
            age = time.time() - p.stat().st_mtime
            if age < CACHE_MAX_AGE_S:
                data = json.loads(p.read_text())
                log.info("Infrastructure cache hit: %d features", len(data))
                return data
        except Exception:
            pass
    return None


def _write_cache(bbox: tuple, data: list[dict]) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    p = _cache_path(bbox)
    try:
        p.write_text(json.dumps(data))
    except Exception as e:
        log.warning("Failed to write infrastructure cache: %s", e)


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _fetch_and_parse(
    west: float, south: float, east: float, north: float,
) -> list[dict]:
    """Fetch from API and parse into feature dicts with EPSG:3035 coords."""
    bbox = (west, south, east, north)
    if not POWER_API_URL:
        return []
    cached = _read_cache(bbox)
    if cached is not None:
        return cached

    categories = "solar_energy,wind_energy,substation,telecom,structure"
    try:
        resp = requests.get(
            POWER_API_URL,
            params={
                "bbox": f"{west},{south},{east},{north}",
                "categories": categories,
            },
            headers={"User-Agent": USER_AGENT},
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        geojson = resp.json()
    except Exception as e:
        log.warning("Infrastructure API fetch failed: %s", e)
        return []

    raw_features = geojson.get("features", [])
    parsed = []

    for feat in raw_features:
        props = feat.get("properties", {})
        geom = feat.get("geometry", {})
        coords = geom.get("coordinates")
        if coords is None:
            continue

        geom_type = geom.get("type", "")
        if geom_type == "Point":
            lon, lat = coords[0], coords[1]
        elif geom_type == "LineString":
            mid = len(coords) // 2
            lon, lat = coords[mid][0], coords[mid][1]
        elif geom_type == "Polygon":
            ring = coords[0]
            lon = sum(c[0] for c in ring) / len(ring)
            lat = sum(c[1] for c in ring) / len(ring)
        else:
            continue

        e3035, n3035 = _T_4326_TO_3035.transform(lon, lat)
        category = props.get("category", "")
        feat_type = props.get("type", "")

        parsed.append({
            "e3035": e3035,
            "n3035": n3035,
            "lon": lon,
            "lat": lat,
            "category": category,
            "type": feat_type,
            "height_agl_m": _safe_float(props.get("height_agl_m")),
            "hub_height_m": _safe_float(props.get("hub_height_m")),
            "rotor_diameter_m": _safe_float(props.get("rotor_diameter_m")),
            "area_sqm": _safe_float(props.get("area_sqm")),
            "capacity_mw": _safe_float(props.get("capacity_mw")),
            "name": props.get("name", ""),
            "source": props.get("source", ""),
        })

    log.info("Infrastructure API: %d raw features \u2192 %d parsed for bbox %s",
             len(raw_features), len(parsed), bbox)

    _write_cache(bbox, parsed)
    return parsed
