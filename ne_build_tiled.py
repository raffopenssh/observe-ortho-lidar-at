"""Memory-bounded NEC1 build: ``ne_cells.algo.build_cells`` over sub-domains.

Part of the optional ``config.BEV_VECTOR_CONTEXT`` (bevdirect) block with
``vct_pilot`` / ``ne_h3``: the input objects are the public BEV vector-tile
features held in RAM for one KG build; nothing property-level is written
to disk, products or logs — only the aggregated per-cell statistics.

Why
---
``build_cells`` (frozen reference algorithm, vtcseamless-py) keeps a dense
per-cell accumulator (``acc``: lists of clipped parcel / landuse / footprint
pieces per res-12 cell) plus one ``rows`` dict per product cell — measured
~3.4 KB/cell peak (75110: 168 k cells → +555 MB, rows alone 2.3 KB/cell).
A parent like Sölden 80110 (1 525 270 cells) therefore needs ≈ 5–6 GB in the
helper alone; with the KG worker, the processor parent and bevdirect-serve
in the same 8 GB cgroup the helper is SIGKILLed (``helper rc=-9``, Oct
2026, 42 blocks × ~15 min each).

How
---
The algorithm is a *function of the input domain*: a product cell lying
fully inside ``input_bbox`` depends only on the objects intersecting it
(``INPUT_RULE`` = geom-intersects-input_bbox; cells not fully inside the
domain are withheld).  So the global domain is cut into an ``nx × ny``
grid of *cores*; every core is built with a padded sub-domain
(``PAD_DEG`` ≫ a res-10 cell + its children) clipped to the global bbox,
and a cell / k-cell is taken from the sub-build whose core holds its H3
centre ("home").  For a homed cell every object that touches it touches
the padded sub-domain, so its row — and for a homed res-10 k-cell the
rows of all its children, hence the k-accumulator — is identical to the
monolithic build.  Records are packed per sub-build (``LU_REC`` /
``K_REC`` are per-cell, and ``pack_records`` is just the id-sorted
concatenation), so the peak is one sub-build plus ~100 B per packed cell.

Verified byte-identical on 75110 (168 243 cells) and 80110 (1 525 270
cells, digest ``47b0708d15561d74`` = the fleet's four-observer value) —
see docs/vct-pilot.md → "Helper rc=-9 on huge parents".

Header ``stats``: ``cells``, ``kcells``, ``normalised``,
``declared_unknown`` and the object / domain counts are exact; the
omission counters ``slivers_omitted``, ``no_own_share``,
``kcells_withheld_coverage`` are summed over the sub-builds and
double-count cells in the overlap bands (75110: 24/69 vs 18/49), and
``cells_outside_domain`` is omitted (per tile it counts the sub-domain
cut) — provenance only (not part of the digest, not in the /contrib
report); ``stats.tiled`` carries the sub-build count so a reader knows.
"""
from __future__ import annotations

import gc
import logging
import math
import time

log = logging.getLogger("ne_build_tiled")

PAD_DEG = 0.006            # ≥ 440 m: a res-10 cell + its res-12 children protrude < 120 m
TARGET_CELLS = int(__import__("os").environ.get("VCT_TILE_TARGET", "120000"))   # per sub-build (~400 MB at 3.4 KB/cell)
TILE_THRESHOLD = 300_000   # estimated cells above which the tiled path is used
CELL_AREA_M2 = 307.1       # mean H3 res-12 cell area

_SUMMED = ("slivers_omitted", "no_own_share", "kcells_withheld_coverage")   # cells_outside_domain: meaningless per tile, omitted


def estimate_cells(own_geoms_wgs, lat: float) -> int:
    """≈ number of res-12 product cells for the KG: own-parcel area / cell area
    (+ a 10 % border allowance).  Degrees → m² with the local cos(lat)."""
    try:
        from shapely.ops import unary_union
        a_deg2 = unary_union(list(own_geoms_wgs)).area
    except Exception:  # noqa: BLE001
        a_deg2 = sum(g.area for g in own_geoms_wgs)
    m2 = a_deg2 * 111_320.0 * 111_320.0 * math.cos(math.radians(lat))
    return int(m2 / CELL_AREA_M2 * 1.1)


def _grid(input_bbox, est_cells: int, target: int):
    W, S, E, N = input_bbox
    n = max(1, math.ceil(est_cells / float(target)))
    nx = max(1, int(round(math.sqrt(n * (E - W) / max(N - S, 1e-9)))))
    ny = max(1, math.ceil(n / nx))
    return nx, ny


def _home(lat: float, lng: float, input_bbox, nx: int, ny: int) -> tuple[int, int]:
    W, S, E, N = input_bbox
    ix = min(nx - 1, max(0, int((lng - W) / (E - W) * nx))) if E > W else 0
    iy = min(ny - 1, max(0, int((lat - S) / (N - S) * ny))) if N > S else 0
    return ix, iy


def _dedupe(parcels, footprints, landuse):
    """``build_cells``' own tile-copy dedupe (verbatim), for the header counts."""
    import collections
    from ne_cells.algo import _union_sorted
    copies = collections.defaultdict(list)
    for p in parcels:
        copies[p["id"]].append(p)
    byid = {}
    for pid, cs in copies.items():
        complete = [p for p in cs if p.get("complete", True)]
        if complete:
            byid[pid] = max(complete, key=lambda p: (p["geometry"].area, p["geometry"].wkb))
        else:
            q = dict(max(cs, key=lambda p: (p["geometry"].area, p["geometry"].wkb)))
            q["geometry"] = _union_sorted([p["geometry"] for p in cs])
            byid[pid] = q
    P = list(byid.values())
    F = list({f["geometry"].wkb: f for f in footprints}.values())
    L = list({(l["ns"], l["geometry"].wkb): l for l in landuse}.values())
    return P, F, L


def build_cells_tiled(kg: str, P, F, L, *, input_bbox, coverage_bbox=None, target_cells: int = TARGET_CELLS,
                      est_cells: int | None = None, heartbeat=None):
    """Returns ``(raw_records_bytes, cells_n, kcells_n, stats, n_tiles)`` — the
    same ``raw`` ``pack_records(res.cells, res.kcells)`` would give for the
    monolithic ``build_cells(kg, P, F, L, coverage_bbox=…, input_bbox=…)``."""
    import h3
    from ne_cells.algo import build_cells, _in_domain, FLAG_NORMALISED, FLAG_DECLARED_UNKNOWN
    from ne_cells.pack import LU_REC, K_REC

    kg = str(kg).zfill(5)
    W, S, E, N = [float(v) for v in input_bbox]
    cov = tuple(float(v) for v in (coverage_bbox or input_bbox))
    if est_cells is None:
        own = [p["geometry"] for p in P if p.get("kg") == kg]
        est_cells = estimate_cells(own, (S + N) / 2.0) if own else 0
    nx, ny = _grid((W, S, E, N), est_cells, target_cells)
    dx, dy = (E - W) / nx, (N - S) / ny
    # Dedupe the tile copies ONCE over the whole input (verbatim replica of the
    # build's own pass).  The build takes id/kg/ez/area_sqm from the largest
    # copy and unions truncated copies; doing that on a per-sub-domain SUBSET
    # of the copies changed the chosen copy / union for wide parcels on the
    # tile seams (80110: digest drift at equal cell count).  The build's
    # dedupe is idempotent on the deduped list, so the per-tile pre-filter
    # below is then exactly the build's own input rule applied early.
    P, F, L = _dedupe(P, F, L)
    lu_rec: dict[str, bytes] = {}
    k_rec: dict[str, bytes] = {}
    stats: dict = {}
    t0 = time.time()
    for iy in range(ny):
        for ix in range(nx):
            sub = (max(W, W + ix * dx - PAD_DEG), max(S, S + iy * dy - PAD_DEG),
                   min(E, W + (ix + 1) * dx + PAD_DEG), min(N, S + (iy + 1) * dy + PAD_DEG))
            # pre-filter (the build re-applies the rule itself; this keeps its
            # sort / accumulator passes proportional to the sub-domain)
            Pi, Fi, Li = _in_domain(P, sub), _in_domain(F, sub), _in_domain(L, sub)
            if not any(p.get("kg") == kg for p in Pi):
                # no own parcel touches this sub-domain (bbox corner outside the
                # KG) — nothing to home; the build would return empty anyway
                # (and with a non-empty P but no own parcel IN DOMAIN it would
                # hit h3.geo_to_cells(None)).
                del Pi, Fi, Li
                log.info("ne_build_tiled: %s tile %d/%d: no own parcels, skipped", kg, iy * nx + ix + 1, nx * ny)
                continue
            res = build_cells(kg, Pi, Fi, Li, coverage_bbox=cov, input_bbox=sub)
            del Pi, Fi, Li
            n_home = 0
            for c, r in res.cells.items():
                lat, lng = h3.cell_to_latlng(c)
                if _home(lat, lng, (W, S, E, N), nx, ny) != (ix, iy):
                    continue
                n_home += 1
                lu_rec[c] = LU_REC.pack(int(c, 16), r["kg_share"], r["land"], r["cov"], r["flags"], bytes(r["lu"]),
                                        r["bld_cover"], r["bld_n"], r["gk"], r["n_parc"], r["line_m_ha"],
                                        r["orient_coh"], r["orient_axis"])
                if r["flags"] & FLAG_NORMALISED:
                    stats["normalised"] = stats.get("normalised", 0) + 1
                if r["flags"] & FLAG_DECLARED_UNKNOWN:
                    stats["declared_unknown"] = stats.get("declared_unknown", 0) + 1
            for c, r in res.kcells.items():
                lat, lng = h3.cell_to_latlng(c)
                if _home(lat, lng, (W, S, E, N), nx, ny) != (ix, iy):
                    continue
                k_rec[c] = K_REC.pack(int(c, 16), r["n_parc"], r["parc_p10"], r["parc_p50"], r["parc_p90"], r["ez_n"],
                                      r["parc_per_ez"], r["bld_n"], r["bld_p50"])
            for k_ in _SUMMED:
                if res.stats.get(k_):
                    stats[k_] = stats.get(k_, 0) + int(res.stats[k_])
            n_sub = len(res.cells)
            del res
            gc.collect()
            msg = f"tile {iy * nx + ix + 1}/{nx * ny}: {n_sub} cells built, {n_home} homed, {len(lu_rec)} total ({time.time() - t0:.0f}s)"
            log.info("ne_build_tiled: %s %s", kg, msg)
            if heartbeat:
                try:
                    heartbeat(msg)
                except Exception:  # noqa: BLE001
                    pass
    # exact object / domain counts over the GLOBAL domain (what the monolithic
    # build reports: deduped copies, then the input rule)
    n_in = (len(P), len(F), len(L))
    Pg, Fg, Lg = _in_domain(P, (W, S, E, N)), _in_domain(F, (W, S, E, N)), _in_domain(L, (W, S, E, N))
    stats.update(parcels=len(Pg), own_parcels=sum(1 for p in Pg if p.get("kg") == kg), footprints=len(Fg), landuse=len(Lg),
                 parcels_outside_domain=n_in[0] - len(Pg), footprints_outside_domain=n_in[1] - len(Fg),
                 landuse_outside_domain=n_in[2] - len(Lg), cells=len(lu_rec), kcells=len(k_rec), tiled=nx * ny)
    del Pg, Fg, Lg
    raw = b"".join(lu_rec[c] for c in sorted(lu_rec)) + b"".join(k_rec[c] for c in sorted(k_rec))
    return raw, len(lu_rec), len(k_rec), stats, nx * ny
