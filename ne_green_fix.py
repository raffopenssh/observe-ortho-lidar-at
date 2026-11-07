"""Green-ground override for NE *observed* cells (NEO1) — read-time only.

Problem (2026-10-26, KG 63330 / cell 8c1e108e86e1bff): the segv2 LightGBM
leans on the BEV ortho NDVI / brightness and ignores multi-season S2.  Where
the 2024 ortho is dull over meadows it labels contiguous pasture ``parking``
(→ NE group ``bau``) or ``rock`` (→ ``alpen``) although the fused S2 NDVI of
the same object is 0.5–0.8.  Corpus-wide the median KG has 26 % of its bau
cells on S2-green ground (p90 72 %).

Until the classifier is retrained the fix lives ONLY at the point where NEO1
rows leave the primary: ``ne_h3.neo1_section_from_h3b`` (what is POSTed to
the optional NE server) and ``cells_api._load_chunk`` (the public cells API).  **No .h3b
file, Zenodo product or JSON is rewritten** — the rule is deterministic and
idempotent, so it can be re-applied on every read and dropped in one place
once products are right.

Rule (per res-12 cell, NEO1 row layout ``ne_h3.OBS_REC``):
  cell ``ndvi_mean`` (fused S2, byte 25) >= :data:`NDVI_MIN` and
  phenology (byte 40) not ``road_or_bare`` →
  ``obs_lu[bau]`` and ``obs_lu[alpen]`` are moved into ``obs_lu[gruen]``.
The verdict (byte 29) is recomputed with the ``ne_h3.build_observed`` rules
for the touched rows (only ``green_undeclared`` can flip).

Tree rule (2026-11-07, applied BEFORE the green rule, same read points):
where the BEV ortho operate 2023160 is hazy / low-NIR (seam at lat 47.218,
lower Inn valley: 84011 Schönwies, 84015 Zams, 80007 Mils, 80001, 80003,
84002 …) segv2 labels closed forest ``rock`` (→ ``alpen``, sometimes ``bau``)
— and the green rule then turns it into ``gruen``, so umfeld shows grassland
over real forest (cell ``8c1f88472a0b3ff``: alpen=250 but h_max 13.5 m,
crown_frac 33 %, h_p90 10.5 m, 5 tree apices).  The LiDAR columns of the same row are
flight-independent and correct, so:
  ``crown_frac`` (byte 24) >= :data:`TREE_CROWN_MIN` and
  ``h_p90`` (byte 20, nDSM p90 of the cell) >= :data:`TREE_HP90_MIN` m and
  phenology (byte 40) not ``road_or_bare`` →
  ``obs_lu[alpen] + obs_lu[bau] + obs_lu[gruen]`` are moved into ``obs_lu[wald]``
  (``forest_loss`` flips to ``consistent`` where wald is declared).
Thresholds (national sample 7.4 M cells): real forest (wald >= 200) has crown
p5 = 0.22 / p25 = 0.43 and h_p90 p5 = 9.5 m — ``crown >= 0.5 & h_max >= 5``
would catch only 65 % of it (and not the 80007 cell, crown 0.33); this rule
catches 88 % of forest, 6.9 % of pure gruen/alpen cells (treed pasture,
forest edges), 0.7 % of acker, ~0 % of bau.  h_p90 (not h_max) so that one
tall tree in a meadow does not qualify.
"""
from __future__ import annotations

import numpy as np

FIX_ID = "green-2+tree-1"   # bump when a rule changes (digest caches / ETags / LRUs key on it)
NDVI_MIN = 0.5
MOVE_GROUPS = ("bau", "alpen")
TARGET_GROUP = "gruen"
# tree rule (LiDAR-only evidence, see module doc)
TREE_CROWN_MIN = 0.3        # crown_frac (byte 24, /254)
TREE_HP90_MIN = 8.0         # m, h_p90 (byte 20), 0.5 m units, 255 nodata
TREE_MOVE_GROUPS = ("alpen", "bau", "gruen")
TREE_TARGET_GROUP = "wald"

_G = ("geb", "bau", "acker", "gruen", "wald", "wasser", "verkehr", "alpen", "sonst")
_GI = {g: i for i, g in enumerate(_G)}
_PHENO_ROAD_OR_BARE = 5
_V_CONSISTENT, _V_FOREST_LOSS, _V_FOREST_GAIN, _V_SEALED, _V_BUILDING, _V_GREEN, _V_UNKNOWN = range(7)
OBS_REC_BYTES = 41
LU_REC_BYTES = 30


def _ndvi_from_u8(b: np.ndarray) -> np.ndarray:
    v = b.astype(np.float32) / 254.0 * 2.0 - 1.0
    return np.where(b == 255, np.nan, v)


def mask(obs: np.ndarray) -> np.ndarray:
    """Rows (n×41 uint8) the override touches: green by S2 AND carrying bau/alpen."""
    ndvi = _ndvi_from_u8(obs[:, 25])
    green = np.nan_to_num(ndvi, nan=-1.0) >= NDVI_MIN
    green &= obs[:, 40] != _PHENO_ROAD_OR_BARE
    has = np.zeros(len(obs), bool)
    for g in MOVE_GROUPS:
        has |= obs[:, 8 + _GI[g]] > 0
    return green & has


def tree_mask(obs: np.ndarray) -> np.ndarray:
    """Rows the tree rule touches: closed canopy by LiDAR AND carrying alpen/bau/gruen."""
    crown = obs[:, 24].astype(np.float32) / 254.0
    crown = np.where(obs[:, 24] == 255, 0.0, crown)
    h_p90 = obs[:, 20].astype(np.float32) / 2.0
    h_p90 = np.where(obs[:, 20] == 255, 0.0, h_p90)
    m = (crown >= TREE_CROWN_MIN) & (h_p90 >= TREE_HP90_MIN)
    m &= obs[:, 40] != _PHENO_ROAD_OR_BARE
    has = np.zeros(len(obs), bool)
    for g in TREE_MOVE_GROUPS:
        has |= obs[:, 8 + _GI[g]] > 0
    return m & has


def _move(sel: np.ndarray, groups: tuple, target: str) -> None:
    moved = np.zeros(len(sel), np.int32)
    for g in groups:
        moved += sel[:, 8 + _GI[g]]
        sel[:, 8 + _GI[g]] = 0
    sel[:, 8 + _GI[target]] = np.minimum(255, sel[:, 8 + _GI[target]].astype(np.int32) + moved).astype(np.uint8)


def _verdict(obs: np.ndarray, lu: np.ndarray) -> np.ndarray:
    """``ne_h3.build_observed`` verdict rules on (already fixed) rows."""
    o = obs[:, 8:17].astype(np.float64) / 255.0
    d = lu[:, 12:21].astype(np.float64) / 255.0
    gi = _GI
    d_wald, o_wald = d[:, gi["wald"]], o[:, gi["wald"]]
    d_geb = d[:, gi["geb"]] + d[:, gi["bau"]]
    d_green = d[:, gi["gruen"]] + d[:, gi["acker"]] + d[:, gi["wald"]]
    o_sealed = o[:, gi["verkehr"]] + o[:, gi["geb"]]
    o_green = o[:, gi["gruen"]] + o[:, gi["acker"]] + o[:, gi["wald"]]
    d_sealed = d[:, gi["verkehr"]] + d[:, gi["geb"]] + d[:, gi["bau"]]
    o_bld = obs[:, 17].astype(np.float64) / 255.0
    v = np.full(len(obs), _V_CONSISTENT, np.uint8)
    v[(d_wald >= 0.6) & (o_wald < 0.25)] = _V_FOREST_LOSS
    v[(d_wald < 0.15) & (o_wald >= 0.6) & (d_green >= 0.5)] = _V_FOREST_GAIN
    v[(o_sealed >= 0.5) & (d_green >= 0.7)] = _V_SEALED
    v[(o_bld >= 0.15) & (d_geb < 0.03)] = _V_BUILDING
    v[(o_green >= 0.7) & (d_sealed >= 0.6)] = _V_GREEN
    return v


def fix_rows(obs_raw: bytes, lu_raw: bytes | None = None) -> tuple[bytes, int]:
    """Apply both overrides to NEO1 rows (tree rule first, then the green
    rule on what is left).  Returns ``(fixed_bytes, n_changed)``; the input
    bytes are returned unchanged (same object) when nothing moves.
    ``lu_raw`` (NEC1 rows of the same cells, same order) enables the verdict
    recompute; without it only the land-use mix is corrected.
    ``n_changed`` counts cells touched by either rule once; the per-rule split
    is available via :func:`fix_rows_detail`."""
    out, n_tree, n_green = fix_rows_detail(obs_raw, lu_raw)
    return out, n_tree + n_green


def fix_rows_detail(obs_raw: bytes, lu_raw: bytes | None = None) -> tuple[bytes, int, int]:
    """``(fixed_bytes, n_tree_fixed, n_green_fixed)`` — a cell counts under the
    tree rule when it moved there; the green rule only sees rows the tree rule
    left alone (after it moved alpen/bau/gruen → wald nothing is left to green)."""
    n = len(obs_raw) // OBS_REC_BYTES
    if n == 0:
        return obs_raw, 0, 0
    obs = np.frombuffer(obs_raw, np.uint8, n * OBS_REC_BYTES).reshape(n, OBS_REC_BYTES)
    mt = tree_mask(obs)
    mg = mask(obs) & ~mt
    m = mt | mg
    if not m.any():
        return obs_raw, 0, 0
    out = obs.copy()
    if mt.any():
        sel = out[mt]
        _move(sel, TREE_MOVE_GROUPS, TREE_TARGET_GROUP)
        out[mt] = sel
    if mg.any():
        sel = out[mg]
        _move(sel, MOVE_GROUPS, TARGET_GROUP)
        out[mg] = sel
    if lu_raw is not None and len(lu_raw) >= n * LU_REC_BYTES:
        lu = np.frombuffer(lu_raw, np.uint8, n * LU_REC_BYTES).reshape(n, LU_REC_BYTES)
        sel = out[m]
        keep_unknown = sel[:, 29] == _V_UNKNOWN
        nv = _verdict(sel, lu[m])
        sel[:, 29] = np.where(keep_unknown, _V_UNKNOWN, nv)
        out[m] = sel
    return out.tobytes() + obs_raw[n * OBS_REC_BYTES:], int(mt.sum()), int(mg.sum())


def header_note(n_changed: int, cells_n: int, n_tree: int | None = None) -> dict:
    d = {"green_fix": FIX_ID, "green_fix_cells": int(n_changed), "green_fix_cells_n": int(cells_n),
         "green_fix_rule": (f"cells with fused S2 NDVI >= {NDVI_MIN} (phenology != road_or_bare): "
                            f"obs_lu {'+'.join(MOVE_GROUPS)} -> {TARGET_GROUP}; verdict recomputed"),
         "tree_fix_rule": (f"applied first: cells with LiDAR crown_frac >= {TREE_CROWN_MIN:g} and nDSM h_p90 >= {TREE_HP90_MIN:g} m "
                           f"(phenology != road_or_bare): obs_lu {'+'.join(TREE_MOVE_GROUPS)} -> {TREE_TARGET_GROUP}; verdict recomputed")}
    if n_tree is not None:
        d["tree_fix_cells"] = int(n_tree)
    return d


def stats(obs_raw: bytes) -> dict:
    """Audit counters for one product: cells touched, ha-equivalent mass moved."""
    n = len(obs_raw) // OBS_REC_BYTES
    if n == 0:
        return {"cells_n": 0, "touched": 0, "bau_cells": 0, "bau_green_cells": 0}
    obs = np.frombuffer(obs_raw, np.uint8, n * OBS_REC_BYTES).reshape(n, OBS_REC_BYTES)
    mt = tree_mask(obs)
    m = mask(obs) | mt
    bau = obs[:, 8 + _GI["bau"]] > 0
    ndvi = np.nan_to_num(_ndvi_from_u8(obs[:, 25]), nan=-1.0)
    return {"cells_n": int(n), "touched": int(m.sum()), "tree_touched": int(mt.sum()),
            "wald_cells": int((obs[:, 8 + _GI["wald"]] > 0).sum()), "bau_cells": int(bau.sum()),
            "bau_green_cells": int((bau & (ndvi >= NDVI_MIN)).sum()),
            "alpen_green_cells": int(((obs[:, 8 + _GI["alpen"]] > 0) & (ndvi >= NDVI_MIN)).sum())}


if __name__ == "__main__":
    import json, sys
    from pathlib import Path
    import ne_h3
    for p in sys.argv[1:]:
        hdr = ne_h3.read_h3b_header(p)
        obs = bytearray(); lu = bytearray()
        for ch in hdr["chunks"]:
            part = ne_h3.read_h3b_chunk(p, hdr, ch)
            obs += part["obs"]; lu += part["lu"]
        st = stats(bytes(obs))
        fixed, n = fix_rows(bytes(obs), bytes(lu))
        o0 = np.frombuffer(bytes(obs), np.uint8).reshape(-1, OBS_REC_BYTES)
        o1 = np.frombuffer(fixed, np.uint8).reshape(-1, OBS_REC_BYTES)
        st["lu_mass_before"] = {g: int(o0[:, 8 + i].sum()) for i, g in enumerate(_G)}
        st["lu_mass_after"] = {g: int(o1[:, 8 + i].sum()) for i, g in enumerate(_G)}
        st["verdict_before"] = np.bincount(o0[:, 29], minlength=7).tolist()
        st["verdict_after"] = np.bincount(o1[:, 29], minlength=7).tolist()
        print(Path(p).name, json.dumps(st))
