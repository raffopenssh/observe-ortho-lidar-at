# v2.4 — NE cells (H3) replace parcels as the sub-KG unit

> **Design note (2026-10-04)** — implemented in `ne_h3.py` / `cells_api.py`; see `docs/v2.4-rollout.md` and `docs/ne-public-api.md` for what actually shipped (the `kg_cells10` index idea was superseded by `cells.sqlite` + `data/ne/*.h3b`; `kg_parcels` is gone since 2026-10-13).

Status 2026-10-04: design agreed, not implemented. umfeld-at's `ne-cells-2`
reproduces bit-identically here (`lu` 1233f3e4f9d07c27, `bev` 594025647ec3557e,
package v2.0.0, shapely 2.1.2 / h3 4.5.0). Contract: `/k/api/v1/ne/draft/ne-cells.md`.

## Units
* KG = summary unit (unchanged JSON `top_10_*`, `terrain`, grid25 …).
* res-12 H3 (~307 m²) = per-cell unit: declared (NEC1, umfeld) + observed (NEO1, ours).
* res-10 H3 (~15 000 m²) = query/index unit (replaces `kg_parcels`), top-5 lists live here.
* Trees: ALL apices h ≥ 3 m kept (NET1), grouped under their res-12 cell.
* Buildings / man-made: ALL objects kept as points WITH height (NEB1), grouped under
  their res-12 cell — never top-N only. Heights are observed (nDSM), not cadastre.
* No parcel ids, no footprints, no cadastre geometry anywhere in products or DB.

## Records (ours; big-endian none, all little-endian, sorted by cell id)
NEO1 `obs-cells-1`, 28 B / res-12 cell, same cell set as NEC1 of the KG:
`cell_id u64, obs_lu u8[9] (9-group order), obs_bld_cover u8, h_mean u8, h_p90 u8,
h_max u8 (0.5 m), tree_n u8, tree_h_max u8 (0.5 m), crown_frac u8, ndvi_mean u8,
ndvi_amp u8, dh_mean i8 (0.25 m), flight_years u8 bitmask, verdict u8, obs_bld_n u8`
(29 B; `obs_bld_n` = NEB1 rows in this cell, sat 255; compare with NEC1 `bld_n`).
(elev/slope/aspect dropped — grid25 already carries them at 25 m.)
Type→group map (frozen with the algo id): geb={roof,greenhouse,solar_panel},
bau={construction,earthwork,excavation,fill,parking}, acker={crop},
gruen={grass,garden,orchard,vineyard,shrub,hedge}, wald={tree,tree_loss},
wasser={water,wetland}, verkehr={road,path,bridge,rail}, alpen={rock,bare_soil,glacier},
sonst=rest.

NET1 `obs-trees-1`, 4 B / tree, rows in NEO1 cell order (count = `tree_n`, saturating 255):
`dx:5 dy:5` (integer metres from the metre-snapped EPSG:3035 cell centre → `tree_id`
bit-exact), `h:8` (0.25 m, sat 63.75), `crown:6` (log2 steps, 1–4000 m²),
`species:4 vitality:3 flag:1`.

NEB1 `obs-bld-1`, 8 B / object, rows in NEO1 cell order (count = `obs_bld_n`); one row per
man-made object whose centroid falls in the cell (roof, greenhouse, solar_panel, mast,
wind_turbine, substation, bridge, wall, fence ≥ 1 m):
`dx:5 dy:5` (1 m from metre-snapped EPSG:3035 cell centre), `h_max:8` (0.25 m, sat 63.75),
`h_robust:8` (0.25 m — the 2.3 ranking height), `area:16` (m², sat), `type:4`, `conf:4`,
`dh:8` (i8, 0.25 m newest−oldest nDSM; new/demolished detection).
Heights of every building point therefore survive 1:1 into NE; the res-10 `top5_manmade`
is a convenience index only. Verdict `building_undeclared` = NEB1 count/cover vs NEC1
`bld_n`/`bld_cover` per cell.

## On-disk fast path (budget 10 GB for Austria)
`data/ne/<kg>.h3b` = header (JSON: digests of NEC1/NEO1/NET1, epochs, sources,
chunk directory) + body of independently zlib-deflated **res-10 chunks**
(NEC1 cells / NEO1 cells / NET1 trees / NEB1 buildings per chunk). Query: bbox → h3 res-10 cells →
SQLite PK → mmap + inflate one chunk (<1 ms). Estimates: NEC1 1.4 GB, NEO1 ~3 GB,
NET1 ~4.5 GB (1.7 G apices), NEB1 ~0.05 GB (~6 M objects), index 0.6 GB ≈ 9.6 GB. Same file is the Zenodo product
`<kg>_ne_v2.4.h3b` (Range-fetchable per chunk via the directory).

## DB (search_index)
Drop `kg_parcels`, `kg_trees`, `kg_buildings`. New `kg_cells10` (PK cell_id int64,
kg_code, frav blob, declared_dom u8, obs_dom u8, bld_cover_decl/obs, tree_n, tree_h_p50/p90/max,
canopy_frac, elev_min/mean/max, slope_p50/p90, aspect_dom, verdict counts u8[7],
top5_trees blob, top5_objs blob, top5_manmade blob, chunk_off). ~5.6 M rows.
Public API bbox/point/KG queries map 1:1; nothing parcel-keyed remains.

## Pipeline (austria_processor, after light GPKG)
1. GET `/k/api/v1/ne/<kg>?layers=lu` (peer token) → verify digest → NEC1 verbatim.
2. Build NEO1 + NET1 + NEB1 from seg_type/nDSM/apex inventory/harmonics/dh per res-12 cell.
3. Verdict per cell = declared vs observed (consistent / forest_loss / forest_gain /
   sealed_undeclared / building_undeclared / green_undeclared / unknown).
4. Write `.h3b`, upload to Zenodo, POST NEO1 to `/k/api/v1/ne/<kg>/obs`.
5. Remove cadastre fetches (`cadastre.py`, footprints, parcels) from the KG pipeline;
   `new_buildings` → cell-level `building_undeclared`.
