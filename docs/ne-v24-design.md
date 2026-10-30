# v2.4 — NE cells (H3) as the sub-KG unit

Design note for the cell records that `ne_h3.py` builds and
`cells_api.py` serves (`docs/ne-public-api.md`). The sub-KG unit of every
product is an H3 cell; nothing in a product is keyed by any property or
ownership unit, and no footprint or outline geometry is stored anywhere.

## Units

* KG = summary unit (JSON `top_10_*`, `terrain`, `landcover.grid25`, …).
* res-12 H3 (≈ 307 m²) = per-cell unit: reference land-use (NEC1, when a
  reference layer is available) + observed (NEO1, ours).
* res-10 H3 (≈ 15 000 m²) = chunk / index unit.
* Trees: **all** apices h ≥ 3 m are kept (NET1), grouped under their
  res-12 cell.
* Man-made objects: **all** kept as points with observed (nDSM) height
  (NEB1), grouped under their res-12 cell — never top-N only.

## Records (little-endian, sorted by cell id)

**NEO1** `obs-cells-1`, 41 B per res-12 cell (`ne_h3.OBS_REC` /
`OBS_FIELDS`, units in `OBS_UNITS`): `cell_id u64`, `obs_lu u8[9]` (group
mix, 9-group order), `obs_bld_cover u8`, `h_mean / h_p90 / h_max u8`
(0.5 m), `tree_n u8`, `tree_h_max u8`, `crown_frac u8`, `ndvi_mean u8`,
`ndvi_amp u8`, `dh_mean i8` (0.25 m), `flight_years u8` bitmask,
`verdict u8`, `obs_bld_n u8`, plus the terrain block (`elev`, `slope`
mean / p90, `aspect`, `roughness`, `TRI`, `TPI`, `dtm_change`,
`hansen_loss_year`, `phenology`).

Type → group map (frozen with the algorithm id): geb = {roof, greenhouse,
solar_panel}, bau = {construction, earthwork, excavation, fill, parking},
acker = {crop}, gruen = {grass, garden, orchard, vineyard, shrub, hedge},
wald = {tree, tree_loss}, wasser = {water, wetland}, verkehr = {road, path,
bridge, rail}, alpen = {rock, bare_soil, glacier}, sonst = rest.

**NET1** `obs-trees-1`, 4 B per tree, rows in NEO1 cell order (count =
`tree_n`, saturating at 255): `dx:5 dy:5` (integer metres from the
metre-snapped EPSG:3035 cell centre → `tree_id` bit-exact with the light
GPKG `tree_apices`), `h:8` (0.25 m, sat 63.75), `crown:6` (log2 steps,
1–4000 m²), `species:4 vitality:3 flag:1`.

**NEB1** `obs-bld-1`, 8 B per object, rows in NEO1 cell order (count =
`obs_bld_n`); one row per man-made object whose centroid falls in the
cell (roof, greenhouse, solar_panel, mast, wind_turbine, substation,
bridge, wall, fence ≥ 1 m): `dx:5 dy:5`, `h_max:8`, `h_robust:8` (the 2.3
ranking height), `area:16` (m²), `type:4`, `conf:4`, `dh:8` (i8, 0.25 m,
newest − oldest nDSM; new / demolished detection).

**Verdict** per cell (`ne_h3.VERDICTS`) = reference vs observed:
`consistent / forest_loss / forest_gain / sealed_new / structure_new /
green_new / unknown`. Without a reference layer every cell is `unknown`.

## On-disk container (`.h3b`)

`data/ne/<kg>.h3b` = JSON header (digests of NEC1/NEO1/NET1/NEB1, epochs,
sources, `summary`, chunk directory) + body of independently
zlib-deflated, byte-shuffled **res-10 chunks** (NEC1 cells | NEO1 cells |
NET1 trees | NEB1 objects per chunk). Query: bbox → res-10 cells → SQLite
PK → seek + inflate one chunk (< 1 ms). Budget for all of Austria ≈ 10 GB
(NEO1 ~3 GB, NET1 ~4.5 GB for ~1.7 G apices, NEB1 ~0.05 GB, index 0.25 GB).
The same file is the Zenodo product `<code>_ne.h3b` (manifest key
`_ne_h3b`). Blocks of a split KG carry the parent's full cell set with
unseen cells as nodata (`partial_cells: true`); `ne_ingest` merges them.

## Pipeline (`austria_processor`, inside `build_light_gpkg_tiled`)

1. At KG start, fetch the reference layer NEC1 if a source is configured
   (`ne_h3.fetch_declared`; optional upstream service, or built locally
   from public BEV vector tiles when `SRTM_BEV_VECTOR_CONTEXT=1` —
   `docs/vct-pilot.md`). Absent → product labelled `v2.4-partial` with
   defect `ne_cells` (`docs/product-repair.md`) and verdicts `unknown`.
2. Build NEO1 + NET1 + NEB1 from the stitched 1 m `seg_type` / nDSM, the
   apex inventory, NDVI harmonics, dh and the 5 m coarse terrain per
   res-12 cell (`ne_h3.build_observed`, bbox-clipped cells).
3. Verdict per cell.
4. Write `.h3b`, verify (`v2_verify`: `ne_h3b_valid`, `ne_observed_digest`,
   `ne_cells_observed`, `ne_algo`), upload after the light GPKG.
