# Hillshade seams (fixed 2026-11-21, tileset `4.<registration>`)

## Symptom
Thin bright/dark line pairs in `/tiles/hillshade` at the same ground position
at every zoom, straight in EPSG:3035, irregular spacing (Frastanz report).

## Cause (measured, not the first guess)
Tiles are rendered on the fly from the per-product 25 m `terrain.grid25`
(`kg_v2_store`), mosaicked per tile. Two things combined:

1. **Old mosaic = hard hand-over**: first product with a clean sample won.
   Where two overlapping products disagree, the hand-over line is a step.
2. **Why they disagree — a 5 m registration bug in the pipeline.**
   `austria_processor._read_dtm_for_tile` re-read a tile with the default
   5 m pad and pasted the array at the tile's own offset → DTM/DSM/ortho in
   the v1 full GPKG sit (−5 m E, +5 m N) off for every tile that had no
   local raster sidecar (i.e. mostly before dd08d74, 2026-05-28). v2.x
   products *upgraded from* such a GPKG inherit it in grid25; fresh v2
   products never had it. Verified against the BEV 1 m DTM (rmse 0.03 m
   with the shift, 2.45 m without).

Also found: 0.0 m fill where the DTM had no data (234 grids, shaded as flat
ground + cliffs), nodata filled with the tile mean before the gradient
(fake cliffs at the data edge), bilinear sampling (faceted 25 m cells,
stair-steps along embankments).

## Fix
| What | Where |
|---|---|
| Tile read aligned to the tile grid (`pad=0` + `_align_tdata_to_tile`) — no new shifted rasters | `austria_processor.py`, `tests/test_tile_align.py` |
| Per-product origin correction for the existing corpus | `grid25_registration.py` → `data/grid25_registration.json` (in git), applied by `grid25_sample.register` for tiles, `/heightfield`, landscape queries |
| 0.0 m fill (+ 1-cell rim) → nodata | `grid25_sample._drop_zero_fill` |
| Mosaic first on the common 25 m lattice (feather-blended overlaps), ONE Catmull-Rom resampling to the tile pixels, then shade with a 1 px halo; stencil touching nodata → transparent | `grid25_sample.merge_mosaic`, `hillshade_tiles.shade` |
| Metadata: `/tiles/hillshade.json`, `X-Tileset-Version`, `native_maxzoom` 13 | `hillshade_tiles._tilejson` |

`grid25_registration.py --build` (5 min, local): all overlapping product
pairs are compared under "relative shift −S/0/+S"; fresh products are
anchors, upgraded ones are labelled by weighted neighbour votes (prior: v1
GPKG date < 2026-05-29). 2026-11-21: 416 of 3 449 upgraded products shifted;
166 "mixed" (some tiles shifted, some not — sidecar hit per tile). Mixed
ones cannot be fixed by one offset; the feathered mosaic hides their edges
and a regen (`v2_regen`) is the real cure. Re-run `--build` after a batch
of regens / ingests; a changed file bumps the tileset version by itself.

## Regression check
`hillshade_seams.find_seams`: in the zone where the mosaic hands over
between products, a shade step > 3 × the median of the 4 neighbouring
steps (floor 1.5 grey), connected over ≥ 40 px, is a seam.

```bash
python3 tests/test_hillshade_seams.py                       # synthetic: legacy fires, new = 0
python3 tools/hillshade_seam_check.py --tile 16/34514/22989 # real tile(s); --legacy = old mosaic
python3 tools/hillshade_seam_check.py --country             # Austria z15, ~3 h niced, resumable
python3 tools/hillshade_seam_check.py --status              # also in /tiles/hillshade.json → seam_check
```
The country run calls the renderer in-process (no HTTP, no cache, no render
gate) so the public service is unaffected. Exit 1 on any seam.

## Contract (unchanged)
Grey PNG, flat = 180, lit > 180, shaded < 180; alpha 0|255 only.
Tileset version = `RENDER_VERSION.registration_version`; bump
`RENDER_VERSION` on any change to `shade()` output. Deploy `srv_public`
with `kill -HUP` of the gunicorn master (graceful, listener stays open).
