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

### Automation
`grid25-registration.timer` (weekly, Sun 02:10 UTC; unit files in the repo
root, installed in `/etc/systemd/system`) runs `grid25-registration.service`:
`grid25_registration.py --build` then `tools/hillshade_seam_check.py
--country` (~45 min, niced, in-process). The registration version only
changes when the offsets change, so a no-op rebuild does not invalidate
client tile caches. A failed unit = seams found (`systemctl status
grid25-registration`, list in `data/hillshade_seam_report.json`).

## Open: products that need a real regeneration (NOT done — decided 2026-11-21)
Do not re-investigate; this is the state:

* **166 "mixed" products (149 parents)** — list: `mixed` in
  `data/grid25_registration.json`. Some of their tiles are 5 m shifted,
  some not, so one offset cannot fix them; terrain inside is up to 5 m off.
  Tiles look fine (edges blended).
* **Products with a corrupt tile** — the country seam check finds these as
  real seams, e.g. 90102 grid #5 has a 400 m jump between two adjacent
  columns at E 4321437 N 2646373 (tile 15/17294/11534 col 190). First
  country run 2026-11-21: see `data/hillshade_seam_report.json`.
* The 416 offset-corrected products are fine in every reader here, but
  their **Zenodo `_full_gpkg` rasters stay 5 m shifted**.

* **Trees / structures / NE cells / segments of the same products —
  measured 2026-11-21**: the v2 upgrade re-ran the whole pipeline on the
  GPKG rasters (`v2_source.install`), so everything derived there carries
  the shift. Test: apices from `ne_trees.points_bbox` vs. a 500 m BEV 1 m
  nDSM window, mean nDSM under the apex for every shift −8…8 m → best
  shift (−5,+5) for 92124, (−5,+4) for 92105, (−5,+6) for 92109 (mean
  nDSM 8.2 vs 3.5 m at zero shift for 92124). So `true = stored + offset`,
  same table as grid25. Classes / heights are right (LiDAR and ortho moved
  together), only the position is off.
  Read-time fix NOT implemented yet. Constraints found:
  - tree identity is position-keyed everywhere: `tree_id` = stored E_N,
    `tree_user` (`tree_ver`, match ≤ 1.5 m), `ne_linear` runs, legacy flag
    points. Shift only at the I/O boundary (emitted lon/lat, bbox filter,
    incoming report points) and keep x / y / cell / tree_id raw, or every
    store has to be migrated.
  - safe scope = whole-parent, non-mixed codes (312 KGs). Block codes (30)
    are merged into the parent `.h3b` without block attribution; mixed
    (166) need a per-tile verdict.
  - NE cells cannot be fixed like `green_fix`: that rewrites values inside
    a cell, this moves content across cells (7 m vs ~16 m cell spacing).
    Needs the reprocess.

Paths considered:
1. *Re-upgrade from the v1 GPKG* (`v2_regen`-style): useless — the GPKG
   itself carries the shift.
2. *Local repair of grid25 from BEV 1 m DTM*: ~2.5 GB of range reads on the
   metered primary, and leaves the Zenodo products wrong. Rejected.
3. **Fleet reprocess (the way to do it)**: the pipeline fix is deployed
   (`_align_tdata_to_tile`), so a fresh run is correct. Use
   `POST /processing/queue {kgs:[parents], position:0, skip_processed:false,
   keep_products:true}` — `keep_products` keeps the current products served
   until replaced (without it a split parent loses every block's manifest
   entry, `docs/app.md → Re-queueing`). The run is in RUN-COMPLETE hold with
   ~7 live peers; queue in batches (≤ 20 parents), confirm peers are on a
   commit ≥ eead178 first, clear the tile checkpoints of the affected KGs
   (old pickles would be restored), then `grid25_registration.py --build`
   and the seam check once they are ingested.

## Contract (unchanged)
Grey PNG, flat = 180, lit > 180, shaded < 180; alpha 0|255 only.
Tileset version = `RENDER_VERSION.registration_version`; bump
`RENDER_VERSION` on any change to `shade()` output. Deploy `srv_public`
with `kill -HUP` of the gunicorn master (graceful, listener stays open).
