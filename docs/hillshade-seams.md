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

## National mask (2026-11-21, tileset v5)

Products are rectangles and BEV served — in places still serves — terrain
beyond the border: 1 469 of 16 775 products have cells outside Austria
(1 124 farther than 75 m, 1 445 km² in total). Alpha therefore did not
follow the border wherever a product reached across.

* `austria_mask.py` = **the outline the NE server clips with**
  (`GET <NE server>/api/v1/ne/glyph/admin/outline?c=AT`, OSM relation 16239
  ~5 m, viewBox → lon/lat with `proj` of `/api/v1/ne/glyph`). Stored as
  `data/austria_outline.json` (gitignored — ODbL; `python3 austria_mask.py
  --fetch`, refreshed by the weekly unit). Do NOT union the Statistik
  Austria Land outlines instead (off by up to ~200 m).
* `grid25_sample.register → _clip_national`: cells > 100 m outside → nodata
  (the 100 m only support the cubic taps / gradient stencil) — applies to
  tiles, `/heightfield`, landscape queries.
* `hillshade_tiles.shade`: alpha cut exactly on the outline for tiles the
  border crosses. Contract: **alpha 255 = inside Austria and terrain
  available, else 0**. No mask file → no clip (never an outage).

## Stale tiles / grid25 patches (`grid25_patch.py`)

Finding (z12 2156/1436, Nofels 92116): a product is stitched from ~1.5 km
processing tiles; some tiles hold terrain that does not match today's BEV
DTM at all (92116 west tile column: 440–488 m stored vs 429 m on the Rhine
plain; tile 9: a smooth 6 m-low plateau) — tiles restored from rasters of
the time BEV served elevation abroad. Result: an opaque rectangle across
the border and a cliff line along the *internal tile edge of one product*.

Why the seam check missed it: it tests **product** boundaries for 1-px
discontinuities. A wrong tile inside one product is (a) not a product
boundary and (b) after the cubic resampling a smooth ramp over several
pixels at any zoom — no pixel pair is an outlier. Hence a second rule in
the DTM's own 25 m domain:

* `--edge-scan` (local, all products): 3-cell step across every internal
  tile edge vs the same step 3 and 6 cells further in on both sides
  (> 3× median, floor 1 m, run ≥ 16 cells) → `index.json → edges`.
* `--scan` (throttled on host RX, resumable): suspects = tiles with ≥ 40
  cells > 300 m abroad + tiles on flagged edges → two 25 m strips of BEV
  DTM per tile → median |stored − BEV| > 0.5 m (healthy: < 0.1 m) ⇒ tile
  rebuilt from BEV exactly as the pipeline does (25 m block mean, ≥ 20 %
  valid) → `data/grid25_patches/<code>.npz`, overlaid by
  `grid25_sample.register → _patch` for every grid25 reader. One tile-set
  version bump per scan that changed something. This also repairs the
  5 m-shifted tiles of *mixed* products and corrupt tiles (90102).
* `--tile CODE E N [--force]` for a spot reported by hand.
* TileJSON `tile_edge_check` = `{edges_open, products_patched, tiles_patched, …}`;
  remaining pixel-rule seams: `/tiles/hillshade/seams.json` (tile + row/col).

Tile-set version = `<render>.<registration>.<patch generation>-<outline sha>`.
Weekly unit order: outline fetch → registration → edge-scan → scan → seam check.
Tests: `python3 tests/test_grid25_patch.py`.

## Trees / structures of shifted KGs (`ne_pos_fix.py`)

The v2 upgrade ran the whole pipeline on the shifted GPKG rasters, so apices
and structures of those KGs are 5 m off as well (true = stored + (−5, +5)).
Read-time fix like `ne_green_fix`: `cells_api._load_chunk` moves the dx/dy
bits of every NET1/NEB1 word by the KG's offset; cache keys / ETags carry
`ne_pos_fix.tag(kg)` (the `neo1_digest` itself is NOT altered). Position-keyed
stores follow: `tree_ver` rows are migrated once, linear-noise runs recomputed
(`ne_linear._dig`), legacy QA points shifted.

Which KGs: measured, never taken from the registration table —
`python3 ne_pos_fix.py --verify` reads 4 × 250 m BEV nDSM windows per
candidate parent (one per processing tile for *mixed* parents): an apex of
height h sits on nDSM ≈ h in the right frame (median |nDSM − h| ≈ 0.5 m vs
≈ 4 m). Fixed only when ≥ 3 windows say shifted and none says zero;
`mixed` / `unclear` KGs are left alone. State `data/ne_pos_fix.json`.
Public record + overrule = one `auto-shift` flag per KG; stale client copies
(mail sync, inventory ids) are re-framed on write:
`docs/tree-user.md → Position frames`.
NOT fixable this way: NE cell statistics (NEO1), segments — reprocess.

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
  Read-time fix: implemented, section above. Constraints found:
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
