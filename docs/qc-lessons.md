# QC ledger and lessons for the next all-Austria run

`qc_ledger.py` joins every read-time fault detector into ONE store
(`data/qc_ledger.sqlite`, summary `data/qc_ledger.json`). The detectors stay
the source of truth; the ledger is rebuilt from them (local files only, ~40 s
niced, 240 MB RSS, skipped when no input changed) as the last step of
`ne-linear.service` (hourly, `batch.slice`). Nothing is written into
`zenodo_manifest.json`: the manifest link is the derived join `for_key()`.

```bash
python3 qc_ledger.py --status               # the qc: line of /process.txt
python3 qc_ledger.py --worst 20             # /process.txt?qc=20
python3 qc_ledger.py --key 22138_full_gpkg  # /process.txt?qc=22138_full_gpkg  (manifest key or product code)
python3 qc_ledger.py --report               # overlap + timing tables (the numbers below)
python3 qc_ledger.py --tiles 30             # tile envelopes carrying more than one fault class
python3 qc_ledger.py --build --force
python3 tests/test_qc_ledger.py
```

## What a row is

One row per **(product code, tile | whole product, fault class)**: detector,
verdict, status (`open` / `handled` / `clear` = measured, no fault), read-time
action (`moved` / `patched` / `registered` / `blended` / `hidden` / `added` /
`requeued` / `none`), evidence numbers, EPSG:3035 tile envelope.

Products, as the processor makes them:

| kind | code | tiling |
|---|---|---|
| parent | `22138` | `_compute_tile_grid`: 1.5 km tiles, 100 m overlap, last column / row overshoots the bbox; stitched with a 100 px feather |
| block | `49006-north-1` | `kg_splitter.maybe_split_kg` (> 22 tiles or ≥ 2 strikes): own bbox, **own tile grid** (tile 1 of a block ≠ tile 1 of the parent), own manifest keys. A split parent has no product of its own |
| sweep unit | `63308-sw1` | `sliver_sweep`: fresh add-on product of the parent (no `_json` / `_full_gpkg`), cells merged into `data/ne/<parent>.h3b` |

KG-keyed detectors (`ne_pos_fix`, `rebin_added`, `ne_linear`) see the parent;
product-keyed ones (`grid25_*`, `product_repair`) the product. Tile rows are
therefore keyed by the tile's **envelope** (same rounding as
`ne_pos_fix.kg_tiles`) and a KG-keyed tile verdict is attached to the block
that owns the envelope. `for_key('<block>_json_v2')` returns the block's rows
plus the whole-KG rows of its parent (`scope: parent`).

WHEN per product (`product.timing`): manifest upload time / version per key,
uploading peer (`product_origin`, else the log), `upgraded_from`, the v2
upgrade's per-tile source (`v2_source`: taken from the v1 GPKG or re-read from
BEV), sessions / peers / checkpoint tars from the harvested per-KG log, and
the `sidecar gpkg_full: dtm=a/b` line of the v1 stitch from the per-day log
archive (`data/log_archive`, cached per day file in
`data/qc_ledger_archive.json`).

## Totals (build 2026-11-22 17:57 UTC)

2 777 products in 2 511 KGs carry a row; 832 rows open in 355 products
(614 `pos_shift` — the per-tile pass adds unmeasured tiles as it goes — 166
`grid25_mixed`, 47 `regenerated` = on the reprocess list, 5 `layer_gap`).

| class | detector | products / tiles | open | read-time action |
|---|---|---|---|---|
| `pos_shift` | `ne_pos_fix` | 474 / 794 tiles (KGs: shift 247, partial 67, mixed 33, unclear 88; tiles: moved 354, kept 428, unmeasured 440) | 614 | moved |
| `pos_sweep_unit` | ledger (from `ne_pos_fix` + store) | 61 | 0 | **kept**: rows the unit supplied are exempt from the move (125 units / 155 265 cells in 84 moved KGs, 2026-11-22) |
| `regenerated` | `qc_generation` | 47 | 47 | on the reprocess list (`grid25_stale`), fresh v2.4 generation in place |
| `grid25_shift` | `grid25_registration` | 416 | 0 | registered (−5, +5 m) |
| `grid25_mixed` | `grid25_registration` | 166 | 166 | blended; bad tiles patched |
| `grid25_stale` | `grid25_patch` | 49 / 274 tiles (3 864 tiles checked ok) | 0 | patched from BEV |
| `grid25_edge` | `grid25_patch` | 553 / 668 tiles | 0 | checked (46 on a stale tile) |
| `label_mix` | `label_mix` | 1 (+1 false positive) | 0 | requeued, reprocessed |
| `layer_gap` | `product_repair` | 5 | 5 (stuck) | none (`-partial` label) |
| `linear_noise` | `ne_linear` | 1 756 KGs | 0 | hidden per apex |
| `apex_unserved` / `apex_frame_pair` / `apex_invariant` | `rebin_added` | 2 / 0 / 0 | — | **input incomplete**: store mid-rescan (`resolved` = 0) |

Inputs still moving at that build: `ne_pos_fix` per-tile pass (134 KGs to do),
`rebin_added` resolve. The `qc:` line says so (`inputs incomplete: …`).

## Fault classes

### 1. `pos_shift` / `grid25_shift` / `grid25_mixed` — rasters 5 m off

* **Symptom**: trees, structures and the 25 m terrain of a product sit
  (+5, −5) m from truth; hillshade seams between products; tree twins across
  KG files.
* **Root cause**: `austria_processor._read_dtm_for_tile` re-read a tile with
  the default 5 m pad and the stitch pasted the array at the tile's own
  offset. It ran for **every tile without a local raster sidecar**.
* **Ledger evidence** (whole-KG verdict × era of the v1 rasters × sidecar
  line of the v1 stitch):

  | verdict | pre-sidecar (v1 before 2026-05-29) | post, no sidecar line | post, sidecar part | post, sidecar all |
  |---|---:|---:|---:|---:|
  | shift | 216 | 15 | 15 | 1 |
  | partial | 25 | 16 | 8 | 2 |
  | mixed | 12 | 19 | 14 | 4 |
  | unclear | 79 | 7 | 1 | 1 |
  | zero | 1 | 28 | 17 | 2 |

  216 of 247 whole-shift KGs were stitched before sidecars existed (dd08d74,
  2026-05-28) — every tile was a second read. After it, shifted products are
  those whose stitch had sidecar misses: the tiles came as checkpoint pickles
  from another peer (pickles travel through the Zenodo registry, raster
  sidecars do not), so the stitching peer re-read them. Example 22138: 26
  sessions on 20 peers, foreign tile-tar, final stitch `dtm=48/58`.
  `grid25_shift` ∩ `pos_shift` = 404 of 408 parents: one fault, two detectors.
* **Second generation**: the v2 upgrade (`v2_source._gpkg_read_dtm_dsm`) takes
  each tile from the v1 GPKG (inherits the shift) or re-reads it from BEV
  (true frame). Tile join: **kept** tiles were BEV re-reads in 36 of 185
  cases, **moved** tiles in 6 of 249. That is where partly shifted products
  come from.
* **Detected by**: `grid25_registration` (cross-correlation against
  neighbours), `ne_pos_fix` (windows / per-tile BEV check), `rebin_added`
  (`dup_shift` file pairs).
* **Guard**: exists — `pad=0` + `_align_tdata_to_tile` (eead178, 2026-11-21,
  `tests/test_tile_align.py`): a raster is placed by its transform, never by
  position in the array. **Added here**: the sidecar summary line now ends
  with `reread_dtm_tiles=…`, and a restored tile carries `restored: true` in
  `data_quality.tiles[]`, so the next ledger knows the tiles without sampling
  the log.
* **Open**: 205 tiles unmeasured, 49 mixed + 88 unclear KGs (per-tile pass
  running).
* **Guard added 2026-11-22** (`tests/test_run_guards.py`):
  - `v2_verify.check_neighbour_registration` → `neighbour_registration.py`:
    before upload the product's `terrain.grid25` is compared with up to 6
    already uploaded neighbours (`_json_v2` straight from Zenodo, ≤ 90 s) over
    a ±10 m offset search, whole product and per processing tile. Warning
    only, verdict in `result.v2_verify.neighbour_registration`
    (`aligned / shifted / tiles-shifted / no-signal / no-neighbours`,
    `mismatch` = best RMS > 3 m). Known neighbour offsets
    (`data/grid25_registration.json`) are taken out first. Off:
    `NEIGHBOUR_REG_CHECK=0`. By hand: `python3 neighbour_registration.py <code>`.
  - Raster sidecars travel in the checkpoint tar
    (`tile_checkpoint_registry`, lossless `F32Z1`, pickles first, rasters fill
    what is left of the 200 MB cap; a tile that does not fit ships its
    `meta.json` only): a restored tile no longer re-reads DTM/DSM from BEV.
    Off: `CHKPT_SHIP_RASTERS=0`.
  - `gpkg_streamed` had its own `_read_dtm_for_tile` with the old `pad=5`
    (the > 100 Mpx path) — now the shared reader.

### 2. `pos_sweep_unit` — fresh strip inside a moved KG (found by the ledger)

* **Symptom (suspected)**: cells of a sweep unit 5 m off the *other* way.
* **Root cause**: sweep units are fresh products on the true grid, merged into
  `data/ne/<parent>.h3b` without attribution; `ne_pos_fix.fix_words` moves
  every word of a whole-`shift` KG. `ne_pos_fix.kg_tiles` skips sweep codes.
* **Evidence**: 61 sweep units in 56 `shift` KGs.
* **Guard**: **added** (owner decision 2026-11-22: sweep units are exempt) —
  `ne_pos_fix.sweep_keep`: the rows whose merged NEO1 record is byte-identical
  to the unit's are kept. 12149: 1 902 apices in 472 kept rows, all unmoved,
  cell rule intact (`ne_rebin --check`). See *Generations / reprocess / keep cells*.
* **Guard at merge time, added 2026-11-22**: `ne_ingest.merge_blocks` writes
  the source product of every cell into the merged file — `NEA1` trailer after
  the last chunk (`<u2` per cell, zlib; `header.attribution.sources / cells`;
  `ne_h3.read_attribution`). Readers that follow the chunk directory do not
  see it; digests are unchanged. It passes through re-merges, and rows that an
  older, unattributed merge holds byte for byte are credited to the unit they
  came from. `sweep_keep` reads it instead of fetching units. Existing files
  get it at their next merge (no backfill run).

### 3. `grid25_stale` — terrain that BEV no longer serves

* **Symptom**: one processing tile holds terrain that does not match today's
  BEV DTM (0.5 m to 600 m off); a cliff line along an internal tile edge, an
  opaque block beyond the border.
* **Root cause**: BEV served elevation beyond the national border during the
  run and withdrew / replaced it later. Tiles read then are frozen in the
  product.
* **Ledger evidence** (distance of the tile envelope to the outline, stale /
  BEV-checked):

  | | crosses border | < 1.5 km | < 5 km | ≥ 5 km |
  |---|---:|---:|---:|---:|
  | stale / checked | 106 / 1 531 | 121 / 311 | 33 / 290 | 14 / 2 006 |
  | of these ≥ 8 m off | 53 | 24 | 20 | 0 |

  260 of 274 stale tiles lie within 5 km of the border, all 97 with ≥ 8 m
  error. The 14 inland ones are all < 1.5 m (12 products). v1 build month of
  the stale tiles: June 87, July 101, August 22, October 42. Caveat: the
  checked sample is biased towards the border (suspects = tiles with cells
  abroad, tiles on flagged edges, their neighbours).
* **Not the 5 m fault**: tile-envelope join — of 82 BEV-checked tiles that
  `ne_pos_fix` moves, 1 is stale; 217 of 274 stale tiles are in KGs without
  any position verdict. The tile with the tree shift is *not* the stale
  terrain tile; the two faults are independent.
* **Detected by**: `grid25_patch` (`--edge-scan`, BEV strips, flood fill).
* **Guard**: **added** — `v2_verify.check_grid25_tile_edges` (warning, never
  fatal): the detector's edge rule on the product document before upload.
  Real terrain fires it too (553 of 16 775 products, 19 with a stale tile), so
  it only puts a line next to the upload in the log.
  **Added 2026-11-22** (both change product content, from the next
  processed product on): (a) `raster_io.clip_national` — DTM / DSM / nDSM are
  NaN beyond outline + 100 m at read time (`OUTLINE_CLIP_M`, env `off`
  disables; also on GPKG-inherited rasters in `v2_source`);
  `data_quality.tiles[].outline_clip_px` counts the pixels. (b)
  `raster_io.source_identity` — one HEAD per BEV file (ETag, Last-Modified,
  size) stored as `data_quality.tiles[].bev_source`, in the sidecar
  `meta.json` and in the checkpoint tar. A stitch-time re-read compares it:
  `source_changed: true` on the tile, `source_changed_tiles=` on the sidecar
  summary line. "Upstream changed" is now a metadata comparison
  (`raster_io.sources_changed`).
  **Added 2026-11-23** — `v2_verify.border_loss_explained`: a fresh run of a
  border product *must* cover less LiDAR than its v1 (withdrawn BEV data +
  the clip), so `segmented_area_ge_v1` / `lidar_tiles_ge_v1` rejected exactly
  the reprocessed `grid25_stale` products (4 stuck at the primary ingest,
  `layer_gap` blocks struck out on peers). The two gates are now warnings
  when, on the same tile grid and without upstream-failed tiles, every tile
  that lost pixels crosses the national outline and still holds ≥ 90 % of its
  in-Austria pixels (outline-derived, independent of what the peer recorded);
  one inland tile with a loss keeps them fatal. Log text: `border loss only:
  N border tile(s) lost X km²`. Test `tests/test_run_guards.py`.

### 4. `grid25_edge` — step along an internal tile edge

668 edges in 553 products; 46 sit on a stale tile, the rest matched BEV
(real terrain: cliffs, quarries, terraces). Kept as rows because an edge on a
tile without a BEV verdict is the cheapest suspect list. Guard: same as 3.

### 5. `label_mix` — restored tile kept the label ids of the first run

* **Symptom**: flat pasture as `roof`, glued polygons, NE cells `geb` at
  `h_max = 0` (63330: 15.8 k of 403 k segments).
* **Root cause**: a restored checkpoint's objects got new ids, its label
  raster did not; the stitch resolves pixels through one global id map.
* **Evidence**: registry now holds 1 repaired product (23005-west) and 1
  false positive; earlier cases were reprocessed and pruned
  (`docs/label-mix.md`).
* **Guard**: exists — `_remap_restored_tile_ids`,
  `_assert_tile_labels_consistent` (`LabelMixError`),
  `v2_verify.segments_label_consistency`, fast-path decline (all 5a6817b,
  2026-10-19, `tests/test_label_mix.py`).

### 6. `layer_gap` — cache-fillable layer missing

* **Symptom**: product labelled `-partial` (harmonics / NDVI / SAR /
  WorldCover / Hansen / NE cells missing in some tiles).
* **Root cause**: openEO / tile-store gap at processing time.
* **Evidence**: 5 products left, all `stuck` (81201-* blocks and others).
* **Guard**: exists — `product_repair.defects`, `-partial` label, repair
  probe (26fa14e, 2026-09-23).
* **The 5 stuck ones (2026-11-22)**: not a cache gap. `v2_source` replaces the
  processor's `_fetch_copernicus_for_tile` with `_gpkg_fetch_copernicus`
  during a v2 upgrade; 394887e (11-10) added the keyword `tolerate_miss` to
  the caller only → `TypeError` on every tile → all four Copernicus layers
  lost → `-partial` again → 2 attempts → `stuck`, with every cell cached.
  Fixed (shim takes `**_kw`, `test_fetch_shim_signature`); the 5 codes were
  reset to `pending` after the rollout. Lesson: a function that is swapped at
  run time needs a signature test.

### 7. `linear_noise` — chains of small "trees"

* **Symptom**: straight chains of apices 2.5–7.5 m apart under overhead lines.
* **Root cause**: thin elevated linear objects leave a ridge in the nDSM; the
  apex finder has no shape test.
* **Evidence**: 1 756 KGs with runs (255 949 apices hidden).
* **Detected by**: `ne_linear` (4c0e800, 2026-11-11), read time only.
* **Guard added 2026-11-22**: `v21_products.flag_linear_noise` runs
  `ne_linear.detect` on the product's apex rows (same quantisation as the NE
  cells, so both passes mark the same apices) and writes
  `tree_apices.noise = 'linear'` + `tree_stats.linear_noise {n_apices, runs,
  len_m, detector}`. Nothing is dropped; the NET1 word has no free bit, so
  the `.h3b` is unchanged and the read-time pass stays the serving rule.

### 8. `apex_*` — apices their file cannot serve

Off-file / lost-cell apices (`rebin_added`): mostly a consequence of class 1
(equal-height twins 4–6 m apart across two files). The store is mid-rescan;
re-read this section's numbers from `--report` after the resolve. Guard: the
class-1 guard; the cell rule `rb1` is read-time by design.

## Run history and v2.4 data quality (read this before touching a read-time fix)

| when | what | effect on the data |
|---|---|---|
| 2026-04 … 05-28 | v1 run, **pre-sidecar era**: the stitch re-read every tile from BEV with a 5 m pad and pasted it at the tile's offset | whole products (+5, −5) m off: `pos_shift` / `grid25_shift` |
| 2026-05-28 (dd08d74) | raster sidecars next to the tile checkpoints; pickles travel between peers (Zenodo registry), sidecars do not | only tiles restored from a foreign checkpoint are re-read → **partly** shifted products |
| June … Oct | BEV served elevation beyond the border, later withdrew / replaced it | `grid25_stale` tiles (260 of 274 within 5 km of the border) |
| until 2026-10-19 (5a6817b) | restored checkpoint kept old label ids | `label_mix` (reprocessed) |
| 09-18 … Nov | v2.x upgrade **from the v1 GPKG** (`upgraded_from`): rasters inherited, some tiles re-read from BEV (true frame) | shift inherited tile by tile → `grid25_mixed`, `partial` KGs |
| 2026-10 | v2.4: cadastre-free, NE cells `.h3b`; 10-27 sliver sweep units (fresh, true grid) merged into the parent `.h3b` | `pos_sweep_unit` |
| 2026-11-21 (eead178) | `pad=0` + `_align_tdata_to_tile`: rasters placed by transform | every run since is on the true grid |
| 2026-11-22 04:41 | run complete (7 850 / 7 850), director on hold | — |

Nothing in a stored v2.4 product was rewritten for these faults. They are
corrected **at read time**, each fix keyed to the generation it was measured on:

| fix | does | store | retired by a fresh generation |
|---|---|---|---|
| `grid25_registration` | shifts a product's 25 m terrain (−5, +5) m / blends a mixed one | `data/grid25_registration.json` | `retire(code)` |
| `grid25_patch` | overlays stale tiles with the 25 m block mean of today's BEV DTM (read once, throttled), per-tile verdicts, edge findings | `data/grid25_patches/` | `retire(code)` |
| `ne_pos_fix` | moves tree / structure words per KG or per processing tile; `keep` cells of sweep units stay | `data/ne_pos_fix.json`, `data/ne_pos_keep/` | `regenerated(code)`: block tiles → kept, last block → `retired`; frame history stays for old client copies |
| `ne_rebin` (rb1) + `rebin_added` | apex listed in the cell of its served position; off-file / lost-cell apices added back or de-twinned | `data/rebin_added.sqlite` | follows the file tag automatically (rescan) |
| `ne_linear`, `tree_qa`, `ne_green_fix`, `austria_mask` | hide chains / implausible apices, class overrides, national outline | own stores | independent of the frame; re-evaluated per file digest |

## Generations / reprocess / keep cells (`qc_generation.py`)

* **Generation** = one `generated_at` of a code's `_json_v2`. `v2_ingest` calls
  `qc_generation.on_ingest` for every stored document: the row in
  `data/qc_generations.sqlite` documents the new quality from the document
  alone (tiles, restored tiles, internal edge cliffs, `-partial` label, model);
  a **fresh** generation (no `upgraded_from`) additionally snapshots the
  ledger rows held against the old one and retires the three raster-bound
  fixes above. QC **never enrols** anything; it documents.
* **Reprocess list** (`data/austria_processor/qc_reprocess.json`,
  `qc_reprocess:` line): operator only —
  `python3 qc_generation.py --enroll-fault grid25_stale --why '…'` /
  `--enroll CODE…` / `--cancel CODE`. The director's audit tick requeues
  2 per tick, ≤ 6 in flight, with `keep_products` + `fresh_run`: no manifest
  entry and no Zenodo file is deleted, the `<code>_fresh` marker only stops
  the processor from reusing the old full GPKG; files are overwritten in
  place and the old product is served until the new one is ingested.
  States: `enrolled → requeued → done` (`stuck` after 2 attempts,
  `waiting: label_mix|v2_regen|product_repair` when another list owns the code).
* **Hold caveat**: the audit tick does not run during the run-complete hold.
  `_run_complete_checks` counts the list as open work (`qc_reprocess`); on a
  director started before that commit kick once by hand:
  `python3 qc_generation.py --sweep` (posts the first two, the queue wakes the director).
* **Keep cells** (`ne_pos_fix.sweep_keep`, `data/ne_pos_keep/<kg>.npy`): the
  res-12 rows a sweep unit won in the merged `.h3b` of a moved KG are never
  moved. Row-aware readers (`fix_words` with cell context: `_load_chunk`,
  `ne_rebin.FileSource` = posted rows) decide by the **row**; point readers
  (`moved`, `moves_point`, `served_frame` for the twin rule) by the cell of
  the position. The file tag gets `~k<crc>`, so bins / blocks / rebin store /
  linear runs of the KG recompute. `ne_pos_fix.py --sweep-keep [--only kg,…]`
  is idempotent (seen units are skipped) — rerun it after new sweep units land
  in a moved KG (not on a timer).
* **Same version only**: this path regenerates v2.4 in place. A v2.5 kept
  next to v2.4 needs its own manifest keys / filenames; the hook
  (`on_ingest`) and the retire calls are version-agnostic and are what every
  product of such a roll-out walks through.
* Tests: `python3 tests/test_qc_generation.py`.

## What the next run should do differently (short list)

All items are in the code since 2026-11-22 (`tests/test_run_guards.py`).

1. Place every raster by transform (`_align_tdata_to_tile`) — done, now also
   in `gpkg_streamed`.
2. Ship raster sidecars with the checkpoint tar — done
   (`tile_checkpoint_registry`, `CHKPT_SHIP_RASTERS`).
3. Record per-tile provenance in the product — done (`restored`,
   `bev_source`, `stitch_reread`, `source_changed`, `outline_clip_px`).
4. Clip to the national outline at read time — done (`raster_io.clip_national`).
5. Attribute sweep-unit cells in the merged `.h3b` — done (`NEA1` trailer).
6. Run the detectors' rules before upload as warnings — done
   (`check_grid25_tile_edges`, `check_neighbour_registration`,
   `flag_linear_noise`).
7. Every function swapped at run time has a signature test
   (`test_fetch_shim_signature`) — the cause of the 5 stuck `layer_gap` products.

Not verified on the fleet yet: the first regenerated products after the
rollout should show `bev_source` in `data_quality.tiles[]`, a
`neighbour_registration` entry in the verify result and
`tree_stats.linear_noise`.
