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

## Totals (build 2026-11-22 17:13 UTC)

2 761 products in 2 511 KGs carry a row; 614 rows open in 355 products.

| class | detector | products / tiles | open | read-time action |
|---|---|---|---|---|
| `pos_shift` | `ne_pos_fix` | 447 / 454 tiles (KGs: shift 247, partial 51, mixed 49, unclear 88; tiles: moved 249, kept 185, unmeasured 205) | 382 | moved |
| `pos_sweep_unit` | ledger (from `ne_pos_fix` + store) | 61 | 61 | moved with the KG — suspected wrong |
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
  running). No pre-upload check compares a product with its neighbours'
  registration — a `v2_verify` check against the overlap with already
  uploaded neighbours would catch any future whole-product offset.

### 2. `pos_sweep_unit` — fresh strip inside a moved KG (found by the ledger)

* **Symptom (suspected)**: cells of a sweep unit 5 m off the *other* way.
* **Root cause**: sweep units are fresh products on the true grid, merged into
  `data/ne/<parent>.h3b` without attribution; `ne_pos_fix.fix_words` moves
  every word of a whole-`shift` KG. `ne_pos_fix.kg_tiles` skips sweep codes.
* **Evidence**: 61 sweep units in 56 `shift` KGs.
* **Guard**: missing. Needed: `ne_pos_fix` treats a `shift` KG with sweep
  units as `partial` with the unit envelopes as `kept` tiles. Not done here
  (changes served positions) — **decision for the owner**.

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
  **Missing**: (a) clip tile rasters to the national outline + buffer at read
  time (`austria_mask`, exists since c1a818e but only at read time); (b) store
  the BEV source identity (file URL + ETag / Last-Modified) per tile in
  `acquisition.tiles[]`, so "upstream changed" is a metadata comparison and
  not a 13 GB re-read. Both change product content — not done here.

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
  probe (26fa14e, 2026-09-23). Open: the 5 stuck ones.

### 7. `linear_noise` — chains of small "trees"

* **Symptom**: straight chains of apices 2.5–7.5 m apart under overhead lines.
* **Root cause**: thin elevated linear objects leave a ridge in the nDSM; the
  apex finder has no shape test.
* **Evidence**: 1 756 KGs with runs (255 949 apices hidden).
* **Detected by**: `ne_linear` (4c0e800, 2026-11-11), read time only.
* **Guard**: missing at processing time. The detector needs only the apex
  columns, so the next run can call `ne_linear` on the tile's apices inside
  `v21_products` and write a `noise` flag. Not done here (changes products).

### 8. `apex_*` — apices their file cannot serve

Off-file / lost-cell apices (`rebin_added`): mostly a consequence of class 1
(equal-height twins 4–6 m apart across two files). The store is mid-rescan;
re-read this section's numbers from `--report` after the resolve. Guard: the
class-1 guard; the cell rule `rb1` is read-time by design.

## What the next run should do differently (short list)

1. Place every raster by transform (`_align_tdata_to_tile`) — done.
2. Ship raster sidecars with the checkpoint tar, or accept the re-read but log
   the tiles (`reread_dtm_tiles=`) — logging done, shipping open.
3. Record per-tile provenance in the product (`restored`, BEV source identity)
   — `restored` done, source identity open.
4. Clip to the national outline at read time — open.
5. Attribute sweep-unit cells in the merged `.h3b` — open.
6. Run the detectors' rules before upload as warnings
   (`check_grid25_tile_edges` done; neighbour registration, linear noise open).
