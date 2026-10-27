# Sliver sweep — closing the umfeld bbox-revision-2 strips

**Module** `sliver_sweep.py` · **state** `data/austria_processor/sliver_sweep.json`
(primary) · **sweep products** `data/ne/sweeps/<parent>-sw<k>.h3b` ·
**frozen bboxes** `kg_bbox_r1.json` (repo) · **dashboard** `/process.txt`
`sweep:` block, `?q=sweep` for events · **API** `/api/v1/director/sweep`.

## Why

Until 2026-10-27 umfeld's `/api/v1/kgs` bbox was parcel-derived and
narrower than the BEV VGD KG polygon envelope. Every product was
rasterised on that bbox, so the NEC1 cells (umfeld's lu / bevdirect
declared layer, which DO cover the whole polygon) in the strip between
the two envelopes were never observed — all-nodata NEO1 rows (63308
Gallmannsegg: 4 653 "unseen" cells on its W/E edges). umfeld now serves
`bbox_new = bbox_old ∪ VGD envelope + 0.0001°` (`bbox_revision: 2`, ETag
`…-b2`, universe hash/count unchanged — nothing shrank) and the repair
list `GET /api/v1/kgs/bbox_changes?min_delta=<deg>` (public tier, ETag):
`changed[] = {kg_code, bbox_old, bbox_new, max_delta_deg, slivers[4]}`.
All 7 850 KGs moved ≥ 0.0002°.

## Two decisions

1. **The fleet's processing bbox stays pinned to revision 1.**
   `austria_processor._normalise_umfeld_kg` replaces umfeld's `bbox` with
   the frozen `kg_bbox_r1.json` entry (full float precision — the coverage
   oracle `app._kg_coverage_complete` compares at 1e-9) and keeps the new
   envelope as `bbox_r2`. Reason: split layouts (`kg_splitter`), tile
   grids, `_json` bboxes and the search-index `kg` row all key on the
   bbox; letting it grow silently would make `_rects_cover_parent` call
   thousands of complete split parents "holes" → mass re-queue of whole
   KGs. `bbox_r2` only feeds the **NE declared-layer viewport**
   (`_parent_bbox_wgs`, bevdirect NEC1 build / vct pilot), so fresh KGs get
   a polygon-complete NEC1 from now on; `_kg_bbox_lookup` (processing
   fallback) stays on `bbox`.
2. **Slivers are swept, never whole KGs re-run.** Only the cells that are
   *actually unseen and outside bbox_old* are rasterised, in small padded
   bbox units, by a handful of cache-only workers; the director merges the
   result into the parent corpus file and umfeld gets a re-POST.

## Pipeline

```
umfeld bbox_changes ──refresh──▶ kgs[code] = wait
   (6 h, ETag)                       │ scan (ne_ingest tick, ≤400/tick, 40 s):
                                     │   parent complete (coverage oracle) AND data/ne/<code>.h3b on disk
                                     │   → unseen rows ∩ centroid outside bbox_old
                                     ├─ none ─────────────▶ clean          (~80 % of KGs)
                                     └─ cells → units ───▶ pending  (per side W/E/S/N, clusters split at
                                                                     1.5 km gaps, pad 130 m, ≤8 units/parent)
director cache-only orchestrator ── sliver_sweep.assign(all_workers) ──▶ dispatched
   sweep_peers (4) sticky workers × sweep_per_peer (3) parents, biggest first
   PUT <peer>/api/v1/processing/sweep_spec {specs:{parent: {bbox_new, attempt, units[]}}}
   parents prepended to the peer's whitelist slice (PUT /processing/queue)
peer processor ── sliver_sweep.peer_units → kg dicts {kg_code '63308-sw1', _parent_kg_code,
   bbox=unit, _parent_bbox=bbox_new, _sweep=True}  (head of pending; mid-run injection on new/higher attempt)
   process_one_kg: block mode (fixed bbox) → tiles → NE declared on bbox_new → full light-GPKG step
   (NE h3b + tree_apices/crowns, rasters, vectors, terrain_coarse) → json_v2 → v2_verify →
   Zenodo: <unit>_light_gpkg_v2 + <unit>_ne_h3b (archive) + <unit>_json_v2 (NO _json / _full_gpkg) → _SweepFinished
   main(): sliver_sweep.peer_report → POST primary /api/v1/director/sweep/result?unit&parent&status&cells&products (body = h3b)
   status ok | nodata (out_of_coverage / 'No tiles cover' / 0 cells) | failed (error text)
   products ok | "upload: …" (unit re-dispatched while attempts remain, h3b kept) | "verify: …" (accepted, logged)
primary accept_result ── verify_h3b → data/ne/sweeps/<unit>.h3b; every unit in → parent received
integrate (ne_ingest tick, ≤6/tick) ── ne_ingest.merge_blocks(parent, [parent.h3b] + sweeps) → parent.h3b
   (per cell the best-observed source wins; interior cells untouched), index rows digest updated
   → ne_ingest.confirm_umfeld sees digest ≠ umfeld's → re-POST /contrib …/obs (bev: NEC1+NEO1) → done
```

**Guard — KGs that ran on the new envelope are never swept**
(`_products_cover_bbox_new`, in `scan` before the lu fetch): if the union of
the parent's own `_json_v2` product bboxes in `kg_v2_store` (blocks, sweep
codes excluded) already reaches `bbox_new` on every side (5e-5°), the parent
goes `clean` with `clean_reason="products on bbox_new"`.  Audit:
`python3 -c` over `kg_v2_store.get_bbox` vs `kg_bbox_r1.json` (2026-10-27:
0 of 250 fresh docs beyond r1 — the pin holds).

States per parent: `wait → clean | pending → dispatched → received → done`,
`failed` after `MAX_ATTEMPTS` (3) parent attempts or 3 integrate failures.
A dispatch with no result for 8 h goes back to `pending` (+1 attempt).
Units: `pending → dispatched → ok | nodata | failed`.
`done` parents whose file digest drifts later (block re-merge after a
re-upload) are re-integrated automatically — the sweep h3bs are kept.

**Outside Austria / no LiDAR**: a unit whose tiles are all outside BEV
coverage resolves as `nodata` (never a failure, never re-dispatched); a
parent with only nodata units goes `done` with `nodata = unseen_out` and
no merge. `sweep:` reports `filled` vs `nodata` cells.

## Fast path (2026-10-27, unit cost ÷ ~5)

A unit ran the stock KG pipeline: the parent-wide bevdirect NEC1 build
(~590 s of an ~1 100 s unit on 57306; 1 400 s on Wildalpen-class
parents), 1.5 km tiles overshooting the padded unit bbox (57306-sw3:
2 × 2.25 km² segmented for 0.3 km² of cells), and the full light-GPKG
step (tree crowns, rasters, vectors, terrain_coarse, parcel enrichment)
before the h3b was thrown away.  Per-tile segmentation (~5 min / 2.25 Mpx,
92 % of tile time) is the only irreducible part, so `kg["_sweep"]` now
switches on:

* **cadastre**: `fetch_cadastre_data_bevdirect(…, cadastre_only=True)` →
  `vct_pilot.fetch_cadastre_only` = the helper's `--cadastre-only` mode on
  the *unit* bbox (viewport fetch, no NEC1 build, no /contrib report);
* **declared layer**: umfeld's lu via the director relay
  (`ne_h3.fetch_declared_director`, cached on the primary 30 d) — the cell
  set the scan diffed against; any failure falls back to the regular path;
* **tiles**: `_compute_tile_grid_clipped` — equal-split tiles that end at
  the unit bbox (the 130 m `PAD_M` and the 0.1 km overlap stay);
* **light step / products**: the full light step runs (see *Products*);
  it is seconds-to-a-minute on a sliver — the parent-wide NEC1 build and
  the overshooting tiles were the waste, not the light extras.

Expected unit wall time: ~1–2 min + ~5 min per ≤ 1.5 km tile of the unit
(was ~10 min + 5 min per overshooting tile) — always well under a fresh
KG (no full GPKG, no BEV re-reads, no Copernicus beyond the unit's tiles).
The `app.py` interactive `/segment` path was considered and rejected: it
runs the same `object_segmentation` core, so it cannot beat this.

## Products (2026-10-27): the strip must not be a hole in the tree services

Every bbox-keyed product service — `/api/v3/trees` (tree_apices +
tree_crowns from the light GPKG), `/api/v1/heightfield` + `/landscape`
(grid25 from `_json_v2`), the public `/segmentation` product mode
(`product_segments`), hillshade tiles — reads the parent's **products**,
all rasterised on `bbox_r1`.  The NE cells alone would have left the
revision-2 strip as a hole there.  So a unit uploads **three add-on
products of the parent**, under its own code (order light_v2 → ne_h3b →
json_v2; `products=ok` requires all three):

| key | content | notes |
|---|---|---|
| `<parent>-sw<k>_light_gpkg_v2` | light_v2 GPKG of the unit bbox (segments, segment_points, rasters, tree_apices + R-tree, tree_crowns, terrain_coarse, new_buildings, infrastructure) | `v2_verify.verify_before_upload` gate, status `cf` |
| `<parent>-sw<k>_ne_h3b` | the unit's NE h3b — **durability archive only** (2026-10-27 pm). The authoritative copy is still POSTed peer→director (`peer_report` → `accept_result` → `data/ne/sweeps/<unit>.h3b`) and merged by `integrate`; the Zenodo `<parent>_ne_h3b` stays pre-sweep | `ne_ingest.pending/ingest_one` skip it, `cells_api.index_file` refuses it, `ne_rollout` counts it as `h3b_sweep` (`+sweep=` on the `ne:` line); only reader: `sweep_paths` restore (below) |
| `<parent>-sw<k>_json_v2` | kgjson/2 of the unit incl. grid25 | ingested by `v2_ingest` into `kg_v2_store` under `parent=<parent>` |

Never uploaded: `_json` (completion marker), `_full_gpkg`.  The h3b upload
does not touch primary bandwidth (peer → Zenodo; `upload_stream` deletes
only gpkg files, so the h3b survives for `peer_report`).

**Archive restore** (`sliver_sweep.sweep_paths`): if an `ok` unit's
`data/ne/sweeps/<unit>.h3b` is gone (disk loss) and the manifest has a
committed `<unit>_ne_h3b`, it is pulled back via `ne_ingest.pull` (peer
first, Zenodo fallback; ≤ `RESTORE_PER_CALL`=2 per call, `verify_h3b`
before use, counted on inbound channel `sweep`) so `integrate` can still
merge.  Never fires on the happy path.

**Visibility rule — `kg_splitter.is_sweep_code('63308-sw1')`.**  A sweep
code is part of the parent's product set for `kg_v2_store.codes_for_parent`
(trees v3 `resolve_coverage`, `landscape_query.grids_for_kg`,
`app._grid25_for_kg`, `product_segments`, `hillshade_tiles`) and
**invisible** wherever `<parent>-*` means "the blocks of the layout":

* `kg_docs.codes_for_parent` → the search-index parent row never sums a
  sliver (the 130 m pad would double-count);
* `v2_ingest.ingest_one` stores it and returns (no index row, no
  `product_repair` row, no quality-flag scan); `v2_ingest.status` folds
  skip it;
* `ne_ingest._live_blocks` (h3b block merge), `ne_ingest.pending` /
  `ingest_one` (the `-sw<k>_ne_h3b` archive is never pulled) and
  `cells_api.index_file` skip it;
* `/process.txt` `zen_stall:` skips it (light_v2 + json_v2 only is *by
  design*, never "stalled");
* the re-queue invalidation in `POST /processing/queue` keeps `c-swN`
  products when parent `c` is re-run (the strip is keyed on bbox_r2, not
  on the parent's run);
* the dup-KG reconciler still sees it as a block (one peer per parent).

Overlap with the parent (the pad) is handled by the consumers: trees v3
dedupes by `tree_id` (integer-metre anchored since 2.2),
`grid25_sample.merge` prefers interior cells, footprints are unioned.

**R-tree envelope.** `search_index.kg_rtree` (what `/kgs?bbox`, trees v3
`parents_for_bbox_wgs`, `heightfield`, cells use to find parents) is the
`bbox ∪ bbox_r2` envelope (`SearchIndex._rtree_envelope`; one-off
migration `apply_rtree_bbox_r2` keyed `index_meta[rtree_bbox_rev]=2`,
run on every `build()`), so a query inside the strip finds the parent.
The `kg` row bbox itself stays revision 1 — `app._kg_coverage_complete`
compares block rects against it.

**Failure policy** (`sliver_sweep.accept_result`, `products=`): the h3b
is stored first, always.  `upload: …` (Zenodo / lock) → the unit goes
`failed` and is re-dispatched while it has attempts left (`MAX_ATTEMPTS`);
on the last attempt it is accepted without products — NE cells never
wait on Zenodo.  `verify: …` is deterministic → accepted, products
dropped, logged.  `sweep:` line: `add-on products: ok= missing=
pre-products=` (pre-products = units swept before 2026-10-27 or reported
by a peer still on old code).  Backfill: `python3 sliver_sweep.py rerun
[codes]` / `POST /api/v1/director/sweep {"rerun": true | [codes]}` puts
such `ok` units back to pending (merge is idempotent; the spec `attempt`
is bumped by 10·rerun so a peer's mid-run gate sees it as new).

## Operating

```bash
curl -s 'https://example.invalid:8000/process.txt?roster=0&log=0' | grep -A4 '^sweep:'
curl -s 'https://example.invalid:8000/process.txt?q=sweep&log=100'       # dispatch / result events
curl -s 'http://localhost:8000/api/v1/director/sweep?kg=63308'                   # status + one parent's plan
python3 sliver_sweep.py status | plan <kg> | refresh [--force] | scan [N] | integrate [N] | reset <kg>… | rerun [<kg>…]
```

Knobs (`director_config.json`): `sliver_sweep` (bool, default on),
`sweep_peers` (4 — the **floor**), `sweep_per_peer` (3), `sweep_min_delta`
(0.0002°), `sweep_peers_auto` (bool, default on), `sweep_peers_max` (24).

**Auto-scale (2026-10-27)**: every sweep-capable cache-only worker the v2
upgrade does not need this tick becomes a sweeper —
`n = clamp(len(capable workers) − (new v2 units this tick + workers mid
v2 unit), sweep_peers, sweep_peers_max)`.  The upgrade is the cache-only
pool's first job (`v24_first`), so as its candidate list drains (dispatch
`3/3cand` on 2026-10-27) the surplus workers sweep instead of only chewing
fresh cache-ready KGs the frontiers also cover.  Sweeps sit at the head of
the slice; fresh work fills behind.  Newly added sweeper slots prefer
workers not on a v2 unit with the emptiest slice; previous sweepers stay
sticky.  Director event `sweep: auto-scale → N sweeper(s) (capable
workers=…, v2 demand=…)` (`?q=auto-scale`) on every change; the `sweep:`
`peers(N, floor 4, …)` line shows the live count.  `sweep_peers_auto: false`
pins the floor.  Only peers on ≥ `SWEEP_MIN_COMMIT` (`f778efe`: fast path +
add-on products + h3b archive) count as capable — older code produced slow
`pre-products` units.
`POST /api/v1/director/sweep {"refresh":true | "reset":[codes] | "scan":N}`.

Sweep workers are chosen from the cache-only worker set (sticky, sorted
id order); they ride the cache-only orchestrator, so with an empty
cache-only whitelist nothing is dispatched (raise `sweep_peers` /
accept the pause). Cache misses on a sliver (unit bbox reaching into an
uncached 0.1° cell) fail the unit; 3 attempts then `failed` — check
`?q=sweep` and `python3 sliver_sweep.py plan <kg>`.

## Invariants

* Sweep units never touch the retry queue, strikes, completed sets or
  `_json`; on Zenodo they own exactly `<unit>_light_gpkg_v2` +
  `<unit>_json_v2` (→ *Products*); `result` is dropped after `peer_report`.
* `GET /processing/queue` keeps a parent queued while a sweep spec for it
  exists on the peer (`_v2_keep`), even though it is complete.
* The dup-KG reconciler sees `63308-sw1` as a block of 63308 — only one
  peer holds a parent's sweep at a time (`assign`).
* Peer→primary result POST is in `peer_tokens.PEER_SCOPE`; inbound bytes
  are counted on `inbound_budget` channel `sweep` (h3b ≤ 64 MB).
* `SRTM_APP_IMPORT_ONLY=1` (set by the CLI) stops `app.py` from starting a
  director thread when imported for `_kg_coverage_complete`.
