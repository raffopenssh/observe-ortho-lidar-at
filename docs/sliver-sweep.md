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
   process_one_kg: block mode (fixed bbox) → tiles → NE declared on bbox_new → light-GPKG step builds the
   NE h3b → _SweepFinished (no full GPKG, no JSON, no verify, no Zenodo, no vct pilot)
   main(): sliver_sweep.peer_report → POST primary /api/v1/director/sweep/result?unit&parent&status&cells (body = h3b)
   status ok | nodata (out_of_coverage / 'No tiles cover' / 0 cells) | failed (error text)
primary accept_result ── verify_h3b → data/ne/sweeps/<unit>.h3b; every unit in → parent received
integrate (ne_ingest tick, ≤6/tick) ── ne_ingest.merge_blocks(parent, [parent.h3b] + sweeps) → parent.h3b
   (per cell the best-observed source wins; interior cells untouched), index rows digest updated
   → ne_ingest.confirm_umfeld sees digest ≠ umfeld's → re-POST /contrib …/obs (bev: NEC1+NEO1) → done
```

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

## Operating

```bash
curl -s 'https://example.invalid:8000/process.txt?roster=0&log=0' | grep -A4 '^sweep:'
curl -s 'https://example.invalid:8000/process.txt?q=sweep&log=100'       # dispatch / result events
curl -s 'http://localhost:8000/api/v1/director/sweep?kg=63308'                   # status + one parent's plan
python3 sliver_sweep.py status | plan <kg> | refresh [--force] | scan [N] | integrate [N] | reset <kg>…
```

Knobs (`director_config.json`): `sliver_sweep` (bool, default on),
`sweep_peers` (4), `sweep_per_peer` (3), `sweep_min_delta` (0.0002°).
`POST /api/v1/director/sweep {"refresh":true | "reset":[codes] | "scan":N}`.

Sweep workers are chosen from the cache-only worker set (sticky, sorted
id order); they ride the cache-only orchestrator, so with an empty
cache-only whitelist nothing is dispatched (raise `sweep_peers` /
accept the pause). Cache misses on a sliver (unit bbox reaching into an
uncached 0.1° cell) fail the unit; 3 attempts then `failed` — check
`?q=sweep` and `python3 sliver_sweep.py plan <kg>`.

## Invariants

* Sweep units never touch Zenodo, the retry queue, strikes, completed
  sets or `_json`; `result` is dropped after `peer_report`.
* `GET /processing/queue` keeps a parent queued while a sweep spec for it
  exists on the peer (`_v2_keep`), even though it is complete.
* The dup-KG reconciler sees `63308-sw1` as a block of 63308 — only one
  peer holds a parent's sweep at a time (`assign`).
* Peer→primary result POST is in `peer_tokens.PEER_SCOPE`; inbound bytes
  are counted on `inbound_budget` channel `sweep` (h3b ≤ 64 MB).
* `SRTM_APP_IMPORT_ONLY=1` (set by the CLI) stops `app.py` from starting a
  director thread when imported for `_kg_coverage_complete`.
