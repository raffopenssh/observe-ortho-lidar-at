# Sliver sweep — `sliver_sweep.py`

**State** `data/austria_processor/sliver_sweep.json` · **sweep products**
`data/ne/sweeps/<parent>-sw<k>.h3b` · **frozen processing bboxes**
`kg_bbox_r1.json` (repo) · **dashboard** `/process.txt` `sweep:` block,
`?q=sweep` for events · **API** `/api/v1/director/sweep`.

## Why

Every product is rasterised on the KG's *processing bbox* (revision 1,
frozen in `kg_bbox_r1.json`). The KG polygon envelope (revision 2,
`bbox_r2`, derived from the public BEV KG boundary) can be slightly wider,
so NE cells in the strip between the two envelopes were never observed —
all-nodata NEO1 rows at the W/E/S/N edges of a parent.

Two decisions:

1. **The processing bbox stays pinned to revision 1.** Split layouts
   (`kg_splitter`), tile grids, `_json` bboxes and the search-index `kg`
   row all key on it (the coverage oracle `app._kg_coverage_complete`
   compares at 1e-9); letting it grow would make thousands of complete
   split parents look like "holes" → mass re-queue. `bbox_r2` only feeds
   the NE declared-layer viewport (`_parent_bbox_wgs`).
2. **Slivers are swept, never whole KGs re-run.** Only the cells that are
   *actually unseen and outside bbox_r1* are rasterised, in small padded
   bbox units; the director merges the result into the parent corpus
   file.

## Pipeline

```
scan (ne_ingest tick, ≤400 parents/tick): parent complete AND data/ne/<code>.h3b on disk
   → unseen rows ∩ centroid outside bbox_r1
   ├─ none → clean (~80 % of parents)
   └─ cells → units → pending   (per side W/E/S/N, clusters split at 1.5 km gaps,
                                 pad 130 m, ≤ 8 units per parent)
dispatch (director, cache-only orchestrator): sliver_sweep.assign(workers)
   PUT <worker>/api/v1/processing/sweep_spec {specs:{parent:{bbox_new, attempt, units[]}}}
   (single VM: the local processor is the worker)
worker: sliver_sweep.peer_units → kg dicts {kg_code '63308-sw1', _parent_kg_code, bbox=unit,
   _parent_bbox=bbox_r2, _sweep=True} → process_one_kg block mode → clipped tiles
   (_compute_tile_grid_clipped) → light-GPKG step (NE h3b, tree_apices/crowns, rasters,
   terrain_coarse) → json_v2 → v2_verify → Zenodo: <unit>_light_gpkg_v2 + <unit>_ne_h3b
   + <unit>_json_v2 (never _json / _full_gpkg)
   → POST /api/v1/director/sweep/result?unit&parent&status&cells&products (body = h3b)
   status ok | nodata (outside LiDAR coverage / 0 cells) | failed
director accept_result: verify_h3b → data/ne/sweeps/<unit>.h3b; all units in → received
integrate (ne_ingest tick, oldest received first, ≤40/tick within a 150 s budget — was 6/tick until 2026-11-04): ne_ingest.merge_blocks(parent, [parent.h3b] + sweeps)
   (per cell the best-observed source wins; interior cells untouched) → done
```

**Guard**: parents whose own `_json_v2` product bboxes already reach
`bbox_r2` on every side (5e-5°) go `clean` with
`clean_reason="products on bbox_new"` (`_products_cover_bbox_new`).

States per parent: `wait → clean | pending → dispatched → received →
done`, `failed` after `MAX_ATTEMPTS` (3) or 3 integrate failures; a
dispatch without result for 8 h returns to `pending` (+1 attempt). Units:
`pending → dispatched → ok | nodata | failed`. A unit entirely outside
LiDAR coverage is `nodata` (never a failure); a parent with only nodata
units goes `done` without merge.

## Add-on products — the strip must not be a hole in the tree services

Every bbox-keyed service (`/api/v3/trees`, `heightfield` / landscape
grids, the public `segmentation` product mode, hillshade tiles) reads the
parent's **products**, so a unit uploads three add-on products under its
own code (`<parent>-sw<k>_light_gpkg_v2`, `_ne_h3b` as durability
archive, `_json_v2` ingested into `kg_v2_store` under `parent=<parent>`).

**Visibility rule — `kg_splitter.is_sweep_code('63308-sw1')`.** A sweep
code is part of the parent's product set for `kg_v2_store.codes_for_parent`
(trees v3, `landscape_query.grids_for_kg`, `app._grid25_for_kg`,
`product_segments`, `hillshade_tiles`) and **invisible** wherever
`<parent>-*` means "the blocks of the layout": `kg_docs.codes_for_parent`
(no search-index summing — the 130 m pad would double-count),
`v2_ingest` (stored, no index row / repair row / flag scan), `ne_ingest`
block merge, `cells_api.index_file`, `/process.txt` `zen_stall:`, and the
re-queue invalidation in `POST /processing/queue` (keeps `c-swN` products
when parent `c` is re-run). Overlap with the parent is handled by the
consumers: trees v3 dedupes by `tree_id`, `grid25_sample.merge` prefers
interior cells, footprints are unioned. `search_index.kg_rtree` uses the
`bbox ∪ bbox_r2` envelope so a query inside the strip finds the parent;
the `kg` row bbox itself stays revision 1.

**Archive restore** (`sweep_paths`): if an `ok` unit's h3b is gone from
disk but the manifest has a committed `<unit>_ne_h3b`, it is pulled back
via `ne_ingest.pull` (≤ 2 per call, `verify_h3b` before use).

**Failure policy** (`accept_result`): the h3b is stored first, always.
`upload: …` → the unit is re-dispatched while attempts remain; on the
last attempt it is accepted without products. `verify: …` is
deterministic → accepted, products dropped, logged. Backfill:
`python3 sliver_sweep.py rerun [codes]` / `POST /api/v1/director/sweep
{"rerun": true | [codes]}`.

## Operating

```bash
curl -s 'http://localhost:8000/process.txt?roster=0&log=0' | grep -A4 '^sweep:'
curl -s 'http://localhost:8000/process.txt?q=sweep&log=100'
curl -s 'http://localhost:8000/api/v1/director/sweep?kg=63308'
python3 sliver_sweep.py status | plan <kg> | refresh [--force] | scan [N] | integrate [N] | reset <kg>… | rerun [<kg>…]
```

Knobs (`director_config.json`): `sliver_sweep` (bool, default on),
`sweep_peers` (floor), `sweep_per_peer` (3), `sweep_min_delta` (0.0002°),
`sweep_peers_auto`, `sweep_peers_max`. `POST /api/v1/director/sweep
{"refresh":true | "reset":[codes] | "scan":N}`. Cache misses on a sliver
(unit bbox reaching into an uncached 0.1° cell) fail the unit; 3 attempts
then `failed` — check `?q=sweep` and `python3 sliver_sweep.py plan <kg>`.
<!-- VERIFY: the scan needs a declared NE cell layer for the parent (optional upstream service or SRTM_BEV_VECTOR_CONTEXT); without one the sweep stays idle. -->

## Closed sweep ⇒ observed nodata at the NE server (2026-11-06)

A parent's sweep is **closed** when `sliver_sweep.json kgs[kg].state ==
"done"`, every unit is `ok` (no pending / dispatched / hold / failed), `unswept
== 0` and `plan_deferred == 0`.  `ne_h3.view_coverage(kg)` combines that with
the view-fill state (`docs/ne-viewfill.md → Observed nodata`) and stamps the
next NEO1 view post with `view_rekey.coverage="swept"` — the NE server then counts the
post's blank rows as `nodata_n` (open water, ALS seams) instead of `blank_n`
and stops listing the KG under `revisit?kind=obs_missing`.  Consequences for
the sweep side:

* `plan_cells` caps **one planning pass** at `MAX_UNITS_PER_PARENT`; kept
  `ok` units no longer eat the room (before, a parent with 8 ok units could
  never re-plan its deferred cells — 6 parents sat at `plan_deferred>0`,
  65114 among them, and could never be flagged swept).
* A `failed` / `hold` unit keeps the parent unswept → no flag; `_reopen_unswept`
  re-plans it after `RESWEEP_COOLDOWN_S`, the integrate changes the digest and
  the ingest re-posts (flag then decided afresh).
* `python3 -c 'import ne_h3; print(ne_h3.view_coverage("65114"))'` answers the
  question "why is this KG still listed with blank_n>0" — `None` means an open
  unit, deferred cells, or a view-fill result still staged.

## Invariants

* Sweep units never touch the retry queue, strikes, completed sets or
  `_json`; on Zenodo they own exactly `<unit>_light_gpkg_v2` +
  `<unit>_ne_h3b` + `<unit>_json_v2`.
* `GET /processing/queue` keeps a parent queued while a sweep spec for it
  exists on the worker (`_v2_keep`), even though it is complete.
* `SRTM_APP_IMPORT_ONLY=1` (set by the CLI) stops `app.py` from starting a
  director thread when imported for `_kg_coverage_complete`.
