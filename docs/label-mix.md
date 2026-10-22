# Label-mix repair (restored-checkpoint label-id collision, Oct 2026)

**Symptom** (spotted by umfeld's glyph on Kohlschwarz 63330): NE cells
`geb` ≈ 1.0 with `h_max = 0`, `structures_n = 0`, phenology *pasture*;
light GPKG `segment_type` raster shows 69 ha roof vs 25 ha roof objects;
`segments` polygons glued across the KG (polygon area ≫ `area_sqm`).

**Root cause** (`austria_processor.process_one_kg`, checkpoint-restore
path): a tile restored from `tile_checkpoints/<kg>/tile_N.pkl` got its
`core_objects` renumbered to the new global id range but its
`seg_result["labels"]` raster kept the ids of the original run. Every
stitcher (`build_full_gpkg_tiled`, `build_light_gpkg_tiled`,
`gpkg_streamed`, `ne_h3.build_observed`) resolves pixels through one global
`obj_map`, so pixels of the restored tile took the type / polygon of an
unrelated object. Only KGs that resumed from checkpoints are affected
(~26 of 3 816 in the h3b corpus; `python3 label_mix.py`).

## The fix — four layers

| Layer | Where | Effect |
|---|---|---|
| restore remap | `_remap_restored_tile_ids` (closure in `process_one_kg`, before `_load_tile_checkpoint`) | renumbers `seg_result.objects ∪ core_objects` **and** rewrites `labels` via a LUT |
| in-memory stitch guard | `_assert_tile_labels_consistent` → `LabelMixError` (`build_full/light_gpkg_tiled`, `gpkg_streamed`) | tile whose labels resolve to foreign objects aborts the KG *before* any raster is painted — nothing corrupt is written or uploaded, KG retries |
| pre-upload gate | `v2_verify.segments_label_consistency` (fatal, `TOL.label_mix_frac=0.005`, 20 k sample, ~0.6 s) | polygon-area vs `area_sqm` ratio on the light GPKG |
| fast-path decline | `_process_kg_v24_fast` → `v24_fast_declined: label-mixed …` | a corrupt v2.2/2.3 light GPKG is never relabelled into a v2.4 product; the full path from the full GPKG runs instead |

## Corpus repair — `label_mix.py` (director tick)

`label_mix.sweep()` runs inside the director's `_ne_audit` thread (same
cadence as `ne_rollout`): incremental audit of new/changed `data/ne/*.h3b`
(cells dominated by `geb`/`wald` with `h_max ≤ 1 m` and no structure/tree;
clean p99 = 0.17 %, affected 6–93 %) + harvest of `fast declined …
label-mixed` lines from the 24 h log ring → `detected` → plain force
requeue (`POST /processing/queue {skip_processed:false}`, all products
invalidated, full rerun with the fixed restore path) → `done` when a fresh
`_json` postdates `requeued_at`. Bounded: 6 requeues/tick, 40 outstanding,
2 attempts then `stuck`. State: `data/austria_processor/label_mix.json`.

- `/process.txt` line `label_mix: detected=… requeued=… done=… stuck=…`;
  events `?q=label_mix`. Dashboard: v2 card second line.
- `/api/v1/director/status` → `v2.label_mix` (`counts`, `rows`).
- CLI: `python3 label_mix.py [scan|sweep|reset <code>…]`.
- Tests: `PYTHONPATH=. python3 tests/test_label_mix.py`.

Why a full rerun and not a v2.4 fast fix: the full GPKG's own
`segment_type` / `segments` layers are corrupt, and the full-GPKG upgrade
path does not rebuild the full GPKG — only a fresh tile loop gives a
solid product. The reruns are cheap-ish anyway: tile checkpoints are kept
and now restore correctly.
