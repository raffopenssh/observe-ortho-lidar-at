# Kataster purge — DONE 2026-10-13 (historical record)

The legacy `/k` parcel surface and every piece of parcel data that v2.4
does not replace were removed on 2026-10-13. The service now serves
**v2.4 products + the public tier** and nothing else (`docs/v2.4-rollout.md`,
`docs/api-tiers.md`, `docs/ne-public-api.md`). This file is the record of
what was removed and what deliberately remains; the original plan text is in
git history (`git show 146030d:docs/kataster-purge.md`).

## Why it was free

Measured inbound on `:8000` (`net_meter`, three windows 2026-10-04 … 10-13):
zero hits on `/api/v1/parcel*`, `/parcels/*`, `/query/*`, `/lookup`,
`/cadastre/*`, `/llm/*`, `/kg/<c>/profile|buildings|new_buildings|infrastructure`.
`data/api_tokens` does not exist on the primary, so the only `/k` token is
the cluster admin token (peers + operator). siedler had already been cut
over to the public `llm.txt`.

## What was done (commits)

| step | commit | what |
|---|---|---|
| 1 gate | — | `touch data/k_landscape_off` → `api_tiers.landscape_route_retired` paths answer 410 even with a token (still in place, permanent) |
| 2 stop writing | `c5d8693` | `search_index`: no `kg_parcels` / `kg_fp_match` / parcel columns ingest (`apply_kataster_purge()` holds the Step-3 DDL); `quality_flags`: `parcel*` emitters dropped; public tree QA → `kind='top_tree'`; `TYPE_LETTER`/`LETTER_TYPE` moved to `object_segmentation`; `SCHEMA_VERSION` untouched (no full rebuild) |
| 3 delete data | `00af951` + primary one-off | `kg_parcels` (4.0 M rows) + `kg_fp_match` dropped, parcel columns dropped (`index_meta.kataster_purge = 2026-10-13`, db 3.4 → 0.9 GB); flags `parcel` / `parcel_top_obj` / `parcel_top_tree` + their `flag_events` deleted; QA-rejected `kg_trees` rows pruned (`flagged_points` single kind again); `data/segv2/dataset/` (cadastre-labelled training corpus) and `data/shares/` removed |
| 4 delete code | `79001d0` | `/k` parcel routes in `app.py` (−1 673 lines), `llm_api` `/llm/*` + `/query/buildings`, `cadastre_bridge.py`, `parcel_compact.py`, `building_footprint_match.py`, `backfill_parcel_top10.py`, `search_index` parcel queries, `static/query.html` presets, `llm.txt` parcel sections; added `/api/v1/processing/kg_search` for the dashboard autocomplete |
| local JSON | `cfea2d5`, `9578890` | primary's local v1 copies: `v2_ingest` deletes `<code>.json` when `_json_v2` is ingested (`freed=` on the `v2:` line); copies whose Zenodo `_json` is already `cf`/`stripped` are stripped in place by `ne_rollout.strip_local_json` (v24-audit thread; `local_json: suspect=N clean=M` on the `legacy:` line) |
| 5 peers | automatic | wave rollout; peers hold no parcel data beyond `cadastre_cache/` (≤ 24 h) |
| 6 docs | 2026-10-13 | `docs/siedler-contract.md` deleted; AGENTS.md / docs refreshed |

Gates (all green a few hours after Step 4): `tools/golden_public.py compare`
showed only the expected diffs (`dropped_qa` / private `trees.count` shrink
from the physical prune, `llm.txt` body, live counters); `/api/v1/health`
`ok:true`; `/contrib` 0 failed; `ne:` confirm ok; `tests/test_cells_vocab.py`
green. The pre-purge backup (`VACUUM INTO` of both dbs + tarballs) was
deleted the same day on the owner's call. Rollback of Steps 2/4 would be a
`git revert`; Step 3 is irreversible by design (nothing public ever read it).

## What remains (on purpose — run-time context, not Kataster output)

* `cadastre.py`, `umfeld.py` + `data/umfeld_token`, `cadastre_cache/`
  (≤ 24 h) — segmentation ground truth / `building_mask` / `new_buildings`
  context; `CADASTRE_PRODUCTS = False` keeps it out of every product.
* `llm_api.py` — only `/api/v1/prewarm` and `/api/v1/trees/bbox` (parcel-free).
* `api_tiers.PRIVATE_ROUTES_LANDSCAPE` + `data/k_landscape_off` — permanent 410.
* `data/ne/**` incl. the NEC1 slice (umfeld's declared layer; never leaves
  via the public tier), `data/best_model/`, `data/austria_processor/ne_declared/`.
* Legacy index rows: `kg_buildings` (fed from pre-2.4 `building_footprints`)
  and `kg_trees` (sampled apices) are frozen — `docs/search-index.md`.
* Zenodo: pre-2.4 `_json` records are re-PUT `status=stripped`, v1
  `_light_gpkg` depositions deleted with the record kept `status=removed` —
  that is the v2.4 legacy clean-up, tracked on the `legacy:` line
  (`docs/v2.4-rollout.md → Legacy clean-up`).

## Golden test (still useful)

```bash
python3 tools/golden_public.py capture data/golden/<name>.json   # ~25 s, read-only
python3 tools/golden_public.py compare data/golden/<name>.json   # exit 1 + first diverging key path
```
Captures every public route + the surviving private routes over ~40 seeded
KGs across all `product_version` buckets; `data/golden/` is gitignored
(`pre-purge.json` / `post-purge.json` exist locally).
