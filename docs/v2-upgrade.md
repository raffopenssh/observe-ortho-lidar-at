# v2 rollout / upgrade (segv2 → the product)

Load when touching `v2_ingest.py`, `kg_v2_store.py`, `kg_docs.py`, `kg_log_harvest.py`,
`v2_verify.py`, the `--v2-upgrade` path in `austria_processor.py`, or the
`_v2_upgrade_fill` logic in `peer_director.py`. Model / training internals: `segv2/README.md`,
`segv2/HANDOVER.md`.

## Products (Zenodo manifest keys)

| key | file | who writes it | notes |
|---|---|---|---|
| `_json` | `<code>.json` | fresh KGs (v1 or v2 processor) | fleet-wide **completion marker** — 44 call sites; never rename. Fresh v2 KGs write it with `version: v2`, compact. |
| `_full_gpkg` | `<code>_full.gpkg` | fresh KGs only | rasters identical in v1/v2 → no `_full_v2`. Upgrade INPUT. |
| `_light_gpkg` | `<code>_light.gpkg` | fresh v1 KGs (pre-2.4) | **legacy**: v2.4 clean-up deletes the deposition and keeps the record with `status=removed`, `size: 0` (read as absent everywhere). |
| `_json_v2` | `<code>_v2.json.gz` | upgrades + fresh v2 | `kgjson/2` (`kg_json_v2.py`), lossless, 18–22× smaller. Primary ingests it. 2.4: `cadastre_free: true`, `ne` section. |
| `_light_gpkg_v2` | `<code>_light_v2.gpkg` | upgrades + fresh v2 | light leg of the zen_stall triple (accepted as `light`). 2.4: no `parcels`/`buildings`/`parcel_outline_z` layers. |
| `_ne_h3b` | `<code>_ne.h3b` | 2.4 upgrades + fresh | NE cells (H3) observed layer, `ne_h3.py`. Absent when the declared layer wasn't built → `v2.4-partial`, repaired later (`docs/product-repair.md`). |

Manifest record `status` vocabulary since 2.4: `cf` (written by 2.4+ code,
cadastre-free), `stripped` (v1 `_json` re-PUT in place without cadastre
sections), `removed` (deposition deleted, record kept) — `docs/v2.4-rollout.md`.
A complete v2.4 KG therefore has `_full_gpkg` + `_light_gpkg_v2` + `_ne_h3b` +
`_json_v2` + `_json`; upload order full → light_v2 → ne_h3b → json_v2 → json.
The v2.4 **fast path** (`v24_fast.py`, cd08d74) relabels a KG from its
v2.3+ `light_v2` + `json_v2` in seconds instead of re-running the tile loop
(`fast_cand=` / `fast_running=` on the `v2:` line).

Codes are product codes (parent `19570` or split block `49006-north`).

## Fleet side (peer, no credential, no BEV/openEO)

`austria_processor.py --cache-only --v2-upgrade` (director start payload
`{cache_only:true, v2_upgrade:true}` → app.py appends `--v2-upgrade` and writes the marker
`data/austria_processor/v2_upgrade_mode`). At startup the processor reads `retry_queue.json`
(the whitelist the director PUT) and appends **upgrade units** for codes that pass
`v2_upgrade_eligible`: `_json` + committed `_full_gpkg`, no `_json_v2`, < 2 strikes in
`v2_upgrade_failed.json`. Per unit: `_v2_fetch_inputs` (v1 JSON → bbox + verify baseline; full
GPKG, 4 streams, disk-checked, md5) into `data/austria_processor/v2_source/`, then
`process_one_kg(source_gpkg=…, v1_doc=…)`: `v2_source` shim replaces BEV reads, no gpkg_full,
`_light_v2.gpkg` + `_v2.json.gz`, `v2_verify.verify_before_upload` (37 checks) → on FAIL
`aborted_v2_verify_failed` (files deleted, strike, **no** failed_kgs entry), on PASS ordered
upload light_v2 → json_v2 under the fleet Zenodo lock. GPKG deleted afterwards.
`progress.v2_upgraded` counts; log prefix `v2up:`.

`MODEL_VERSION` defaults to v2 iff `segv2.model_v2.available()` (lightgbm importable + model
present or fetchable from Zenodo depo 22824068/22824064). `admin_update` pip-installs lightgbm
(`_ensure_v2_deps`) after every git pull, so the fleet gains v2 on the normal rollout wave. A peer
without lightgbm logs `--v2-upgrade requested but MODEL_VERSION=v1` and just runs v1 cache-only.

## Director side (`peer_director._orchestrate_cache_only`)

**Priority (2.3)**: `_compute_v2_upgrade_candidates` ranks `(-1 half-landed pair, then
older-v2 re-upgrades, then never-upgraded v1)` while `V2_REUPGRADE_FIRST=True` — the
2.3 top-N change rewrites every v2 JSON, so the live v2 line converges first. Set it
False to go back to never-upgraded-first.

`_v2_upgrade_fill(whitelist, cfg)`: when `len(cache_ready_kgs) < V2_UPGRADE_FILL_BELOW_READY`
(40) append up to `MAX_V2_UPGRADE_PEERS` (24) codes from `_compute_v2_upgrade_candidates()`
(manifest scan, smallest `_full_gpkg` first, 5-min cache, skips codes whose primary ingest struck
out). Zero injection while fleet Zenodo warns > `V2_UPGRADE_ZEN_WARN_MAX` (2/min). The codes skip
block expansion (they are already product codes; the completed-block guard would drop them) and
join the LPT partition. Config keys in `director_config`: `v2_upgrade` (bool, default true),
`v2_upgrade_fill_below_ready`, `max_v2_upgrade_peers`. State: `v2_upgrade_assigned`,
`_v2_cand_cache`. Status: `/api/v1/director/status.v2.dispatch`.

`processing_queue_get` (GET `/processing/queue`, which prunes "complete" codes) keeps
upgrade-eligible codes while the `v2_upgrade_mode` marker exists — otherwise the whitelist the
director just PUT would be pruned before the processor read it.

**Peer-claim check is in-progress-only for upgrade units.** The processor's per-KG
`_get_peer_claimed_kgs` folds every reachable peer's manifest-derived `completed` list into the
claimed set. An upgrade unit is v1-complete *by definition*, so with that check every unit was
skipped as "claimed by peer" and the peer exited seconds after start; the director then re-issued
the same 24 codes every tick (Sep 2026, right after the fleet doubled — the new peers were the
first reachable ones advertising the full completed set). Units tagged `_v2_upgrade` now only
honour live `blocks` / `parents_unsplit` claims. Fresh-KG semantics are unchanged.

## Primary side

* `kg_v2_store.py` — `data/kg_v2_store.db` (WAL). `kg_v2` (verified gz blob per code, `bbox`,
  `generated_at`, `v1_bytes_freed`), `kg_log` (per-code history rings, cap 600 rows), `meta`.
  NOT the search index (that one is rebuildable and DROPs tables); the store is source data.
  Test with `kg_v2_store.STORE_PATH = Path('/tmp/x.db')` **before importing** anything else.
* `kg_docs.py` — the only doc loader: store first, `json/<code>.json` second. Used by the search
  index selector, `/api/v1/kg/<code>`, coverage oracle, quality flags.
* `v2_ingest.py` — daemon thread in `app.py` (`_v2_ingest_thread`, primary only, fcntl
  single-flight across gunicorn workers, 5-min tick, ≤20 codes/tick, `V2_INGEST=0|1` env
  override). Per code: download (+size/md5 check) → `verify_for_ingest` (v1 doc = file or older
  store copy; flat index row fallback) → `store.put` **first** → `search_index.update_kg` →
  delete `json/<code>.json` + `add_freed` → `quality_flags.scan_doc`. Then
  `kg_log_harvest.harvest_live()` + `harvest_archive_step(1)`. Failures: `meta.ingest_failed`
  `{code:{n,reason,uploaded_at}}`, 3 strikes per upload (a newer upload resets).
* Peer-sync (`_sync_peer_data`) skips `_json` downloads for codes in the store.
* `kg_log_harvest.fold_lines` is hooked into `app._archive_lines` — every merged-log line is
  folded into the per-code ring once, when it leaves the live ring.

## Telemetry

**Source of truth for every progress indicator is the search index**
(`search_index.progress_summary(manifest)`): one row per *parent* KG, `product_version` = the
manifest version tag (`v1`/`v2`/`v2.1`/`v2.2`) taken as the **minimum across split blocks**,
plus `json_uploaded_at` / `json_v2_uploaded_at` (from the manifest; `sync_manifest_links()` is
run by the app watcher whenever `zenodo_manifest.json` changes). Consequences:
* `progress:` / health banner / `processing/status.manifest_rate_*` / daily sparkline count
  fresh `_json` completions of parents only — never split blocks, never v2.x upgrade
  re-uploads (Sep 2026: `done=3508` was product codes and the `13.9/h` was the upgrade rate).
* `v2:` / `status.v2.upgraded_parents` = parents whose every block is at
  `v21_products.MANIFEST_VERSION`; `stale_total` / `stale_versions` = parents on an older v2.x
  (re-upgrade pending); `upgraded_24h` / `rate_per_h` / `eta_days` see only current-version
  uploads, so a parent that went v2 → v2.1 → v2.2 is counted once. `by_version` = parents,
  `codes_by_version` = product codes (manifest). ETA falls back to manifest `_json` timestamps
  when the index has none (`source: "manifest"`).

`/process.txt` `v2:` line:
```
v2: at_v2.2=N/8440 (p%) +N/24h @r/h eta=Dd parents_by_version[v1=… v2=… v2.1=… v2.2=…] stale=N any_v2=N ·
    fresh_v2=N · verify_fail=N pending=N ·
    store=Ncodes/MB freed=GB avg/kg=KB · json_files=N disk=GB · kg_log=codes/rows/KB
    archive_remaining=Dd · dispatch=N/Tcand (fill<40 ready, cap 24)
```
Structured: `/api/v1/director/status.v2`, `/api/v1/model/v2?rollout=1`. Grep the merged log
for `v2up:` (peers), `v2verify` (gate verdicts), `v2_ingest` (primary).

**`verify_fail` triage** (`python3 -c 'import v2_ingest,json;print(json.dumps(v2_ingest.failed(),indent=1))'`).
Two classes were auto-handled from 2026-09-29 on:

* **Stale-layout baseline** — reason `parcels_vs_v1 … bbox_covers_v1`, code is a block. Split
  layout drifted (`kg_splitter` is a function of `kg_strikes.json`), the block *code* survived
  but names a new rectangle; the stored v1 doc is the old rectangle. `verify_for_ingest` now
  drops the baseline when the v2 bbox matches `app._current_block_bboxes(parent)[code]` and
  the v1 bbox does not (warning `baseline_layout_stale`), and verifies structurally only.
* **Dangling `_json_v2`** — reason `download: 404 …`. Deposit deleted on Zenodo (e.g. parent-level
  product left behind after a re-split). `sweep_gone()` (each ingest tick) probes the
  *deposition* — a 200 counts as present even if the filename isn't listed (PUT-then-DELETE
  replaces rename files) — and drops the manifest key (tombstoned via `app._manifest_drop_keys`)
  only after **2 gone-verdicts ≥ 30 min apart**, gated on a Zenodo control probe + clear
  `zenodo_circuit`, max 3 drops/tick. One `present` verdict clears the candidate. Same
  discipline as `v2_regen` — never drop manifest entries on a single 404.

`/process.html` mirrors this as the **v2 Rollout** card (`renderV2`, fed by
`director/status?slim=1 → .v2`; click the card to filter the Live Log to `v2`). Per-peer:
progress.json carries `model_version` / `v2_upgrade` / `v2_upgraded` and `current_kg.v2_upgrade`
(written in `austria_processor.main()` / `set_current_kg`); `peer_director.get_status` forwards
them as `model_version`, `v2_upgrade_mode`, `v2_upgraded`, `current_kg_v2_upgrade`. The peer strip
shows a `v2` badge (`v2↑` = upgrade mode; yellow `v1` = running peer without lightgbm) and a
`V2-UP` role tag while a peer is on an upgrade unit. The Zenodo badge appends `v2: N json + M light`
(from `/processing/manifest?summary=1 → by_product`).

## Ops

```bash
python3 v2_ingest.py status                 # v2: line + JSON
python3 v2_ingest.py                        # one ingest tick now (primary)
python3 v2_ingest.py 19570                  # dry ingest one code (no delete / index / flags)
python3 -c 'import v2_ingest;print(v2_ingest.failed())'
python3 kg_log_harvest.py 19570             # targeted history fold + last rows
curl -s 'localhost:8000/api/v1/kg/19570?history=1' | jq '.history[-5:]'
```
Re-ingest a code: delete its store row (`kg_v2_store.delete(code)`) — next tick re-downloads.
Reprocess an upgrade: drop `<code>_json_v2` + `<code>_light_gpkg_v2` from the manifest
(the requeue/tombstone path already includes both suffixes).

**Never use `POST /processing/queue {skip_processed:false}` (dashboard "Queue KG by name" /
re-queue) to get a v1-complete KG onto the current v2.x.** That path tombstones and deletes
*every* product pointer incl. `<code>_full_gpkg`, so the KG stops being a v2-upgrade candidate
(the upgrade path needs the v1 full GPKG in the manifest) and becomes a from-scratch v1 rerun
that needs Copernicus → cache miss → frontier → openEO. 63330/Kohlschwarz sat 36 h like that
(Sep 20–22 2026) while its 5 Zenodo files were intact. Recovery recipe:

```bash
TOKEN=$(cat data/admin_token)
curl -s -X POST -H "X-Admin-Token: $TOKEN" -H 'Content-Type: application/json' \
  -d '{"prefix":"63330"}' localhost:8000/api/v1/admin/drop_tombstones      # incl. _requeue
# rebuild the 3 v1 (+2 v2) entries from the deposition API and push them:
#   GET /api/deposit/depositions?q=<code>  →  /depositions/<id>/files  (bucket, md5, size)
#   POST /api/v1/manifest/push {"key":"<code>_full_gpkg","entry":{...,"uploaded_at":<depo modified>}}
# a 409 stale_vs_tombstone = the other gunicorn worker's in-memory copy; retry or restart srv.
curl -s localhost:8000/api/v1/processing/queue >/dev/null                 # GET-prune drops the code
curl -s -X PUT -H "X-Admin-Token: $TOKEN" -d '{"<code>-<block>": -1}' localhost:8000/api/v1/processing/kg_strikes
curl -s -X POST -H "X-Admin-Token: $TOKEN" -d '{"kgs":["<code>"]}' localhost:8000/api/v1/director/v2/priority
```

**Then verify the drop actually reached the fleet** (aa82994, 2026-09-22): the drop journal
rides the director's 5-min `cache_manifest` PUT to every peer. Before that commit there was
*no* primary→peer path (peers only peer-sync among themselves; the primary is absent from
`peer_urls.txt`), so every peer kept the live tombstones, dropped 63330 from
`completed_codes` and re-ran it as a v1 KG for another 3 h after the "fix". Check a peer:

```bash
curl -s --compressed 'https://example.invalid:8000/api/v1/processing/peers?manifest=0' \
  | python3 -c "import json,sys; d=json.load(sys.stdin); print({k:v for k,v in d['tombstones'].items() if k.startswith('<code>')}, {k:v for k,v in d['tombstone_drops'].items() if k.startswith('<code>')})"
# want: {} + the drop timestamps. Peers on pre-aa82994 code ignore the piggybacked journal.
```

Strike reset: use `scripts/reset_kg_strikes_v2pending.py --apply --codes <code>` (pushes the
negative to *every* peer) — a primary-only PUT is undone by the next peer max-merge push.
Run it **after** the tombstone drop has converged, or bouncing peers re-earn block strikes.

## Strikes, deferrals, and the v2_regen auto-requeue (2026-09-21)

Peer file `v2_upgrade_failed.json` = `{code: {n, defer, reason, ts}}`; read it with
`GET /api/v1/admin/v2_strikes` (admin token), clear with `POST …/v2_strikes/clear
{"codes":[…]}` or `{"checks":[…]}`. Fleet union at `data/austria_processor/v2_strikes_fleet.json`
(`v2:` line → `strikes_fleet=… struck_out=…`).

* **Verify FAIL** → `n += 1`, struck out at `V2_UPGRADE_MAX_STRIKES = 2`.
  * `parcel_segmentation_coverage_pct_ge_v1` (2026-09-22): fatal only when **both** the raw
    figure and the ≥300 m² figure drop > `TOL.seg_coverage_pp_fatal` (8 pp) — that is a lost
    tile, and `segment_type_no_holes` / `lidar_tiles_ge_v1` / `segmented_area_ge_v1` catch it
    independently. Smaller gaps become the non-fatal `…_drift` warning (still carrying the
    `lost=N … v1_types[…]` forensic tail). Before: fatal past 2.5 pp on the ≥300 m² figure
    alone, whose small denominator on 50–200-parcel KGs turned 3–6 lost parcels into 3–7 pp;
    217 strike events / 35 parents struck out in 48 h, every one deterministic (identical
    figures on two peers) — a segv2-vs-v1 class-set difference on 3–7 % of parcels, not a
    data hole. Root cause (which parcel types lose `area_summary` under segv2) is still open;
    mine `?q=coverage_pct_drift` for the `v1_types[…]` histogram.
  * Fleet-wide clear after a gate fix: `python3 scripts/clear_v2_strikes_fleet.py --checks
    parcel_segmentation_coverage_pct_ge_v1 --apply` (hits every peer's
    `/api/v1/admin/v2_strikes/clear`; `--reason-re 'download: HTTP 5\d\d'` drops the
    pre-deferral 5xx strikes). The director's fleet union drops a peer's stale entries on
    its next full status push.
* **Deferral** (input download 5xx / timeout / disk pressure, or a 404 while the fleet
  `zenodo_degraded` circuit is tripped) → `defer += 1`, **no strike**; struck out only after
  `V2_UPGRADE_MAX_DEFERS = 8`. (Before: a deferral was a strike, so one Zenodo outage struck
  out every code a peer touched twice.)
* **Stale v1-JSON md5** in the manifest → accepted if the bytes parse as this KG (baseline
  only), `v1 JSON manifest md5 stale` warning.
* **`_full_gpkg` or `_json` gone** (404 with circuit clear) → fatal strike on the peer +
  reported as `status.v2_gone` (`{code: {ts, key}}`; the key names which product is gone —
  19704's v1 JSON vanished while the manifest still listed it). The director (`_check_v2_regen`, every 5 min, `data/austria_processor/
  v2_regen.json`) then verifies it against the deposition API — **never while the Zenodo
  circuit is degraded**, control probe must be 200, **two "gone" verdicts ≥30 min apart** —
  and requeues the KG for a fresh v2.2 run through the canonical
  `POST /processing/queue {skip_processed:false, keep_products:true, gone_keys:[<code>_full_gpkg]}`.
  `keep_products` stamps the `_requeue` tombstone and invalidates *only* the gone key: the v1
  `_json` / `_light_gpkg` (and any `*_v2`) stay referenced and served until the fresh run
  replaces them. `sync_queue_to_peer` forwards the same `gone_keys`, so peers never tombstone
  more than the primary did. Caps: 3 requeues/tick, 20 outstanding. States:
  `candidate → confirming → requeued → done` (or `dismissed` when the file turns out to be
  present). Follow on `/process.txt` (`v2_regen:` line) and `?q=v2regen` (director events).

  **Correction (2026-10-21) — lossy is the steady state.** Three holes froze the pipeline
  for a month (20 `requeued`, 23 `confirming` with 6 gone-checks each, 11 `candidate`,
  67105 re-dispatched + deferred 9×/48 h):
  1. The director's `prune retry_queue` had a timestamp-ignoring
     `_kg_structurally_complete` fallback. A `keep_products` requeue keeps the v1 `_json`
     by design, so it is structurally complete *by construction* → tombstones dropped
     2–3 min after every regen requeue → GET prune evicted the code → peer-sync re-imported
     the dead `_full_gpkg` pointer. The fallback now only fires once every product key
     tombstoned in the same batch (same ts) is back in the manifest.
  2. The verification gate used the raw `zenodo_circuit.degraded` flag, which is tripped
     for days while ~20 GB/h lands (`ZENODO-LOSSY`). It now holds only when
     `app._zenodo_upload_health()` shows a dry hour (<5 files / <1 GB); the deposition-API
     GET it uses is independent of bucket PUT health anyway.
  3. Peers only reported `v2_gone` for the *strike* flavour (`gone from Zenodo`); the
     no-strike `404 during Zenodo-degraded circuit` deferral never became a candidate.
     `status.v2_gone` now carries both.
  Settle loop also self-heals: a `requeued` entry that is >30 min old, not in the queue,
  without `_requeue` tombstone and with the gone key referenced again is demoted to
  `confirming` (`reverted` counter, `v2_regen <code>: requeue reverted` log line).
  **Orphan residue is dismissed, not requeued** (Oct 2026): a candidate /
  confirming / requeued code whose *parent bbox is covered by committed
  `_json` products without any of the code's own entries*
  (`_v2_regen_orphan` → `app._kg_coverage_complete(parent, entries − code_*)`)
  is old-split residue — block codes of a superseded layout (`48210-east`
  after the unsplit 2.4 run), or a parent whose blocks now cover it
  (`03123`, `72321`, `66326`). Those used to loop requeue → queue-GET prune
  → "reverted" → requeue forever, each round costing a `v2up: … gone from
  Zenodo` dispatch. Now: `state=dismissed`, note `orphan (old-split residue)`,
  dropped from `retry_queue.json` + `_requeue` tombstone, director event
  `v2regen: <code> dismissed — old-split residue`.

### Primary ingest strikes (`verify_fail=N` on the `v2:` line)

Separate from the peer strikes above: `v2_ingest` on the primary verifies every landed
`_json_v2` against the stored v1/previous doc (`v2_verify.verify_for_ingest`) and strikes it
(`store.meta ingest_failed`, `MAX_FAILS = 3` per upload; `director/status.v2.verify_fail_codes`
lists them). Three kinds, all expected to **self-heal** — do not hand-clear:

* `download: 404` → `sweep_gone` (two deposition probes ≥30 min apart, circuit not dry)
  drops the dangling manifest key; typical for parent-level `_json_v2` residue after a split
  (72008/72120/72194).
* `*_ge_v1` gates reproduced **bit-identically by a second upload** → accepted as
  deterministic (`_deterministic_repeat`, 66319 Laafeld).
* **BEV clipped to Austrian territory (2026-10-21, 80110-southeast-2 Sölden).** BEV
  initially served ALS cross-border strips (South Tyrol / Bavaria); it now clips cleanly to
  AT for sovereignty reasons. The v1 baseline therefore carries DTM tiles that every fresh run
  reads as all-NoData (`outside_austria` + `no_valid_pixels`, same as fully foreign tiles —
  **not** `upstream_fail`), and `lidar_tiles_ge_v1` / `segmented_area_ge_v1` strike out
  forever. `_bev_empty_confirmed` waives the strike when every fatal error is a `_ge_v1`
  gate, no tile reported `upstream_fail`, each tile v1 had but v2 lacks is flagged empty by
  the pipeline, **and** a direct windowed BEV read from the primary (`raster_io.read_dtm_dsm`,
  default dataset, cached per upload in the strike record as `bev_probe`) returns <100 valid
  px. Director event `v2_ingest: <code> accepted — DTM tiles v1 had are now clipped/empty on
  BEV`. Expect this on every border KG re-run whose baseline predates the clip; the shrunk
  `total_segmented_area_sqm` is the correct value, not a data loss.

**Zenodo replace is PUT-then-DELETE** (`zenodo_client._replace_in_bucket`): same filename →
one in-place PUT, different filename → PUT new, then DELETE old. A failed PUT can no longer
leave a deposition empty (how 18127 / 01504 / 19102 lost their full GPKG), and a v2.2
re-upgrade keeps the v2 / v2.1 product live until the new one is committed.

## Invariants

* `_json` stays the completion marker; upgrades never touch `_json`/`_light_gpkg`/`_full_gpkg`.
* `store.put` before `update_kg` before deleting the v1 file (selector reads store first).
* Only the primary ingests / deletes v1 files. A temporary director elsewhere keeps its file
  corpus; nothing is lost.
* `CredentialRotatedError`-class rules from `docs/copernicus-throttle.md` are irrelevant to the
  upgrade path (no upstream fetches) but still apply to fresh v2 KGs.
