# `app.py` — Mental Model

22k-line Flask app (the hidden `/k` tier since Oct 2026 — `docs/api-tiers.md`; the public tier is `public_api.py`). Single process, two gunicorn workers × four threads.
Everything mutating goes through admin-token auth except loopback (127.0.0.1
without XFF). All long files use `# === SECTION:` markers —
`grep -n '# === SECTION' app.py`.

## Roles this single file plays

```
          ┌─────────────────────────────────────┐
          │           gunicorn (srv.service)         │
          │  worker A           │     worker B          │
          │   │   │   │           │     │   │   │   │   │
          │  thr thr thr thr   │    thr thr thr thr    │
          └──┬────────────────────┴───────────────────────┘
             │   one worker holds:
             ├─ director loop (fcntl lock on director.lock)
             ├─ status push thread
             ├─ peer-data sync thread
             ├─ role-data eviction loop
             └─ zenodo lock broker (in-memory mutex)
```

- **Sync HTTP**: most endpoints (geometry parse, query index, share I/O, shallow proxies)
- **Async tasks**: `/segment`, `/onestop`, `/export/geopackage|mbtiles` — spawn daemon threads, write file-backed progress
- **Background loops** (one per worker, gated by file lock so only one is real): director, status push, peer sync, role eviction
- **Proxies**: dashboard `process.html` and `query.html` call director + processor APIs through this file

## Section map (line numbers as of 2026-10-13; `grep -n '# === SECTION' app.py` for the live list)

| L | Section | What lives here |
|---:|---|---|
| 423 | Response compression + conditional GET | `_compress_and_etag` (gzip ≥1 KiB, 304 on `If-None-Match`) |
| 774 | Copernicus credentials API | `/api/v1/credentials*` — add/list/validate creds. Pure cache reads on hot path; only director may probe. |
| 2344 | Status push to director | Thread that POSTs `/processing/status` to current director (peers → director) |
| 2757 | Role-data eviction | Demoted peers free `json/*.json` + `search_index.db` after `ROLE_EVICT_GRACE_SECONDS` (1 h) |
| 3085 | Processing queue | `_TASK_SEMAPHORE` — max 2 concurrent heavy tasks (segment / export) |
| 3181 | Async task system | File-backed progress (`/tmp/segment_progress/`) + result storage (`/tmp/segment_results/`, gzip, 4 h TTL). Auto-saves to share. |
| 3782 | Austria Processor endpoints | Proxy `/processing/*` endpoints. Read `progress.json`, write retry queue, start/stop subprocess, broker cache misses, `/processing/kg_search` (dashboard autocomplete). |
| 5583 | KG coverage completion oracle | `_kg_coverage_complete` — "done" iff committed `_json` products cover the parent bbox |
| 5734 | Split-layout-drift wedge detection & repair | |
| 7150 | Bandwidth & Peer Director | Local vnstat reader + all `/director/*` endpoints. Director loop body. |
| 8784 | srv memory sampler | `srv_mem` 5-min sampler, `/admin/mem_top` |
| 9593 | Zenodo upload mutex | In-process mutex used by primary's own subprocess |
| 9652 | Zenodo lock broker proxy | `/zenodo/lock` — cluster-wide TTL lease, peers point at primary |
| 10013 | bev-mode relays | `/api/v1/ne/declared/<kg>` — umfeld `lu` NEC1 relayed to bev-mode peers (peers never call umfeld's `/k`) |
| 10877 | Director high-availability | Heartbeat / shadow / handover / takeover / step-down / announce |
| 11264 | Search index API | `/index/*`, `/kg/<code>` (+ `history|landcover|heightfield|layers|segments`), `/insights` |
| 11666 | Zenodo fetch progress | sync GPKG endpoints answer 202 + progress while the light GPKG downloads (`zenodo_fetch.py`) |
| 11751 | `/kg/<code>/heightfield` | index-only gridded DTM estimate |
| 12056 | Geometry + parameter helpers | `_get_geometry`, `_get_params`, `_validate_area`, `_clean_polygon` |
| 12547 | `/elevation` | DTM enrichment |
| 12639 | `/terrain` | Slope/aspect/roughness |
| 12673 | `/segment` | **Main analysis endpoint.** `_segment_core`, `_segment_worker` (async). |
| 13187 | `/segment/overlay` | RGBA raster rendering, overlay cache |
| 13772 | `/export/geopackage` | Async GPKG (raster + vector) |
| 13880 | `/export/kml` | Grouped/styled KML |
| 14768 | `/export/mbtiles` | Async single-layer MBTiles |
| 15041 | `/changes` | Multi-date DTM temporal analysis |
| 15108 | `/changes/trees` + `/trees` | Tree growth analysis |
| 15477 | `/api/v2` tree service | apex-based forestry inventory |
| 16402 | `/api/v3` tree service | product-backed: `tree_apices` + grid25 |
| 16989 | Multi-epoch summary | Aggregated change summary |
| 17019 | `/info` | Server info, capabilities, `git_commit`, deps |
| 17021 | LiDAR/ortho/DTM/CIR/Hansen | Tile overlays + raw GeoTIFF download |
| 17622 | RF classifier training | `/classifier/train`, `/classifier/status` |
| 17835 | `/docs` + share | Docs endpoint, share helpers |
| 17941 | `/attribution` | `attributions.py` |
| 17995 | `/layers` | Per-bbox availability check |
| 18094 | `/onestop` | Single-URL segment + download (queued) |
| 18603 | Quality flags + feedback | `/flags*`, `/feedback*` |
| 18970 | `/parse-geometry` | KML/GeoJSON/Shapefile/GPX/WKT upload |
| 19125 | `/share` | Save/load/rename/list shares (1 GB cap, LRU, `data/shares/`) |
| ~19650 | `/process.txt` | text dashboard renderer (`health:` banner, `products:`/`v2:`/`zen_stall:`/`legacy:`/`ne:` lines) |

The parcel / cadastre sections (`/parcel*`, `/parcels/batch`, compound
query parsers, cross-API bridge, `/kg/<c>/profile|buildings|new_buildings|
infrastructure`, `/llm/*`) were deleted on 2026-10-13 (`docs/kataster-purge.md`);
`api_tiers.landscape_route_retired` answers 410 for those paths.

## Async task lifecycle

```
client POST /segment?async=true
    → returns task_id (uuid)
    → spawns daemon thread _segment_worker(task_id, ...)
          │
          ├─ writes /tmp/segment_progress/<task_id>.json (atomic temp+rename)
          ├─ reads aborted flag from same dir
          └─ on success: writes /tmp/segment_results/<task_id>.json.gz
                           and saves auto-share `auto-<task_id[:8]>`

client polls /segment/progress?task_id=<id>
    → reads progress JSON. When status=done, includes auto_share_id.

client GET /segment/result?task_id=<id>
    → streams gzipped JSON.

client POST /segment/abort?task_id=<id>
    → writes abort flag; worker checks at safe points and bails.
```

Frontend stores active `taskId` in `localStorage`; on reload either resumes
polling or loads the auto-share. Cleanup runs every hour, drops anything
>4 h old.

## Background threads (one is real, others fail file-lock)

| Thread | Purpose | Cadence | Section |
|---|---|---|---|
| Director loop | Reads `peers.json`, polls bandwidth, picks active peer, pushes plans, reissues cred slices, manages parallel frontiers + cache-only fleet | 30 s tick | 7150 |
| Status push | POSTs local `/processing/status` to current director | 30 s | 2344 |
| Peer data sync | Pulls KG JSONs + manifest from peers, merges manifest, refreshes search index | 5 min | (utility) |
| Zenodo lock heartbeat | Renews lease on broker every 30 s while holding | per-lease | 9593 |
| Role-data eviction | Demoted peers — frees JSON+index after 1 h grace | 10 min | 2757 |
| HA watchdog | Pings director heartbeat, promotes shadow on 6 misses | 30 s | 10877 |

All loops use `_DIRECTOR_FILE_LOCK` style fcntl locks so only one gunicorn
worker actually runs them — the other worker's loop sleeps.

## Authentication

`_require_admin_token()` (see top of file). Loopback (127.0.0.1, no XFF) is
exempt. Token at `data/admin_token` (mode 0600), re-read every call so
rotations propagate without restart.

Mutating routes that require it:
- `/admin/*`
- `/director/*` (except `heartbeat`, `identity` GET)
- `/processing/start|stop|pause|resume|single|retry|throttle|cache_misses|cache_manifest`
- `/credentials*` POST/DELETE
- `/zenodo/lock*` (peer→broker)

Dashboard prompts on first 401 and stores token in `localStorage` key
`srtm_admin_token`. Reset: `srtmResetAdminToken()`.

## Where to look (debug fast-path)

```bash
# Endpoint lives in which section?
grep -n '# === SECTION\|@app.route' app.py | grep -B1 'route("/api/v1/<path>"'

# A specific async task
ls /tmp/segment_progress/ /tmp/segment_results/
cat /tmp/segment_progress/<id>.json | python3 -m json.tool

# Director state
cat data/austria_processor/director_state.json | python3 -m json.tool

# Who holds Zenodo leases? (broker on 127.0.0.1:8001; proxied via :8000)
curl -s localhost:8000/api/v1/zenodo/lock | jq '{shared_used,shared_slots,cache_held,leases}'
```

Zenodo lock classes (2026-09-21): `kg_upload*` leases are **shared**
(bounded pool, `ZENODO_LOCK_KG_SLOTS`=8 — each KG product has its own
deposition, so they never 409 each other); everything else
(`cache_flush_zip:*`, `reconcile`, `chkpt_upload:*`/`chkpt_delete:*`) is
the **cache** class — one at a time, but **independent of the shared
pool** (they write the shared tile-cache draft, which KG uploads never
touch). Never-heartbeated leases are reaped after 100 s (`ORPHAN_S`, 3 missed heartbeats);
heartbeated ones after the 120 s TTL.

**Concurrency experiment (2026-10-10 06:22 UTC, evaluated 21:15 UTC).**
Cap `ZENODO_LOCK_KG_SLOTS` 16→8 (drop-in `kg_slots.conf`) + client
`_ACQUIRE_TIMEOUT` 1800→7200 s (f0b19ef). Hypothesis "fewer concurrent
multi-GB PUTs → faster per-stream → fewer SSLEOF deaths → more GB/h
landed" was **not** confirmed. 12 h before vs 12 h after (manifest
`uploaded_at` + merged log):

| metric (12 h window)            | before | after | note |
|---|---|---|---|
| landed GB (all products)        | 30.0 (2.50/h) | 23.8 (1.99/h) | −20 %, within supply noise |
| `_full.gpkg` landed             | 14 / 15.1 GB | 10 / 12.4 GB | |
| `_full.gpkg` PUT starts (streaming lines) | 75 / 91.7 GB | 33 / 36.5 GB | cap halves attempts |
| PUT failures (any / final 6/6)  | 641 / 91 | 366 / 54 | −43 %, proportional to attempts |
| full-PUT fail per start         | 5.1 | 5.5 | **per-stream failure unchanged** |
| `upload_full_gpkg` step median  | 109 min | 102–110 min | **per-stream speed unchanged** |
| lease acquire timeouts → no lease | 0 | 25 | cap leaks (11 uploading vs 8 slots) |
| peers sitting in `upload` step  | — | 11/48, up to 6h50m | peer-hours lost queueing |

Read: SSLEOF / `Max retries exceeded` rate is proportional to attempts
and insensitive to our concurrency, i.e. Zenodo-side, not contention on
our egress. The cap only trades wasted upload bandwidth (attempted→landed
efficiency 16 % → 34 %) for peers blocked in the lease queue; it does not
raise GB/h. The failure collapse seen 20:00–21:00 UTC (5 → 2 fails/h,
9.6 GB/h landing) started 14 h after the cap — Zenodo recovery, not us.
Recommendation: restore 16 slots (drop the drop-in); the 7200 s client
timeout is harmless and can stay.

History: before the pool, KG uploads were fully serialised — with a
degraded Zenodo (10–30 min per upload) every peer hit the 1800 s client
timeout and "proceeded without lease". The first pool version (3b1b9bd,
same day) made the cache class *exclusive* (needs zero leases, drains
the shared class for 300 s while refused). That re-serialised the fleet
through a different door: every KG completion fires a `chkpt_delete`,
each one drained the pool for 5 min, and the granted lease was routinely
orphaned because the processor exited for a rollout restart right after
spawning the daemon delete thread (no heartbeat, no release → 120 s
hold). Symptom: `Zenodo lock busy (held by None …)` for 30 min, then the
same `zenodo_lock:` timeout error. Fixed by independent classes +
orphan reaping + synchronous first release attempt in `zenodo_lock.py`
+ the processor joining `_BG_ZENODO_THREADS` at graceful shutdown.
`zenodo_lock:` errors in the log = a peer waited 30 min for nothing.

## Re-queueing already-processed KGs (the tombstone trap)

(Moved from `AGENTS.md`, 2026-10-13.)

`POST /api/v1/processing/queue {kgs:[…], position:0, skip_processed:false}`
is the **only** correct path to reprocess a KG that already has manifest
products. The dashboard's "Queue KG by name" box and `↻ re-queue all`
button call exactly this — there is no separate "forced" endpoint.
(`/processing/single` and `/processing/retry` just append the code; they
do **not** clear completion state, so the GET prune below re-drops it.)

**Why a re-queue can silently fail to stick.** `GET
/api/v1/processing/queue` (which `process.html` polls every few seconds)
is **not** read-only — it rewrites `retry_queue.json`, pruning any code
its coverage-oracle (`_kg_coverage_complete`) calls complete
(`_kg_coverage_complete`, section *KG geographic-coverage completion oracle*). A KG survives the prune **only if** it is
oracle-incomplete OR carries a live `<code>_requeue` tombstone. The
oracle judges completeness by `_json` **coverage of the parent bbox**,
ignoring missing GPKGs — so a **JSON-only KG (has `_json`, no
`_full_gpkg`/`_light_gpkg_v2`) reads as complete** and is pruned.

**The trap (cost us a confusing 274→254 drop, Jun 2026).** The POST has
an *idempotent re-push guard* (`processing_queue_add`): if a `<code>_requeue`
tombstone **already exists** and the KG is structurally complete, the
POST treats the call as a director re-push — it **drops** the tombstones
and **refuses to re-delete `_json`**. Net effect: the code is appended
to the queue, then pruned on the next dashboard GET. The first-time path
(no live `_requeue`) instead **deletes the `_json` manifest entry** +
stamps fresh tombstones → oracle now incomplete → KG sticks. So whether
a re-queue holds depends on **prior tombstone state**, not on which UI
you use.

**Recipe to reliably reprocess a JSON-only / coverage-complete KG:**
1. Ensure no stale live `<code>_requeue` tombstone exists (else you hit
   the idempotent guard and `_json` is never deleted). Drop it first via
   the satisfied-tombstone sweep (just re-POST once; if it's complete the
   guard removes the tombstone) **then** POST again, OR delete the
   `<code>_json` manifest entry directly so the oracle reads incomplete.
2. `POST …/processing/queue {kgs:[code], position:0, skip_processed:false}`.
3. Verify it survives a GET: `curl …/processing/queue` then re-read
   `retry_queue.json` — the code must still be present.

Diagnostic one-liner — list queued parents the oracle will prune (so you
know which re-queues won't hold without a manifest `_json` delete):
```bash
python3 -c "import json,app; me=json.load(open('data/austria_processor/zenodo_manifest.json')).get('entries',{}); q=json.load(open('data/austria_processor/retry_queue.json')); print([c for c in q if app._kg_coverage_complete(c,me)[0]])"
```

## Critical invariants (re-stated; see AGENTS.md)

- Don't probe Copernicus credentials from the request path. Pure cache reads only.
- All Zenodo writes go through `/zenodo/lock`.
- `_require_admin_token()` on every mutating endpoint.
- File-lock the director loop before starting a daemon thread that mutates global state.
- Async tasks must call `_check_aborted()` at progress checkpoints — otherwise abort is a lie.

---

*See `AGENTS.md` for the project map.*
