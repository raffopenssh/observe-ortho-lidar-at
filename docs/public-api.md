# Public API — `public_api.py` (`:8100`)

The public tier is a separate gunicorn app (`srv_public.service`,
`public_api:app`, bind `0.0.0.0:${SRTM_PUBLIC_PORT}` = 8100). It never
imports `app.py`; it reads the product corpus on disk (`data/ne/*.h3b`,
`data/kg_v2_store.db`, `data/search_index.db`) and relays an explicit
allow-list of analysis routes to the operator API on
`config.APP_URL` (`http://127.0.0.1:8000`). The machine-readable
contract is `GET /api/v1/docs/llm.txt` (`public_docs.LLM_TXT`); keep it
in sync with every change here.

## Route families

| family | routes | module |
|---|---|---|
| health / meta | `/api/v1/health` (store-free, 503 once the memory watchdog flagged), `/api/v1/status[?guard=1]`, `/api/v1/license`, `/`, `/llm.txt` | `public_api.py` |
| KG summaries | `/api/v1/kgs`, `/api/v1/kg/<code>`, `/api/v1/kgs/summary`, `/llm/kg/<code>.json`, `/llm/manifest.json` | `public_api.py`, `cells_api.py` |
| landscape | `/api/v1/landscape`, `/api/v1/context`, `/api/v1/heightfield`, `/api/v1/trees/bbox`, `/api/v1/landmarks/bbox`, `/api/v1/buildings/bbox` | `landscape_query.py` |
| NE cells | `/api/v1/cells[/<h>]`, `/cells/chunk/<h>`, `/cells/dict`, `/cells/bins`, `/cells/national`, `/kg/<code>/cells` | `cells_api.py` (`docs/ne-public-api.md`) |
| stats | `/api/v1/stats/unit` | `stats_unit.py` |
| flags | `/api/v1/flags*` | `public_flags.py` (`docs/public-flags.md`) |
| raster tiles | `/tiles/{hillshade,slope,ndsm}/z/x/y.png` | `hillshade_tiles.py` |
| segmentation UI + relay | `/segmentation`, plus the allow-list in `public_segmentation.ROUTES` (`/api/v1/segment*`, `/elevation`, `/terrain`, overlays, exports, `/parse-geometry`, `/layers`, `/attribution`, shares) relayed to app.py with the area cap `PUBLIC_MAX_AREA_KM` (5 km²) | `public_segmentation.py` |

Everything not listed is 404 on this tier — extend `ROUTES`, never add a
catch-all. Responses ≥ 1 KiB are gzip'd with a strong ETag (304 on
`If-None-Match`).

## Resource limits (the public tier must survive a crawler)

* **Byte-bounded caches** (`mem_lru.ByteLRU`, per worker; never
  entry-count caches): `HILLSHADE_GRID_MB` 96, `LQ_GRID_MB` 128,
  `TILE_PNG_MB` 32, `CELLS_HEADER_MB` 24, `CELLS_CHUNK_MB` 64.
* **Tile PNG disk cache** `data/tile_cache/` keyed
  `kind/z/x/y/<stamp>` (stamp = blake2b of the parents' `ingested_at`),
  `TILE_DISK_CACHE_MB` 2048, `TILE_DISK_CACHE_DIR`.
* **Concurrency gate** `service_guard.Gate` — `TILE_MAX_CONCURRENT` 3
  renders at once, 3 s queue wait → 503 + `Retry-After: 2`. Any request
  that costs > 50 ms CPU goes behind a Gate and raises
  `service_guard.Overloaded` instead of queueing.
* **Rate limiter** `service_guard.RateLimiter` (per-client bucket on
  cache misses): `TILE_RATE_PER_S` 25, burst 150 → 429 + `X-RateLimit-*`.
  Flags: `FLAGS_POST_RATE_PER_S[_BURST]`, `FLAGS_GET_RATE_PER_S[_BURST]`.
* **Single-flight** render per tile key; identical concurrent requests
  share one computation.
* **Memory watchdog** `service_guard.MemoryWatchdog`:
  `PUBLIC_RSS_SOFT_MB` 850 → shed caches + GC; `PUBLIC_RSS_HARD_MB` 1100
  → the worker SIGTERMs itself (gunicorn respawns). Unit:
  `MemoryHigh=2400M` / `MemoryMax=2800M` (2 × hard < MemoryHigh),
  `--max-requests 20000 --max-requests-jitter 2000`.
* **External watchdog** `srv-watchdog.timer` probes
  `:8000/api/v1/ping` and `:8100/api/v1/health`; 3 strikes → restart the
  unit, at most once per 10 min.

Triage:

```bash
curl -s localhost:8100/api/v1/health
curl -s 'localhost:8100/api/v1/status?guard=1' | python3 -m json.tool | head -40
cat /sys/fs/cgroup/system.slice/srv_public.service/memory.{current,high,events}
ss -ltn | grep 8100
```

## Rules

* Public routes never read Zenodo or BEV/Copernicus upstreams at request
  time; the only upstream calls happen through the relay to app.py
  (which applies the same allow-list, `PUBLIC_MAX_AREA_KM` and its own
  task semaphore).
* Subjects are H3 cells or KG codes — nothing in the public tier is keyed
  by any property or ownership unit (`tests/test_cells_vocab.py`).
* New public endpoints: add to `public_docs.LLM_TXT` and to the family
  table above; relayed routes also to `public_segmentation.ROUTES` and
  `tests/test_public_segmentation.py`.
