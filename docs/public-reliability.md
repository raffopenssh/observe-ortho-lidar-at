# Public-tier reliability (`srv_public`, :8100 → https://example.invalid/)

## Post-mortem — 2026-10-04 "Safari kann die Seite nicht öffnen"

**Symptom.** The public host answered nothing for hours. `ss -ltn` showed
600+ connections in the :8100 accept backlog; `srv_public` was `active`,
both gunicorn workers alive but in **D state** at ~9 % CPU. :8000 was fine.

**Cause.** Both workers had grown to 1.33 GB anon (`memory.current`), above
the unit's `MemoryHigh=1200M`. cgroup v2 then *reclaim-throttles* the
cgroup (`memory.events high=765779`, `memory.pressure some avg10=99 %`):
every allocation stalls, so each request took seconds→minutes and the
backlog filled. Nothing crashed, so `Restart=on-failure` never fired and
the old `srv-watchdog` only probed :8000.

Why 1.33 GB: entry-count caches with wrong size assumptions.
`hillshade_tiles._grid_cache` was capped at 600 entries "≈65 KB each"; real
decoded grid25 blocks are ~320 KB–1.4 MB *per parent* (split KGs hold
several), so that cache alone was ~200 MB per worker, `landscape_query`'s
grid cache similar, plus 24 threads of in-flight numpy temporaries. A tile
consumer (siedler) hitting `/tiles/hillshade` across Austria warmed them
all.

## What now protects it (all per worker, env-tunable in `srv_public.service`)

| Layer | Where | Knob (default) |
|---|---|---|
| Byte-bounded LRUs (`mem_lru.ByteLRU`) for grid25 blocks, PNGs, cells headers/chunks | `hillshade_tiles`, `landscape_query`, `cells_api` | `HILLSHADE_GRID_MB` 96 · `LQ_GRID_MB` 128 · `TILE_PNG_MB` 32 · `CELLS_HEADER_MB` 24 · `CELLS_CHUNK_MB` 64 |
| Tile PNG disk cache, keyed `kind/z/x/y/<stamp>` where stamp = blake2b of the intersecting parents' latest `ingested_at` (`kg_v2_store.parents_stamp`) → a product re-ingest invalidates exactly its tiles; janitor trims oldest-mtime past budget | `data/tile_cache/` | `TILE_DISK_CACHE_MB` 2048, `TILE_DISK_CACHE_DIR` |
| Render concurrency `Gate` — ≤N renders at once, ≤3 s queue wait, else **503 + Retry-After: 2** | `service_guard.Gate` | `TILE_MAX_CONCURRENT` 3, `_WAIT_S` 3 |
| Per-client token bucket on cache **misses** only → **429 + Retry-After + X-RateLimit-*** | `service_guard.RateLimiter`, key = first `X-Forwarded-For` hop | `TILE_RATE_PER_S` 25, `_BURST` 150 |
| Single-flight render per tile key (N clients, one render) | `hillshade_tiles._render_cached` | — |
| Strong ETag on tiles, `If-None-Match` → 304 | | — |
| RSS watchdog sampled from the request path (≤1/5 s): soft → shed all `mem_lru` caches + GC; hard → SIGTERM self (gunicorn drains + respawns) | `service_guard.MemoryWatchdog` | `PUBLIC_RSS_SOFT_MB` 850, `PUBLIC_RSS_HARD_MB` 1100 |
| Worker recycling | gunicorn `--max-requests 20000 --max-requests-jitter 2000` | |
| cgroup sized so 2 × hard < MemoryHigh — throttling cannot start before a worker recycles | unit | `MemoryHigh=2400M MemoryMax=2800M` (srv lowered to 3800/4200M — :8000 is fleet-only now) |
| External liveness: `srv-watchdog.timer` (1 min) probes `:8000/api/v1/ping` **and** `:8100/api/v1/health`; 3 strikes (≥8 s or non-200) → `systemctl restart`, ≥10 min between restarts | `srv_watchdog.sh` | |

`/api/v1/health` is deliberately store-free (RSS + cache bytes only) and
returns 503 once the watchdog has flagged the worker. `/api/v1/status?guard=1`
exposes the whole picture for *the worker that answered* (there are two):
`watchdog`, `cgroup` (current/high/max + PSI), `caches`, `tiles.{gate,
rate_limit, png_mem, png_disk}`.

## Triage

```bash
curl -s localhost:8100/api/v1/health                              # <5 ms or it's wedged
curl -s 'localhost:8100/api/v1/status?guard=1' | jq .guard         # per-worker
cat /sys/fs/cgroup/system.slice/srv_public.service/memory.{current,high,events,pressure}
journalctl -u srv_public --since -1h | grep -E 'memory watchdog|overloaded|trimmed'
journalctl -t srv-watchdog --since -1d
sudo ss -ltn | grep 8100                                           # Recv-Q = accept backlog; >50 is bad
```

`memory.pressure some avg10` > 10 % with `memory.current` near `high` is the
throttle signature. If it ever recurs: a cache is unbounded or
mis-sized — `caches_total_mb` in `?guard=1` tells you which; fix the sizer,
don't just raise the limits.

## Rules for adding to `public_api.py`

* No entry-count caches. Use `mem_lru.ByteLRU(name, max_mb, sizer=…, env=…)`
  so the watchdog can shed it and `?guard=1` can see it.
* Anything that burns > ~50 ms CPU per request goes behind a
  `service_guard.Gate` and raises `Overloaded` (the app-level handler turns
  it into 503 + Retry-After).
* Document the limits in `public_docs.LLM_TXT` — clients can only behave if
  they know the rules (siedler reads it).
