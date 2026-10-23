# BEV proxy pool — REMOVED 2026-10-23

The free-proxy pool described below was deleted for security (peers were
connecting to thousands of anonymous third-party hosts per hour, which
tripped a C2 network alert). `bev_proxy.py` is now a direct-only stub
with the same API; `bev_retry._PROXY_DISABLED = True`. The `fleet_proxy:`
line stays as `proxies=0`. See `docs/security-tokens.md → Why`.
**Do not reintroduce proxy lists.** Historical notes follow.

---

# BEV proxy pool (`bev_proxy.py`) and the `fleet_proxy:` line

> Moved verbatim out of `AGENTS.md` (2026-10-13). Load when touching `bev_proxy.py`, `bev_retry.py`, the proxy source lists, or when `/process.txt` shows `no_proxies=` / `stale=` on the `fleet_proxy:` line. BEV `/24` outage parks are a different mechanism: `docs/fleet-bandwidth.md → Role-based parks` #5 and `docs/peer-director.md → BEV outage handling`.

## BEV proxy pool (`bev_proxy.py`)

The processor reads BEV TIFFs via `raster_io` → `bev_retry.open()`,
which rotates through `bev_proxy.next_proxy()`. The pool is
**direct slots + free HTTP proxies** — a defensive fallback for when
BEV rate-limits a peer's IP. Direct egress is the primary path; proxies
buy us per-peer resilience when individual IPs get shaped (different
failure mode from a `/24`-wide BEV outage, which is handled by
`_check_bev_outage` pool-parking instead).

**No paid proxies.** Total throughput is ~5 GB/h per peer × ~50 peers,
far beyond any affordable paid plan. We live entirely off free
aggregator lists.

**Tiered source funnel** (2026-05-29, commit bda1a39):
* `_PROXY_SOURCES_VERIFIED` (1 entry: `elliottophellia/yakumo`) — a
  pre-checked, continuously-validated list. Live phase-1 yield ~22 %
  (vs ~0.04 % for raw aggregators). We validate **every** entry from
  these each refresh, no sampling.
* `_PROXY_SOURCES` (~30 raw aggregators, ~160k unique entries) —
  random-sampled to fill the remaining slots up to a 5000-entry
  validation budget per refresh tick.
* `_fetch_candidates()` returns `(verified, raw)`. `raw -= verified`
  keeps the two disjoint so budget isn't wasted re-testing the same
  entries.

**Two-phase validation** (`_refresh_pool`, every 30 min in a background
thread per process):
1. **Phase 1**: HTTPS CONNECT to `httpbin.org/ip` (fast, ~3s/proxy,
   ~80 workers). Filters out dead / non-HTTPS proxies cheaply.
2. **Phase 2**: range-request a real BEV BigTIFF and verify the TIFF
   magic bytes. Two random tiles per proxy (± layer / dataset / N×E)
   picked via `_random_bev_test_urls()` from the 55-tile × 2-layer ×
   3-dataset grid (330 distinct URLs). Spreads load so one BEV CDN
   object isn't hit 10 000× per refresh.

Proxies that pass go into `_pool` interleaved with `DIRECT_WEIGHT=3`
direct slots. `report_failure()` puts proxies on exponentially-
increasing cooldown (`BASE_COOLDOWN=300s` → cap `3d`); `report_success()`
decays fail_score by 0.5. State persists to `data/proxy_history.json`
across restarts.

**Steady-state pool size** (sampled on primary 2026-06-01, refresh
took 100 s):
```
verified: 1377 candidates, raw: 160 107 candidates
phase-2 survivors: 19 proxies + 3 direct slots = 22
```
Fleet-side: peers don't currently NEED proxies (BEV direct egress is
working), and the merged 48h log has **zero** `bev_proxy` warnings
across all peers. The pool exists as insurance.

## Fleet proxy view in `/process.txt`

Each peer ships a slim `proxy_pool` summary (`bev_proxy.summary()`, ~200 B)
on every `/api/v1/director/peer_status` push (full **and** slim heartbeat),
so the director can render an aggregate without any extra fanout traffic.
Only peers whose processor is actively `running` / `processing` are counted
(stopped peers carry stale pool state). Surfaces as a single line:

```
fleet_proxy: peers=N proxies(min/med/max)=X/Y/Z total=T \
             phase2_med=P refresh_age_med=Am [max=Bm] \
             [no_proxies=K] [stale=S] \
             hist[Hh n=M med_min=A med_med=B med_max=C peers_med=P]
```

Fields:
- `peers` — running peers reporting a `proxy_pool`
- `proxies(min/med/max)` — across the fleet's pools right now
- `total` — sum of healthy + cooling proxies across all peers
- `phase2_med` — median of each peer's last-refresh phase-2 survivor count
  (proxies that passed both HTTPS-CONNECT and BEV-magic validation)
- `refresh_age_med` / `max` — minutes since peers last refreshed their
  pool. `max` is only printed when > 60 min (well past the 30 min
  `REFRESH_INTERVAL`); a high value means at least one peer's refresh
  thread has hung.
- `no_proxies` — running peers operating with **zero** validated proxies
  (direct slots only). Fine while BEV direct egress is healthy; load-bearing
  signal when BEV starts shaping per-IP.
- `stale` — peers whose last refresh is > 90 min old (3× `REFRESH_INTERVAL`).
- `hist[...]` — 24h sliding-window strip from `fleet_proxy_history`
  (sampled every 10 min on the director-loop worker, persisted to
  `director_state.json` so the **other** gunicorn worker and HA shadow
  see it). 144 entries max. Lets a forensic check tell `"this is normal"`
  from `"the fleet's proxy pool has been collapsing for hours."`

Structured view in `/api/v1/director/status` under `fleet_proxy`
(same fields) + `fleet_proxy_history` (the ring as a list of compact
entries: `{t,n,tot,mn,md,mx,no,st}`).

**Cross-worker correctness**: only the director-loop worker (lock holder)
appends to the in-memory ring; the other worker reads it from disk via
`load_director_state` and surfaces the same history.

**Debug recipes**:
```bash
# Live pool state on primary's gunicorn worker:
curl -s http://localhost:8000/api/v1/info | jq .proxy_pool

# Force a refresh from a python shell (takes ~100s):
python3 -c 'import bev_proxy;bev_proxy._refresh_pool();print(bev_proxy.status())'

# Inspect a peer's processor-side bev_proxy log (via admin proxy):
curl -s 'http://localhost:8000/api/v1/director/proxy/log?peer_id=at87&lines=500' \
  | jq -r '.lines[]' | grep -E 'bev_proxy.*(Fetched|Pool refreshed|Phase|verified)'

# Validate a single proxy against BEV manually:
python3 -c 'import bev_proxy;print(bev_proxy._validate_proxy("1.2.3.4:8080"))'
```

**When to add more sources**: if `Fetched` lines show < 100k unique
candidates for several days, or if peers start logging `"No proxies
passed BEV validation"` AND we're seeing concurrent per-peer BEV
throttling (look for clustered `bev_pool_park` events in director
log), check whether yakumo-style continuous validators have replaced
the one we use. Live-test new candidates with `curl --proxy http://...
--connect-timeout 3 https://httpbin.org/ip` against 50 random samples
before adding — phase-1 yield must be ≥10 % to count as "verified";
else add to the raw funnel.

**Critical invariant**: the proxy validator must NEVER hammer a
single BEV CDN object. The randomised tile selection in
`_random_bev_test_urls()` is load-bearing — reverting to a fixed
two-URL test would mean 10 000 range-requests against the same files
per 30 min per peer, which BEV would absolutely rate-limit and which
would defeat the whole point of having proxies.

