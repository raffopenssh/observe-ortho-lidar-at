# Fleet bandwidth telemetry, canary parks and role-based parks

> Moved verbatim out of `AGENTS.md` (2026-10-13). Load when touching `_peer_bw_depleted`, `_sample_canary_history`, `_enforce_peer_bandwidth_walls`, `_check_steal_health`, `_check_bev_outage`, the `fleet_bw:` / `bw_learn:` / `throttle:` lines of `/process.txt`, or when a peer shows an unexpected `parked→Xd`. Related: `docs/peer-director.md` (cluster bandwidth accounting, BEV outage handling), `docs/bev-proxy.md`.

## Bandwidth fields in `/process.txt`

The text dashboard surfaces all the bandwidth telemetry needed to debug a
fleet that's canary-by-default (every peer sampled, slowdown auto-park
on throughput collapse, park-until-renewal on near/over budget):

* **Top-line `fleet_bw`** — `used_gb`, `budget_nominal_gb`, peers,
  parked, soonest `next_renew_in_days`, plus `wall~Xgb(min=Y,n=Z)`
  distilled from peers that auto-parked on throughput collapse.
* **Per-peer columns** — `bw%` (used/effective_budget), `used/bud`
  absolute GB, plus a free-form `bw_extras` tail with optional tokens:
  - `r=0.42` — canary recent/baseline throughput ratio (red <0.30)
  - `CANARY` — has an explicit `budget_gb` override
  - `cap=80G` — peer's persisted `observed_cap_gb` (it has hit a wall)
  - `parked→3.1d` / `parked→4.5h` — active `not_before` cooldown
  - `rd=15` — effective renew_day (override > **observed BW drop** >
    stored > first_seen day-of-month). Learned values are persisted
    as `renew_day_learned` in peers.json.
* **`fleet_bw` JSON block** in `/api/v1/director/status` for structured
  consumers — fields above plus `observed_cap_gb_min/median`,
  `peers_enabled`, `peers_parked`.
* **`bw_learn` line** (below `fleet_bw:`) — empirical billing-cycle
  learning: `peers_with_history`, `learned`, `by_renew_day=[d1=50 …]`
  histogram, plus the 5 most recent observed drops
  (`peerX@dN(prev→newGB)`). Structured at
  `/api/v1/director/status.bw_learn`. Detection: cumulative
  `used_bytes` only decreases at a cycle renewal, so a one-step diff
  in `_sample_canary_history` is enough; we persist just `bw_cycle =
  {peer: {last:[ts,used], events:[…]}}` (~few KB fleet-wide, **not** a
  30-day-of-samples ring — same persist-only-the-shape discipline as
  `fleet_proxy_history`). Throttled hourly per peer + hourly on the
  loop worker; `_bw_learn_summary` aggregate TTL-cached 5 min.

**Critical invariant — eligibility is evidence-only.** Every gate
that asks "does this peer have bandwidth headroom?" goes through
`_peer_bw_depleted(peer, bw, cfg)`. A peer is depleted iff it has an
`observed_cap_gb` AND `used_gb` is within headroom of that cap. Peers
without a measured cap are NEVER disqualified by eligibility — the
canary slowdown detector + `_enforce_peer_bandwidth_walls` will park
them when their real wall hits. Do **not** re-introduce
`_peer_budget_bytes(p,cfg) - used < 2 GB`-style gates: the nominal
budget (95/200/250 GB) is a guess and a per-peer vnstat cycle that
doesn't match `renew_day` (e.g. fleet rolled in on day 1, vnstat
still aggregating from day 17) will strand the peer for weeks. See
the Jun 2026 incident where 50 peers vanished from `cache_only_eligible`.

Thresholds (peer_director.py):
* `BANDWIDTH_LOW_WATER_GB = 4` — mid-KG triggers graceful stop +
  park-until-renewal so the in-flight KG finishes & uploads.
* `BANDWIDTH_HARD_DEPLETED_GB = 1` — hard stop, even mid-KG.
* `CANARY_SLOWDOWN_RATIO = 0.30`, baseline 30 min, 500 MB minimum bytes
  before a peer can be parked for throughput collapse ("soft" park).
* `CANARY_BASELINE_NETWORK_MBPS = 5.0` and `CANARY_RECENT_PARKED_MBPS
  = 0.5` — a soft park is upgraded to a **quality observation**
  (sets `observed_cap_gb` + counts toward fleet wall) only when the
  pre-collapse baseline was network-grade AND the peer truly stalled.
  Otherwise the peer was probably just Zenodo-upload-bound.
* `CANARY_QUALITY_PERSIST_S = 15 min` — the slowdown must be
  continuously observed for 15 min (streak resets above ratio 0.60)
  before the peer is parked at all (Jul 2026: previously the soft
  park fired at streak 0 s, which both wasted a 1 h park per
  transient blip and made the quality gate unsatisfiable — the peer
  was stopped before the streak could mature, so `observed_cap_gb`
  was never learned fleet-wide). During the window the peer keeps
  running (`canary observe <pid>` INFO line, once per streak); if
  the ratio recovers ≥0.60 the streak resets and zero peer-hours
  are lost. Only a slowdown that survives the full 15 min parks —
  quality-grade (network-grade + not fleet-wide) parks learn the
  wall, others soft-park. Streak is cleared on park so the peer
  gets a fresh observation window after the cooldown.
* `FLEET_CONCURRENT_SLOWDOWN_FRAC = 0.30` — if ≥0.30 of canary-eligible
  peers are simultaneously in slowdown, treat as a fleet-wide upstream
  event (BEV/Zenodo/internet hiccup); soft park still fires defensively
  but no quality observation is recorded for any peer that tick.
  Surfaces in `/process.txt` as `slowdown N/M [FLEET-WIDE]`.
* `FLEET_WALL_MIN_QUALITY_OBS = 5` — fleet wall (`observed_cap_gb_min/median`)
  is suppressed in `fleet_bw` until ≥5 distinct quality obs across
  the fleet. Until then `process.txt` shows `wall=? (gathering, N
  quality obs)` instead of inventing a confident wall.
* `CANARY_PARK_COOLDOWN_S = 6h` — quality-grade parks (real shaping).
* `CANARY_PARK_COOLDOWN_SOFT_S = 1h` — soft parks (defensive, not
  network-grade or not persistent). Almost always upstream blips
  (Zenodo / openEO / transient internet); the short cooldown lets the
  peer rejoin quickly. If the issue is real, the peer will soft-park
  again next tick and eventually graduate to quality-grade.
* Park-until-renewal cooldown = peer's effective `renew_day` next
  occurrence (no need to track per-peer budgets separately; the existing
  `_peer_is_scheduled` gate makes the scheduler skip the peer).

## Throttle / director-efficiency sparkline

The Service-card sparkline (`/process.html` → `renderCapacityHistory`)
plots the rolling `capacity_history` ring (240 ticks ≈ 2h). Each tick
carries:

* `f`   — EMA-smoothed capacity factor in `[THROTTLE_MIN_FACTOR, 1.0]`
* `bev` / `zen` / `cop` — fleet warnings/min per upstream
* `stl` — **fleet CPU-steal median** across *running* peers (same
  population the cache-only ramp brake uses)
* `cpu` — derived `cpu_factor` ramp brake: `1.0` below 30% steal,
  gentle linear ramp, floor `0.55` at 60%+ steal. Mirrors the damping
  curve in `_max_cache_only_peers` so the chart shows *why* the
  director may be running fewer cache-only peers than the warning-rate
  ceiling alone would allow.

Low capacity_factor with steal high → hypervisor pool congested
(reducing peers won't recover cycles; LPT partition does the real
balancing). Low capacity_factor with steal low → a real upstream
is pushing back — check `B / Z / C` chips for which one.

History persists to `director_state.json` every tick so it survives
HA handover and gunicorn's two-worker swap (just like the BEV / Zen /
Cop sub-EMAs). Schema is a 9-tuple (`t,f,bev,zen,cop,stl,cpu,fk,pk`);
the load path tolerates legacy 7-tuples (no `fk`/`pk`) and the
v1-marker 5-tuples written by pre-2026-05-19 directors. `fk`/`pk`
are **cumulative** KG-failure / partial-completion counters read
from `failed_kgs.json` and `partial_kgs.json` (both already
replicated to shadow via `director_ha.SNAPSHOT_FILES`), so the rate
per hour is derived as `(newest - oldest)/window` over any
sub-window of the 2 h ring.

`/process.txt` carries a `throttle:` block (window / cap_factor /
steal_med / cpu_factor / per-upstream warns-per-min, min/med/max over
the window) so forensic mining over the long-term archive can
correlate director efficiency with steal trends without parsing JSON.
Line 2 of the throttle block adds a `kg_outcome:` field with
cumulative + per-hour failed/partial KG rates so a future BEV
outage's downstream impact is visible inline. The Service-card
sparkline shows the same data: `F x.xx/h` / `P x.xx/h` chips and
red/yellow vertical tick marks on the chart at every tick where the
cumulative counter increased.

## Role-based parks (who is parked, and why)

Only five reasons a peer is parked (`not_before` in the future):

1. **Primary** — `_enforce_primary_park`, every tick. Floor
   `not_before=2027-01-01`, `pinned_role=idle`. Manual reset won't
   survive (next tick re-extends). Primary hosts the public
   dashboard and director state; it must never process.
2. **Active director + shadow** — `_enforce_director_self_park`,
   every tick. **Rolling 2 h cooldown**, refreshed while the role
   is held. As soon as the peer stops being director/shadow, the
   stamp expires within 2 h and the peer rejoins rotation — no
   explicit release needed. (Primary is exempt; covered by #1.)
3. **Canary-evidenced BW wall** — `_park_peer_until_renewal`,
   only when the peer has an `observed_cap_gb` set by a quality-
   grade canary slowdown AND used_gb has reached that cap. We do
   NOT park on the nominal 95 GB budget; exe.dev's real limits and
   billing anchors are unknown. Soft canary auto-parks use a short
   1 h cooldown (`auto_park` event); quality parks 6 h.
4. **Rolling-steal park** — `_check_steal_health`, every tick.
   30 min cooldown (`steal_park` event) when the per-peer
   `peer_steal_ema` (~15 min half-life) sits ≥ `STEAL_PARK_THRESHOLD_PCT`
   (70 %%) for ≥ `STEAL_PARK_PERSIST_S` (10 min). Streak resets when
   the EMA falls below `STEAL_PARK_RECOVERY_PCT` (50 %%). Goal: stop
   burning Copernicus credits / KG wall-time on peers stuck on a
   structurally over-subscribed hypervisor host. exe.dev sometimes
   re-places idle VMs onto quieter hosts; either way, 30 min off
   is cheap. Frontier scheduling additionally biases against
   high-steal peers via `FRONTIER_HIGH_STEAL_BIAS_PCT` (50 %%) so
   credentials always land on the lowest-steal eligible peer.
5. **BEV egress-pool park** — `_check_bev_outage` (since 2026-05-28,
   commit 8ce44cd). When fleet BEV warns are sustained AND the
   warning peers cluster into ≤ `OUTAGE_POOL_MAX_SET` (default 3)
   /24 egress pools whose combined share ≥ `OUTAGE_POOL_DOMINANCE`
   (70 %%), the director parks every peer in those /24s with
   escalating 1h/4h/12h/24h cooldowns (`bev_pool_park` event,
   level tracked per pool in `bev_pool_escalation`). Peers in
   healthy pools keep working — no fleet-wide pause. Root cause is
   per-egress reachability: AWS/cloud pools like `109.94.96.0/24`
   and `162.43.189.0/24` can have TCP timeouts to `data.bev.gv.at`
   while other pools (and the primary itself) reach it in <1s.
   Fleet-scope pause is the fallback when attribution fails (>3
   bad pools or `known_frac < OUTAGE_POOL_KNOWN_FRAC = 0.60`).
   **Canary unpark probes**: every 5 min the director polls one
   peer in each parked /24 via its `/api/v1/bev_probe` endpoint
   (single range request to `_BEV_TEST_URL` + TIFF magic check).
   On success it clears `not_before` on every `bev_pool_park`-tagged
   peer in that /24 early so a 20-min route flap doesn't waste the
   full cooldown. Event: `bev_pool_unpark` in `canary_notes`.
   See `docs/peer-director.md → BEV outage handling` for details.

The one-shot `_release_unverified_bw_parks` rescues peers that got
parked-until-renewal *without* an `observed_cap_gb` (legacy budget-
guess parks). It looks for our own `park_until_renewal` note tag,
so primary's 2027 stamp and canary `auto_park` cooldowns are
preserved.

**Operator note**: if you see an unexpected long `parked→Xd` on a
peer, check `peers.json` → `canary_notes` for the most recent
`event`. `role_park`/`park_until_renewal` are director-written;
`auto_park` is canary-written; anything else is manual.

