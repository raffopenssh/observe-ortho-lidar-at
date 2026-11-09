# Director — `peer_director.py`

The director is a thread inside `app.py` (`srv.service`) that decides
*what the processor works on next* and keeps it running. On a single
machine (`SRTM_PEERS` empty → `config.single_vm()`) it drives the local
`austria_processor.service`; with `SRTM_PEERS=http://host2:8000,…` the
same loop orchestrates the processors of several machines over their
operator APIs (`X-Operator-Token`, `SRTM_OPERATOR_TOKEN` shared).

## Loop (every 30 s, `PeerDirector.tick`)

1. Single-flight: `fcntl` lock on `data/austria_processor/director.lock`;
   the winning gunicorn worker writes the `is_director` flag file and
   runs the loop, the other worker serves requests only.
2. Re-read `peers.json` (`SRTM_PEERS` rows are merged in, never removed —
   disabling a peer is an operator action, `DELETE /director/peers/<id>`).
3. Poll every peer's `/api/v1/processing/status`; restart a stopped
   processor (`POST …/processing/start`) in mode `auto`.
4. Compute the work plan and PUT it as the peer's queue slice
   (`PUT /api/v1/processing/queue`): fresh KGs for the **frontier**
   (the one processor with Copernicus credentials — several only if
   their credentials are disjoint), the **cache-ready whitelist**
   (`_compute_cache_ready_kgs`: KGs whose every 0.1° cell is in the tile
   cache per `cache_manifest` / `tile_cache.is_kg_fully_cached`) for
   cache-only workers, then upgrade units (`_v2_upgrade_fill`) and sweep
   units (`sliver_sweep.assign`) when fresh work runs low.
5. Audits (own thread, 5-min cadence): `ne_rollout`, `label_mix`,
   `product_repair`, `sliver_sweep.scan` / `integrate`, stuck-KG watchdog
   (a KG whose progress fingerprint is frozen for 12 h → hard restart of
   the processor), duplicate-KG reconciler (one owner per code across
   peers).
6. Credential revalidation (`_refresh_credentials_if_due`) — director
   only, never from a request path.
7. Save `director_state.json`.

Completion guard (2026-11-07): a parent is dropped from the whitelist when
(a) it is in `_get_completed_kgs()` (plain `_json`), (b) every block of its
*current* split layout is completed, or (c) the search index says
`processed=1` **and** the geometric coverage oracle
(`app._kg_coverage_complete` via `_parent_geometrically_done`) confirms
its committed products cover the bbox. The old guard used `processed=1`
alone, but the index flips that as soon as *one* block JSON lands — 74
half-done split parents (138 fully-cached blocks) were invisible to the
cache-only pool and crawled through the frontiers at ~3.6/h. Half-done
parents now pass through; the block-level exclusion in the dispatcher keeps
their finished siblings out. `_frontier_cells()` applies the same oracle so
layout-drift parents (91123 done as 6 blocks, today's plan 8) no longer make
a cell look pending and restart-loop a frontier into quarantine. Log line:
`Cache-ready scan: N processed=1 parent(s) have a coverage hole — kept on
the whitelist`.

Modes (`POST /director/mode {mode}`): `auto` (default), `manual` (no
automatic start / plan), `paused`.

Single-VM specifics: the local instance is both director and the only
peer (`peers.json` row `{id: "primary", url: null}`); the Zenodo mutex
(`/api/v1/zenodo/lock`, TTL 120 s, heartbeat 30 s) is served locally;
multi-VM setups point every peer at one broker with `ZENODO_LOCK_URL`
(fail-open when unreachable, uploads then rely on the per-key manifest
merge).

## Files (`data/austria_processor/`)

`peers.json` (`peers[]: {id, url, enabled, role: frontier|cache_only,
min_reserve_peers, max_cache_only_peers}`), `director_state.json`,
`director_config.json` (knobs below), `director.lock`, `is_director`,
`self.json`.

## API (`app.py`, operator gate; GETs marked ◦ are open to the dashboard)

| route | |
|---|---|
| ◦ `GET /api/v1/director/status` | plan, peers, audits (`v2`, `repair`, `sweep`, `label_mix`), events |
| ◦ `GET /director/log/history?hours=` | merged director + peer events (`?q=` filter) |
| `POST /director/mode` / `…/activate` / `…/stop` | mode; activate = start the plan now; stop = stop every processor |
| ◦ `GET` / `POST /director/peers`, `POST …/peers/add`, `DELETE …/peers/<id>` | roster |
| `POST …/peers/<id>/cmd/<start\|stop\|pause\|resume>` | forward a lifecycle command |
| `POST …/peers/<id>/cooldown` / `…/pin` / `…/release_hold` | scheduling overrides |
| ◦ `GET` / `POST /director/config` | knobs: `ne_ingest_enabled`, `sliver_sweep`, `sweep_*`, `v24_first`, `vct_pilot_auto`, … |
| ◦ `GET` / `POST` / `DELETE /director/v2/priority` | pin codes to the head of the upgrade list |
| ◦ `GET` / `POST /director/throttle` | Copernicus throttle (`docs/copernicus-throttle.md`) |
| ◦ `GET /director/barren_cells`, `POST …/clear` | 0.1° cells with no usable upstream data |
| ◦ `GET /director/sweep[?kg=]`, `POST …/sweep` | `docs/sliver-sweep.md` |
| `POST /director/restart_peer` | hard restart of a peer's processor |
| `GET` / `POST /director/reboot_slot` | fleet-wide graceful host-reboot leases (`docs/host-maintenance.md`) |
| `POST /api/v1/admin/restart_processor` / `…/disable_autostart` | local processor control |

```bash
curl -s localhost:8000/api/v1/director/status | python3 -m json.tool | head -40
curl -s 'localhost:8000/process.txt?log=100'                     # text dashboard
curl -s -XPOST -H 'Content-Type: application/json' -d '{"mode":"paused"}' localhost:8000/api/v1/director/mode
curl -s -XPOST -H 'Content-Type: application/json' -d '{"ne_ingest_enabled":false}' localhost:8000/api/v1/director/config
```

## Invariants

* Exactly one director per deployment (`director.lock` + `is_director`);
  a second machine running `app.py` with `SRTM_PRIMARY_URL` set is a
  peer, not a director.
* The processor runs as the service user, never root.
* Every peer call goes through `http_pool` with the operator header; the
  director never ships secrets to peers (each machine has its own
  `SRTM_SECRETS_DIR`).
* Zenodo writes from any machine are serialised through the lock
  (`docs/zenodo-cache.md`).

* **The primary is absent from every `peer_urls.txt`**, so peers never pull the primary's
  manifest. Primary-originated edits ride the 5-min `cache_manifest` delta PUT: the tombstone
  drop journal and the **product-status rider** `product_status = {key: [status, uploaded_at,
  size]}` for every `removed` / `stripped` record (`app._product_status_rider` → peer
  `_apply_product_status`, delta per peer by `uploaded_at`, ~124 KB gz full set every 6 h).
* **Completed-claim hygiene** (`app._get_completed_kgs`, processor `completed_codes`): a
  `status=removed`/size-0 `_json` is not a product; a local `json/<code>.json` without a
  committed record counts only for `LOCAL_JSON_ORPHAN_GRACE_S` (24 h) — an older orphan is a
  failed upload that was re-queued and must not be advertised to peers as completed.

## Endgame triage — last blocks of the run (Nov 2026)

When `health:` says ~100 % and only a handful of split blocks are left, the
whole fleet collapses onto **one planning cell**. Every mechanism that was
tuned for 7 850 KGs now has a single-point-of-failure flavour. Cheapest
reading order (≈3 curls):

```bash
curl -s "http://localhost:8000/process.txt?hidden=1&log=0"           # roster incl. STOPPED/COMPLETE; `cells:` work=/quarantined=
curl -s "http://localhost:8000/process.txt?q=<block>&hours=12&log=30&roster=0'   # who touched the block, where it died
python3 - <<'PY'   # which blocks of the parent are actually committed
import json; m=json.load(open('data/austria_processor/zenodo_manifest.json'))['entries']
print(sorted(k for k in m if k.startswith('81206')))
PY
```

Symptoms and what they really mean:

| Seen | Meaning | Action |
|---|---|---|
| Fresh frontiers log `frontier start → … ×6` then `no-work restart loop … quarantined for 6h`, `cells: quarantined=<the only cell with work>` | Everything left in the cell is *claimed*. Either genuinely running on other peers (fine — the newcomers have nothing to do) **or** a stale claim (next row). | Roster check: is every remaining block on a RUN/FRONTIER row? If yes, nothing to do. |
| A peer is `STOPPED` but its row still shows a `kg`, and nobody else runs that block | **Stale claim.** `progress.json` kept `current_kg` after SIGTERM / a silent death (OOM, hard update mid-tile). Since fc5c10c `/api/v1/processing/peers` only reports `current` / `in_progress` while `state ∈ running/processing/paused*` (`processor_state` is in the reply), so this no longer blocks others. | Director restarts the stopped peer on its own; if the cell is quarantined, release it: `POST /api/v1/director/barren_cells/clear` (owner token). |
| Peer log ends mid-step with no error, `processor_state=stopped`, `Received SIGTERM — will finish current KG then exit` 10–15 min earlier | Graceful update escalated / process killed before the KG boundary. Tile checkpoints survive (`tile N restored from checkpoint`, chkpt registry) → the retry is cheap. | Nothing — the block is re-picked by the next idle peer in the cell. |
| `copernicus: NDVI <month>: month too heavy … skipping month`, `copernicus (2700s) completed with warnings`, `ndvi_harmonics: No monthly NDVI data retrieved` | openEO degraded; harmonics come out partial. The product still uploads (`-partial` label) and lands in `repair: pending[harmonics]` → re-upgraded later when cells are cached. | Nothing. Don't touch timeouts (`docs/copernicus-throttle.md`). |
| Same block `Starting KG` on 2–3 peers within one minute | Dispatch-claim race (150 s window). The dup-KG reconciler keeps the furthest-along peer. | Nothing; `?q=dup-kg` to audit. |

Do **not** hand-start processors or `update_peers` across the fleet to
"unstick" the endgame — one stale claim + one quarantined cell is the
whole story 9 times out of 10, and both self-heal or take two POSTs.
