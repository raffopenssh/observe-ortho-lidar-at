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
