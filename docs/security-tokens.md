# Cluster security model (2026-10-23 hardening)

> Load when touching `peer_tokens.py`, `_enforce_admin_token` / `install_token`
> in `app.py`, `director_ha.py`, `deploy.sh`, the "+ Add Peer" modal, or when
> `/process.txt?q=security` shows `master-token-off-channel` / `peer-scope-deny`.

## Why

2026-10-23: a network-security alert flagged a peer talking to a known
C2 network. Root cause was **`bev_proxy.py`**: every peer scraped ~30
public free-proxy lists and validated up to 5 000 random third-party
hosts every 30 min (HTTPS CONNECT + BEV range reads through them). Free
open proxies are largely compromised machines / botnet nodes → the
fleet looked like a bot. The pool was never needed for throughput and is
**gone** (`bev_proxy.py` is a direct-only stub; `bev_retry._PROXY_DISABLED
= True`). **Never reintroduce anonymous third-party egress.**

The incident also exposed the real design flaw: one shared
`data/admin_token` on every VM — any compromised peer could stop/start/
update/re-point the whole fleet and read the director snapshot
(Copernicus credentials, roster). This file documents the replacement.

## Token classes (`peer_tokens.classify`)

| class | lives in | who presents it | what it unlocks |
|---|---|---|---|
| **master** | `data/admin_token` on the **primary** only | owner (dashboard on `:8000`, on-box CLI) | everything — but **only on the owner channel** (below) |
| **peer** | `data/admin_token` on each peer (unique per host); map in `data/peer_tokens.json` on the primary (0600, gitignored, never in a snapshot) | that peer → primary; primary/director → that peer | on the primary: `peer_tokens.PEER_SCOPE` allow-list only (status push, Zenodo lock, cache-manifest sync, kg_strikes, products, `ne/declared`, self-registration). On the peer itself: full rights (it *is* the director's handle for that VM) |
| **mesh** | `data/mesh_token` on every VM (fleet-shared) | peer → peer | `MESH_SCOPE`: `GET/PUT /api/v1/processing/cache_manifest` only |

Everything else → `401`/`403` plus a rate-limited **security event** in the
director log (`/process.txt?q=security`): `master-token-off-channel`,
`peer-scope-deny <peer> <METHOD> <path>`, `mesh-scope-deny`. If a
legitimate peer→primary call is missing from `PEER_SCOPE` it shows up
there within one tick — add it to the allow-list, never widen the class.

### Owner channel (`app._request_is_owner_channel`)

The master is accepted only from loopback or from requests that arrived
on the **private `:8000` exe.dev port** (`X-Forwarded-Host` ends in
`:8000`, not the `/k` tier) **and** carry a proxy-asserted
`X-ExeDev-Email` listed in `data/owner_emails`. The peers live in a
different exe.dev account and reach the primary only via the public
`/k` tier, so a master token seen on `/k` is by definition stolen →
401 + event. Net effect: the master is useless without *also* being
logged into the owner's exe.dev account (two independent factors).

### Outbound rewriting (`peer_tokens.install_outbound_hook`)

`requests.Session.request` is patched at import (app.py, austria_processor
main) so every outbound call carrying `X-Admin-Token` gets the right
token for its destination host: primary → peer = that peer's token
(unmapped peer → the recorded legacy master during migration, else the
header is **stripped**); peer → director = own token; peer → peer =
mesh. No call site had to change; the master physically cannot leave
the primary. `update_peers` no longer pushes the master
(`install_token_on_peer` only re-seeds the *mapped* peer token).

## HA / director takeover: OFF

`director_ha.ha_enabled()` is false unless `data/ha_enabled` exists on
the primary (a file, deliberately not an API knob). While off:

* watchdog never promotes; shadow election cleared, no snapshot / log
  archive / combined-log pushes leave the primary;
* `/director/{takeover,announce,step_down,handover}` refuse
  (`ha_disabled`), `snapshot` PUT/`log_archive` PUT/`combined_log_delta`
  → 403, `snapshot` GET only from loopback;
* peers accept `director_url` (identity POST, announce,
  `set_director_url`) only when its host is `director_ha.PRIMARY_HOST`.

So **no token of any class can move the director role** — the only way
is the owner creating `data/ha_enabled` on the primary.

## Rotation — never through a channel the attacker may own

```bash
python3 peer_tokens.py --status            # mapped / unmapped peers
python3 peer_tokens.py --rotate            # every enabled peer → unique token, then the master
python3 peer_tokens.py --rotate --peer at12 --no-master   # one peer (e.g. after re-image)
python3 peer_tokens.py --purge-legacy      # once every peer is mapped: forget the old shared token
```

`--rotate` pushes each peer's *new* token to *that peer only*
(`POST /api/v1/admin/install_token`, authenticated with the token that
peer currently holds — the old shared one on first run, its own one
afterwards — plus the mesh token in the body), records it in
`peer_tokens.json` on success, and rotates the master last, printing
the new owner token once (paste into the dashboard prompt /
`srtmResetAdminToken()`). A peer that fails rotation keeps the old
token and can't reach the primary any more (master already rotated) →
re-run `--rotate --peer <id>` (uses the recorded `_legacy_master`) or
re-image it. Rotation is idempotent and safe to repeat.

**After rotating or after any change to `api_tiers.py` / `peer_tokens.py`:
`sudo systemctl restart srv_public` too.** The `/k` gate is a separate
gunicorn (`public_api`); on 2026-10-23 it kept running the pre-rotation
code and 404'd every peer→primary call for 35 min (status pushes, Zenodo
lock, leases) while the director's own peer polls made the fleet look
healthy — the only symptom was `EDGE-INBOUND-UNREACHABLE` in the banner.

## New peers

* Dashboard "+ Add Peer": `POST /director/peers/mint_token {vm}` (owner
  channel only) mints the VM's token + returns the mesh token; the deploy
  command bakes `ADMIN_TOKEN=<peer token> MESH_TOKEN=<mesh>` — **never
  the master**. `deploy.sh` self-registers with that token
  (`peers/add` is in `PEER_SCOPE` but a peer token may only register
  *its own* host). `peers/add` then (re)installs the mapped token
  (first-write-wins bootstrap for token-less peers; `token` body field
  = the peer's current token if it differs).
* CLI: `python3 peer_tokens.py --new-peer at77` prints the same env.

## Files (all gitignored, mode 0600)

`data/admin_token` (master on primary / own token on a peer),
`data/peer_tokens.json` (primary), `data/mesh_token` (all),
`data/owner_emails` (primary; one exe.dev login per line),
`data/ha_enabled` (primary; absent = HA off).

## Provider secrets (`fleet_secrets.py`, since 2026-10-23)

Zenodo / umfeld / GitHub tokens live **only** in the primary's
`data/secrets.json` (0600, gitignored, `{rev, zenodo, umfeld, github}`).
Peers lease the bundle with their own peer token from
`GET /api/v1/secrets/lease`, hold it in process memory, and scrub any
legacy `data/umfeld_token` / `data/github_token` after the first lease.
Re-lease is triggered by the `secrets_rev` field in the status-push ack
(no polling) or by one provider 401 (rate-limited).

* Rotate: rotate at the provider → `python3 fleet_secrets.py --set zenodo`
  (value on stdin) → fleet converges within one push interval (~30 s).
* Audit: `python3 fleet_secrets.py --leases` (peer → rev, ts, ip),
  `--deny <peer>` / `--allow <peer>` (`data/secrets_deny`),
  `/process.txt?q=secret-lease` for events (`secret-lease-refused` = denied
  or unknown peer).
* `/process.txt` `secrets:` line: `rev=N leases_at_rev=a/b
  github_sha_ok=x/n umfeld_sha_ok=y/n denied=d legacy_file=no`. The
  `*_sha_ok` counts compare the sha each peer reports in its status push
  with the store value — equal ⇒ the peer runs on the leased value, not a
  stale disk copy; `github_stale=…` / `umfeld_stale=…` list offenders.
  `legacy_file=yes` means the primary still has a bootstrap
  `data/github_token` (only needed while peers run pre-lease code —
  delete it once `leases_at_rev` is full).
* Convergence verified 2026-10-23 21:00 UTC: 51/51 leases at rev 4, 0
  refusals, interim bootstrap PAT removed from the primary.

## Remaining trust anchors (out of scope here)

* Code: peers `git reset --hard origin/main` on update — GitHub repo
  access = fleet code execution. Keep the repo private; the read-only
  PAT is leased (above) and rotated with `--set github`.
* Zenodo token / Copernicus credentials / umfeld token are still
  fleet-shared (peers need them to work). A compromised peer can spend
  Copernicus credits and PUT garbage into Zenodo buckets; rotate those
  at the provider (+ `--set`) if a peer is confirmed compromised. Zenodo
  has no scoped tokens — `safe_load.py` is the blast-radius limit for
  poisoned artefacts.
* Peer `:8000` ports are public (other exe.dev account) — only the
  per-peer token guards them; blast radius of a leaked peer token = that
  VM.

Tests: `python3 tests/test_peer_tokens.py`.
