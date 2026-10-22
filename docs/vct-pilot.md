# VTC pilot — vtcseamless for the cadastre, one frontier at a time

Status 2026-10-06: code landed; enrol with `vct_pilot_auto` / `vct_pilot_peers`.
Modules: `vct_pilot.py` (peer), `vct_fleet.py` (director/primary),
hooks in `austria_processor.py` (step 1c), `app.py` (`/api/v1/admin/vct_pilot`,
start payload, status push, `/process.txt`), `peer_director.py` (knobs, start
payload, 5-min tick), `ne_h3.py` (bevdirect-serve launch flags).

## What the pilot does (and does not)

A pilot peer, at every KG's `ne_declared` step, **additionally**:

1. asks its local `bevdirect-serve` (github.com/raffopenssh/vtcseamless,
   `v0.3.2` — pin `ne_h3.BEVDIRECT_TAG`) for one `/viewport` per aligned 0.02° cell of the parent's bbox —
   parcels + footprints + landuse assembled from the public BEV Katastral-
   mappe vector tiles (CC BY 4.0);
2. turns the document into the declared NE layer **in process** with
   umfeld's pinned `ne_cells` package (`ne-cells-2`, source
   `bevdirect@v0.3.2`, epoch = current month; v0.3.1 / v0.3.2 are registered as
   cell-equivalent to the bev baseline on the NE server —
   `manifest.bev_equivalent_tags` — so no re-baselining);
3. compares it with umfeld's frozen `lu` layer of the same KG (cell set
   overlap, dominant-group agreement, share of cells within umfeld's ±8/255
   tolerance);
4. **contributes** the digests to umfeld-at:
   `POST https://example.invalid/contrib/api/v1/ne/{kg}/report` with our
   cadastre token (observer `srtm-peer-<id>`); the answer tells whether the
   chunks match umfeld's baseline / earlier observers.

The **product is unchanged**: the light GPKG / JSON / `_ne.h3b` keep
umfeld's `lu` layer, and the NEO1 `/obs` POST (whose cell set must equal
umfeld's) works exactly as before.  The pilot is telemetry + contribution;
a failure is one WARNING line and a jsonl row, never a KG failure.

> Why not `ne_source=bevdirect`?  During umfeld's freeze (→ 2027-04-30) a
> bevdirect-built declared layer differs from the index epoch by a few cells
> (63330: 52 478 vs 52 476), so every `/obs` post would 409.  The knob is
> now refused without `_force`.  The pilot is the supported path.

## Rules (the VTC posture) and how the code keeps them

| rule | implementation |
|---|---|
| cadastre data only in memory | bevdirect-serve ≥ v0.3.1 caches raw BEV tiles **only in its own RAM** (`-tile-cache-mb 512` LRU on our 8 GB peers, `GOMEMLIMIT=1280MiB`; upstream default 1024/2560 — over budget = refetch, nothing breaks; no `.pbf` is ever written) and the assembled cells in RAM (`-ttl 6h`); `ne_h3._purge_legacy_tile_dirs` deletes the pre-v0.3.1 `/dev/shm/srtm_bevcache` / `data/bevcache` dirs on every launch / tooling install (`-cache` is no longer passed); the viewport document is parsed in-process and dropped; only counts/digests/bytes are written (`data/austria_processor/vct_pilot.jsonl`, ≤200 rows). Row field `lru=used/maxMB` (from `/health tile_cache`) shows the budget |
| ≤ 24 h | `-tile-ttl 24h` (expired tiles dropped hourly from RAM), everything is gone with the process; `ne_source=bevdirect`'s legacy cell files live on tmpfs (`/dev/shm/srtm_ne_work`) and are unlinked after the build |
| one server per batch | the cache no longer survives a restart, so `ne_h3._ensure_bevdirect_server` launches **one** long-lived server per peer (pidfile `data/bevdirect-serve.pid`) and only restarts it when `/health` reports a version ≠ `BEVDIRECT_TAG` |
| one request per processing | one `/viewport` per **aligned 0.02° bevdirect cell** of the padded parent bbox (`req=N` on the dashboard; since 2026-10-07 — see *Request pattern*), all local to the peer; blocks of a split parent on the same peer re-use the parent's run for 24 h; `-prefetch 0` (no speculative neighbours) |
| never interrupt the herd | bounded helper subprocess (`VCT_PILOT_TIMEOUT_S`, 25 min) in a thread; the KG waits for it only at exit; flips are live sidecar writes (no restart); one enrolment per director tick |
| bandwidth visible | per row: tiles, tile bytes, cache hits, **estimated network bytes** (`bytes × (1 − hits/tiles)`, 0 for warm cells); summed per peer (24 h / total) and fleet-wide on `/process.txt` |

## Operating it

```bash
TOKEN=$(cat data/admin_token)
# let the director pick ONE frontier peer with ready tooling (sticky; logged as 'vct_pilot: enrolled …')
curl -s -X POST -H "X-Admin-Token: $TOKEN" -H 'Content-Type: application/json' \
  -d '{"vct_pilot_auto":1}' localhost:8000/api/v1/director/config
# or name peers explicitly (list replaces)
curl -s -X POST -H "X-Admin-Token: $TOKEN" -H 'Content-Type: application/json' \
  -d '{"vct_pilot_peers":["at201"]}' localhost:8000/api/v1/director/config
# stop the pilot everywhere (sidecars cleared within one 5-min tick)
curl -s -X POST -H "X-Admin-Token: $TOKEN" -H 'Content-Type: application/json' \
  -d '{"vct_pilot_peers":[],"vct_pilot_auto":0}' localhost:8000/api/v1/director/config

# follow
curl -s 'localhost:8000/process.txt?roster=0&log=0' | grep -A12 '^vct_pilot:'
curl -s 'localhost:8000/process.txt?q=vct_pilot&log=100'        # peer + director events
curl -s -H "X-Admin-Token: $TOKEN" localhost:8000/api/v1/director/status | jq .vct_pilot
curl -s -H "X-Admin-Token: $TOKEN" 'https://example.invalid:8000/api/v1/admin/vct_pilot?rows=20'
curl -s -H "X-Admin-Token: $TOKEN" 'https://example.invalid:8000/api/v1/admin/ne_tooling'  # last_install.pip_tail when package=false
```

Director flow (`vct_fleet.director_tick`, every 5 min): enrol (frontier,
tooling ready, lowest steal) → stage tooling on pilot peers that lack it
(`POST /admin/ne_tooling`, own pacing) → flip the sidecar on running pilot
peers (`POST /admin/vct_pilot {"on":true|false}`) → fold the peers' rows
into `data/austria_processor/vct_fleet.json` → confirm contributions
(`GET /k/api/v1/ne/{kg}/report` must list our observer + digest, ≤5/tick).
`start_peer_processor` also sends `vct_pilot: true|false` in every start
payload, so a peer leaving the pilot clears its sidecar on its next start.

## Reading the `/process.txt` block

```
vct_pilot: peers=at201(on) auto=1 · kgs_24h=7 ok=7 · bw(vtcseamless): net~41.2MB/24h tiles=88.0MB req=9 total~41.2MB · contrib_confirmed=6/7 obs_confirmed=5/7
    split_assembly(fleet): parents=174 complete=72 confirmed=72 partial=102 awaiting_blocks=0 merge_errors=0
    23308 at201 10-06 06:41 bevdirect@v0.3.1 cells=5141 req=1 net~3.3MB lru=41.2/512MB 14s | umfeld common=5140/5141 dom=97.3% tol=84.7% | report 200 base=report same=133 chg=0 unk=0 ✓contrib | unsplit obs=✓confirmed
    73008 at201 10-06 05:02 … | split 3/4 blocks merged-partial obs=posted?
```

* `peers=` — `on` (sidecar set), `pending` (configured, flip not yet
  acked), `tooling?` (binary/package missing → being staged),
  `not-configured` (peer reports on but is no longer in the list → flipped
  off next tick).
* `bw(vtcseamless)` — fleet sum of the peers' 24 h windows: `net~` estimated
  bytes fetched from the BEV tile host, `tiles=` bytes of tiles touched
  (incl. cache hits), `req=` viewport requests.  Per peer in
  `director/status.vct_pilot.peers`.
* `contrib_confirmed=a/b` — reports umfeld lists under our observer+digest /
  reports that got HTTP 200.  `obs_confirmed` — pilot KGs whose NEO1 digest
  umfeld's manifest shows (the normal 2.4 round-trip, via `ne_ingest`).
* `split_assembly(fleet)` — every split parent the primary merges
  (`ne_ingest`): `complete` = all live blocks ingested + coverage complete,
  `confirmed` = merged NEO1 digest == umfeld's, `partial` = still waiting
  for blocks, `nec1_disagree` = blocks whose per-block NEC1 differed and
  were re-keyed (see *Split blocks in bev mode*), `bev_lu_delta` = parents
  whose merged `bevdirect@` NEC1 has a different cell count than umfeld's
  frozen `lu` — expected source noise, quiet.  `CELLS-MISMATCH` (loud) =
  the same count mismatch on a parent whose NEC1 **is** umfeld's `lu` —
  must never happen (blocks carry the parent's full cell set).  Before
  2026-10-18 the audit lacked the bev carve-out and reported ~90 bev
  parents as `CELLS-MISMATCH`; that was an audit bug, not data loss.
* Row: `umfeld common=c/u dom=% tol=%` — our cells ∩ umfeld's, dominant
  land-use agreement, share inside the ±8/255 tolerance (umfeld documents
  index-vs-bevdirect source noise up to 8/255 — this is *not* change);
  `report … base=` `section:bev.nec` (compared to umfeld's own bevdirect
  build), `report` (per-chunk baseline from earlier observers, e.g.
  siedler), `this_report` (we are the first → becomes the baseline);
  `same/chg/unk` = chunks vs that baseline.  Tail = split state + obs.

First live run (primary, 23308 Guntrams, 2026-10-06): 1 request, 276
tiles, 6.8 MB tiles / ~3.3 MB network, 5 141 cells both sides, 5 140
common, dominant agreement 97.3 %, 84.7 % within tolerance, report 200
`identical_kg_digest:true` against an earlier observer's baseline.

## Change protocol (vtcseamless-py 0.2.0 / `ne_cells.change`, umfeld 2026-10-08)

Spec: `GET https://example.invalid/api/v1/docs/ne-change.md` (public, also
under our `/k` prefix).  The `change(ne-change ap-1):` line on
`/process.txt` and the `chg:` row tail come from here.

* **Step 1** — the `/contrib` report carries `chunks_ap` (sha256 per chunk,
  `gk` / `n_parc` zeroed).  Answer: `baseline_digest` + `baseline_bbox` =
  the first-seen *peer* report under the current bbox layout (epoch 0 is
  re-anchored per chunk on it — expect `chunks_changed` vs epoch 0 to be 0
  for v0.3.3 cell blocks; `our_digest` is the same value, kept for compat,
  it is not a build of umfeld's), `chunks_compared/same/changed`,
  `want_chunks` (rows umfeld lacks), `chunks_held`, **`chunks_confirmed`**.
* **Step 2** — `POST …/chunks` with the NECH rows of `want_chunks`
  (`_post_chunks`, ≤ 1.5 MB per body).
* **Step 0** (`--header-first`) — header-only report first.  Keyed like the
  full path: token + source class (`bevdirect@*` = one class) + algo + digest
  + epoch; **observer and bbox are not in the key**.  Same digest as any
  earlier report of ours → `200 unchanged:true, chunks_confirmed:n,
  since_last{compared,same,changed}` (~300 B instead of the chunk table);
  else `202 need:chunks` → the full report follows.  Before 2026-10-08 the
  probe required a chunk-bearing prior report with the same bbox, so it
  answered 202 forever after the v0.3.3 bbox change (23308 repro — now
  `unchanged:true, compared:133, chunks_confirmed:133`).
* **Confirmation (§5)** — a chunk's statistics are confirmed by a second
  sighting: another observer at once, or the same observer ≥ 20 h later.
  Both an `unchanged:true` header and a step-1 report whose `chunks_ap`
  matches the held version (`want_chunks:[]`) count — no step 2, no dedupe
  loosening needed.  `_report_policy` therefore sends `--header-first` for
  every parent somebody of ours already reported (same peer or a sibling
  block's peer) until it has ≥ 2 observers, then `--skip-report`.
  Telemetry: `hdr_unchanged`, `confirmed_chunks` (24 h) on the change line,
  `confirmed=n` in the row tail, `2obs` = parents with two observers.

## Is the contribution lossless / efficient? (checked 2026-10-07)

* umfeld stores every 200-answered report (`data/ne_cells/{kg}.reports.jsonl`);
  the first full report of a KG becomes the per-chunk baseline, later ones are
  compared; same-digest re-posts are deduped to a stub.  `contrib_confirmed`
  on `/process.txt` is the director re-reading `GET /k/api/v1/ne/{kg}/report`
  and finding our observer + digest — the proof nothing was dropped.
* **Retry** (since 2026-10-07): the `/contrib` POST retries 429 (honouring
  `Retry-After`) and 5xx / connection errors up to 4× inside the helper's
  deadline (`report_attempts` in the row).  Before, one transient lost the
  KG's report until its next pass (months).
* **Split blocks dedupe fleet-wide** (`vct_pilot.already_reported`): a block
  first asks umfeld whether any `srtm-*` observer reported the parent with
  our bevdirect tag in the last 30 d; if so it skips the viewport
  (`skipped=fleet-reported` row, `skipped_24h` in the summary).  Previously
  every block of a 12-block parent on a different peer re-assembled the
  whole parent (~10 MB, 1–2 min) only to be deduped by umfeld.
* `kgs_24h` / `ok=` exclude skipped rows, so the ramp health gate is unaffected.

## Quality: vtcseamless build vs umfeld's `/k` index layer (2026-10-07)

19 full-coverage pilot KGs (at68): cell sets identical (±≤6 border cells),
dominant-group agreement median 96.6 %, within-±8/255 median 83 %.  The
low tail (63101 Graz Innere Stadt 76 %/47 %, 09104, 19452) is **not** our
noise — it tracks umfeld's own `declared_unknown` share: the `index-2026-03`
copy lacks most *verkehr* land-use pieces there (63101: lu[verkehr]
Σ 1 138 vs ours 230 241; 1 726/3 894 cells `declared_unknown` vs our 4;
`cov` mean 193 vs 255).  Determinism across hosts holds: the primary's
rebuild of 63101 answered `identical_kg_digest:true`, 103/103 chunks same
as at68's.  Conclusion: the bevdirect-built declared layer is at least as
good as the index and more complete in dense towns — safe to ramp, and the
`/k` declared-layer fetch is only still needed for split blocks (freeze
rule) and for the comparison column.

Recipe for the per-group diff of one KG:
```bash
python3 vct_pilot.py run --kg 63101 --bbox W,S,E,N --umfeld --no-record --out /tmp/63101.nec
python3 - <<'PY'
import ne_h3, numpy as np
o=[s for s in ne_h3.unpack_sections(open('/tmp/63101.nec','rb').read()) if s.magic==ne_h3.MAGIC_LU][0]
u=ne_h3.fetch_declared_umfeld('63101'); a,b=o.lu_table(),u.lu_table()
print(np.asarray(a['lu']).astype(int).sum(0), np.asarray(b['lu']).astype(int).sum(0))  # groups: geb bau acker gruen wald wasser verkehr alpen sonst
print(o.header['stats']['declared_unknown'], u.header['stats']['declared_unknown'])
PY
```

## The `ne_declared` step issue (fixed alongside)

Fresh KGs on frontiers often hit `NE declared layer not built at umfeld`
(umfeld builds ~1 KG/33 s in sorted order) and shipped as `v2.4-partial`
(defect `ne_cells` → a full re-upgrade later).  Step 5c now retries the
fetch once right before `gpkg_light` (1–3 h later); `result.ne_status`
is `ok_late` when that rescued the product.  The step 1b warning text
says so.

## Bevdirect tooling on peers

**Binary source & version pin (v0.3.2, 2026-10-07; v0.3.1 2026-10-06).**
v0.3.2 (upstream commit 5ffbe23) fixes the duplicate footprint fragments in
multi-cell `/viewport` documents that we had worked around by requesting
aligned cells (see "request pattern" above); aligned-cell builds are
digest-identical before/after (23308 → `7c7e0eb45c2148d0` on both, verified
on the primary at upgrade). Release tarballs are sha256-pinned in
`ne_h3.BEVDIRECT_RELEASE_SHA256`. Pieces now carry `complete` and merged
footprints `members`; `ne_cells` canon ignores both. `ne_h3.bevdirect_ready()`
reports `binary:true` only when `bevdirect-serve -version` equals
`BEVDIRECT_TAG` (`binary_version` carries what is installed), so a tag
bump makes the whole fleet read `binary:false` and re-install:
`ensure_bevdirect_tooling()` tries the static GitHub release tarball
first (`BEVDIRECT_RELEASE_URL`, un-metered; runs on every peer at srv
start via `app._ensure_v2_deps` with `primary_fallback=False`), then the
primary's `/api/v1/admin/bevdirect_binary` (only from the paced director
stager), then `go install`.  A server still running the old version is
stopped (`_stop_bevdirect_server`) and relaunched at the next KG.
Watch `bevdirect_ready=N/50` on the `ne:` line (50 = every enrolled peer incl. idle ones — the slim idle heartbeat carries `ne_tooling` since Oct 2026 so the HA shadow is counted too; primary is excluded) drop and climb back after
a bump; rows switch to `bevdirect@<tag>` automatically (source string is
read from the documents).

**Package source (fixed 2026-10-06, 2nd commit).** The first pilot commit
pip-installed `ne_cells` from `cadastre-process-api`, which is *private*
— peers could not clone it, so 48/49 stayed `tooling?` and the
auto-enrolment had only the hand-installed at68 to choose from.  The
package now comes from the public
[vtcseamless-py](https://github.com/raffopenssh/vtcseamless-py) repo
(pip project `vtcseamless`, modules `vtcseamless` + `ne_cells`;
`algo/pack/canon/ns_groups` byte-identical to umfeld's copy, pinned
`shapely==2.1.2` / `h3==4.5.0`).  Pin: `ne_h3.NE_CELLS_PIN` (commit of
vtcseamless-py), min package version `NE_CELLS_MIN_VERSION = 2.1.0`.

**Rollout path.** `app._ensure_v2_deps` (srv startup thread, ~45 s after
start, and the post-`git pull` update path) calls
`ne_h3.ensure_ne_cells_package()` on **every** peer: legacy private-repo
dist (`ne_cells` 1.0.0/2.0.0) is uninstalled, the public one installed
`--user` with `sys.executable`.  GitHub egress only — the primary's
metered link is untouched.  The ~9 MB `bevdirect-serve` binary stays on
the paced director stager (`app._maybe_stage_ne_tooling`, 1 peer /
10 min) — 49/49 already have it.  Watch convergence on the `ne:` line:
`bevdirect_ready=N/49` (N = binary + package), and
`GET /api/v1/admin/ne_tooling` → `last_install.pip_tail` on a peer that
stays at `package:false`.

49/49 peers had the binary, 1/49 the `ne_cells` package (Oct 6).  The
installer now runs with `sys.executable`, keeps pip's tail in
`data/ne_tooling_last.json` (shown as `last_install` by
`GET /api/v1/admin/ne_tooling`) and `bevdirect_ready()` calls
`importlib.invalidate_caches()` so a package installed after gunicorn
started is seen without a restart.

## bev mode without umfeld's `/k` tier (2026-10-07)

A peer in `vct_bev_peers` (`ne_source=bevdirect`) makes **no call to
`https://example.invalid/k/…`** any more.  It still uses umfeld's public
tier (`/api/v1/kgs` for the KG list) and `/contrib` (the report POST).  The
director / primary keeps `/k` (lu relay, `ne_ingest` merge + `/obs`,
report confirmation, `/api/v1/kgs` universe).

| former `/k` call from a bev peer | now |
|---|---|
| `cadastre.py` + `austria_processor.fetch_cadastre_data` — `/export/geojson?kg=`, per-tile `/spatial/footprints`, `/spatial/points` | **`fetch_cadastre_data_bevdirect`**: parcels + footprints from the parent's ONE viewport grid — the same documents `vct_pilot.build_declared` turns into the NEC1 (`cadastre_from_docs`).  The helper subprocess streams them to the KG subprocess over an anonymous pipe (`--cadastre-fd`): **RAM only, never a file** (no tmpfs either).  Per-tile neighbour footprints come from the same padded viewport (`building_footprints_viewport`) |
| `/search/kg` (bbox when the kg dict has none; `--kg` CLI) | `_kg_bbox_lookup`: `kg_list.json` → local `bevdirect-serve /kg/<kg>` (BEV VGD) → our public `/api/v1/kg/<kg>` |
| `ne_h3.fetch_declared_umfeld` for **split blocks** + the `ne_declared_late` retry + the pilot comparison | blocks build their own bevdirect NEC1 (they fetch the parent viewport anyway).  Fallback for a failed build / `DeclaredDeficient` → **`fetch_declared_director`**: `GET <director>/api/v1/ne/declared/<kg>` (admin token; the director fetches umfeld's lu once, caches `data/ne/lu_cache/`, 30 d / 400 MB).  The comparison column is dropped in bev mode (`lu_diff` comes from `/obs`) |
| `ne_h3.post_observed` (`/ne/{kg}/obs`) | not a `/contrib` write (survey token only) → the peer records `ne_obs_posted: {deferred_to: primary}`; the primary's `ne_ingest.confirm_umfeld` posts NEC1+NEO1 after the h3b ingest (it already did so as the safety net; now it is the only path for bev peers).  Expect `obs_confirmed` to lag the upload by one ingest tick + budget |
| `vct_pilot.already_reported` (`GET /ne/{kg}/report`) | `GET <director>/api/v1/director/vct_reported?kg=` — `vct_fleet.json` keeps a 30-day `reported` map folded from the peers' pushes.  In bev mode a block still fetches the viewport (it needs the cadastre) and only skips the `/contrib` POST (`skipped_report` in the row, `skip_rep` in the push) |
| manifest reads (`fetch_manifest`) | primary only (`ne_rollout`) — unchanged |

**What the cadastre loses / gains vs `/k` (checked on 23308, 63101, 75110):**
parcel ids identical ±1–6 border parcels; parcel area sum within 0.4–2.4 %
(VTC geometry is generalised: median per-parcel area difference 0.6–2.5 %;
irrelevant at 1 m raster); own-footprint union IoU 0.96 / 0.86 / 0.98 —
63101 bevdirect has 30 000 m² *more* footprint inside the parcels (the
index copy lacks pieces), 16 900 m² less; fields not available: `kg_name`,
`landuse_summary`, `ns_recht`, the address-point layer — none is read since
v2.4 (`CADASTRE_PRODUCTS=False`; `building_addresses` is always `[]`).
Products (JSON / light GPKG / h3b schema, the 25 types) are unchanged.

**Clipped alpine parcels (the one real difference).**  bevdirect-serve
assembles per 0.02° cell (+0.004° pad); a parcel wider than that has no
`complete` copy (75110: 13 / 2 643 own parcels, 6.5 Mm² = 13 % of the KG).
For the *pipeline* `cadastre_from_docs` unions the clipped pieces back
(`restored`; area sum −0.44 % vs `/k`).  For the *declared layer* the frozen
`ne_cells` keeps one piece per parcel, so the bevdirect NEC1 of 75110 has
164 258 cells vs lu's 168 225 (`only_umfeld=4 024`) — a loss, and it is the
`bevdirect@v0.3.1` source as umfeld defines it (deterministic:
`identical_kg_digest:true`), so it cannot be "fixed" client-side without a
new source tag.  **Superseded the same day**: with the per-cell request pattern (next
section) 75110 builds 168 243 cells vs lu's 168 225 — the loss was the
viewport artefact.  `vct_pilot._hybrid_check` is therefore OFF
(`VCT_LU_ON_CLIPPED=1` re-enables); the director lu relay remains the
fallback for a *failed* local build only.  The row still carries
`cad.incomplete_own` / `restored`.

**Split blocks in bev mode.**  Every block posts its own NEC1 (bevdirect, or
lu when deficient / fallback).  `ne_ingest.merge_blocks` no longer fails on
a NEC1 digest mismatch: it picks the declared layer with the most cells
(lu-declared blocks first), re-keys the other blocks' rows by cell id and
records `nec1_disagree` in the merged header / index (`split_assembly:`
line shows `nec1_disagree=N`; observed cells outside the chosen set are
counted as `observed_dropped`).  Tested with a synthetic 3-cell mismatch on
01006 (merged == reference merge except the removed cell).

Peer-side invariants: no `/k` host in bev mode (grep
`umfeld.CADASTRE_BASE` users: `cadastre.py`, `fetch_cadastre_data`,
`fetch_declared_umfeld`, `post_observed`, `vct_fleet._confirm_contrib` —
all gated behind `cadastre_source()/ne_source() != bevdirect` or primary
only); the umfeld token still reaches peers (needed for `/contrib`).

**Is it an almost perfect match? (re-checked geometrically, 2026-10-07)**
Yes for what the raster pipeline reads: per-parcel geometry IoU median
0.999 / 0.996 / 1.000 (23308 / 63101 / 75110), geometric area difference
median 0.06 % / 0.14 % / 0.00 % — the 0.4–2.4 % "area" gap quoted above was
umfeld's *legal* `area_sqm` attribute vs geometric area (itself 0.6–2.5 % off
its own geometry).  Footprints: per-feature best IoU median 1.000; every
`/k` footprint without a bevdirect match lies outside the padded bbox
(umfeld's export carries a wider ring) except 38 in 63101 (index copy ≠
current tiles).  The IoU < 0.9 tail (6 / 65 / 46 parcels) is (a) tiny
parcels the index stores as ~11 m² placeholders, (b) border wobble of the
tile simplification, and (c) in 75110 two large parcels bevdirect-serve
truncates (`2254/2` 27 213 vs 43 493 m², `762/2` 687 k vs 991 k m²; every
copy identical, `union_guard_trips=4`) — a vtcseamless assembly limit
(`UnionPieces` guard / piece clipping), not something the client can
restore; it also shrinks the bevdirect NEC1 for such parents, which is why
`DeclaredDeficient` sends them to lu.  Worth a fix upstream in vtcseamless.

## bev mode is the only cadastre source — the ne_source=umfeld incidents (2026-10-16)

umfeld's `/k` cadastre routes (`spatial/points`, `spatial/footprints`,
`export/geojson`) answer **410** since the Kataster purge (2026-10-13).  A KG
run with `ne_source=umfeld` logged `cadastre fetch failed (non-transient):
410`, then **proceeded without parcels/footprints** (v2 context with no
parcels, every roof a "new building", `done … 0 par, 0 bldg`) and hit 410
again per tile.  Seen on 20 KGs 10-15/10-16, two causes:

1. **14:21 10-15 — shadow takeover without knobs.** An edge blackout handed
   the director to shadow at216; `director_config.json` was not in
   `director_ha.SNAPSHOT_FILES`, so `vct_bev_peers` read as `[]` and
   `vct_fleet._sync_peers` flipped **46 peers to `ne_source=umfeld`** in one
   tick (flipped back 15:1x when the primary resumed).
2. **Post-update race.** 75efad1 re-pinned bevdirect-serve v0.3.4, so on a
   freshly auto-updated peer `bevdirect_ready().binary` was False for a few
   minutes; `processing/start` degraded `bevdirect → umfeld` (wrote the
   sidecar) and kicked the installer, the director's `peer_bev_ready()`
   sent `umfeld` too, and the KG started before `_sync_peers` flipped the
   sidecar back.

Fixed in layers (all must hold):

* `director_ha.SNAPSHOT_FILES` includes `director_config.json`;
  `vct_fleet._sync_peers` **never downgrades** a peer to umfeld (warns once).
* `peer_director.start_peer_processor` sends `ne_source=bevdirect` for
  every `vct_bev_peers` member unconditionally.
* `app.processing_start` **refuses** (`503 {retry:true}`) a bevdirect start
  while the tooling is not ready and installs in the background; the
  director logs `start deferred: bevdirect tooling installing (<ver>)` and
  retries next tick.  It never writes `umfeld` into the sidecar any more.
* `austria_processor` cadastre step: sidecar ≠ bevdirect → if the tooling is
  ready, **flip the sidecar itself** and run bev mode; else defer the KG
  (`_fetch_failed`, log `not in bev mode … deferring`).  A 410 from
  `fetch_cadastre_data` is `_fetch_failed` (never "empty cadastre").
  `cadastre.py` remembers a 410 for 6 h (`CadastreRetired`) so no per-tile
  umfeld calls follow.

Audit: `/process.txt?hours=48&q=410` must show no `cadastre:` lines;
`?q=switched to bev mode` / `?q=deferring KG` / `?q=start deferred` show the
guards firing.  Affected products were re-queued (`processing/queue`,
`skip_processed:false`) on 2026-10-16.

## Request pattern: per aligned cell, not per viewport (2026-10-07)

A multi-cell `/viewport` (our former ≤0.045° grid) is **not** the reference
input.  `bevdirect-serve.compose` merges the cells' footprints/landuse by
their per-tile ids (`z_x_y#i`) and footprints carry no `complete` flag: a
building crossing a z16 tile edge that lies inside a neighbour cell's 0.004°
pad is whole in one cell (both tiles fetched) and a clipped fragment with a
*different* id in the other → both emitted.  23308: 8 such fragments on the
tile edge x = 16.1554, `bld_n` +1 in 7 cells, digest `3ace8fba…` instead of
the per-cell `7c7e0eb4…`.  63330, the documented reference: per-cell fetch →
**`594025647ec3557e`, 52 478 cells = umfeld's `bev` section bit for bit**;
the viewport grid → `60d6d156…`, 52 458 cells.  So every earlier pilot
report was only ever compared with our own earlier reports (same bug both
sides), never with the operator / siedler baseline.  `vct_pilot._grid` now
returns the aligned 0.02° cells (what `vtcseamless observe` and umfeld's
`ne_all.sh` do); the BEV traffic is identical (the server assembles per
cell), `req=` grows (20 for 63330, 56 for 75110).  The cadastre for the
pipeline gets the dissolved buildings too (no edge fragments).

Upstream (vtcseamless) should still fix `compose` for multi-cell viewports —
siedler reads them: give footprints / landuse a `complete` flag like parcels
and apply "complete copy wins", or drop a fragment whose bbox touches the
cell's fetched bound when another cell holds a copy.

## Helper `rc=-9` on huge parents — tiled NEC1 build (2026-10-16)

**Symptom.** Every block of Sölden 80110 (42 blocks, parent bbox
0.372×0.241° → `req=260`, 1 525 270 cells) logged `bevdirect
cadastre/declared build failed … helper rc=-9:` (empty stderr) 10–20 min
after `cadastre — bevdirect viewport 80110`; the cadastre had already
arrived over the pipe, so the block went on with umfeld's `lu` relay
(~15 min wasted per block, no bevdirect NEC1 / report).  81123 (`req=140`,
822 k cells) and 86041 (`req=78`) succeeded; 80110 succeeded only when the
peer happened to have the RAM (at226 05:46, 705 s).

**Cause (measured on the primary, `/usr/bin/time -v` + `/proc/self/status`).**
The frozen `ne_cells.algo.build_cells` is dense per cell: `acc` (clipped
parcel / landuse / footprint pieces per res-12 cell) + `cellpoly` + the
`rows` dicts ≈ **3.4 KB per product cell** (75110: 168 243 cells → helper
HWM 772 MB, +555 MB for the build alone; rows alone 2.3 KB/cell).  The 260
documents of 80110 are only ~0.8 GB; the build needs ≈ 5–6 GB, and the
helper shares the processor's `systemd-run … MemoryMax=8G` scope with
the KG worker, the parent and bevdirect-serve (`-tile-cache-mb 512`) → the
cgroup OOM killer takes the helper (SIGKILL = `rc=-9`, never our
`TimeoutExpired` path; the same scope showed `oom_kill=2` in the stuck-KG
investigation, `c8a3952`).

**Fix.** `ne_build_tiled.build_cells_tiled`: above `VCT_TILE_CELLS`
(300 000) estimated cells (own-parcel area / 307 m²) the global input
domain is cut into an nx×ny grid of cores; each core is built with a
0.006°-padded sub-domain and only the cells / k-cells whose H3 centre
lies in the core are kept ("homed"), packed straight into `LU_REC` /
`K_REC` bytes.  Because the algorithm is a function of the input domain
(ne-cells-2 `INPUT_RULE`, cells not fully inside are withheld), the homed
rows — and the k-accumulators of homed res-10 cells, whose children all
lie inside the pad — are those of the monolithic build.  Verified
**byte-identical** on 75110 (12 tiles, HWM 388 vs 763 MB) and 19732
(6 forced tiles); 80110 (15 tiles, 1 525 270 cells, same BEV tile hash
`2b8189e9a021fac7`) reproduces the four-observer fleet digest
`47b0708d15561d74` at HWM 1.25 GB in 16 min.  Pitfall found on the way
(fixed in the commit after 0e34e03): the parcel tile-copy dedupe must be
done ONCE over the whole input (`ne_build_tiled._dedupe`, verbatim
replica) *before* the per-tile pre-filter — the build picks id/kg/ez/
area_sqm from the largest copy and unions truncated copies, so a per-tile
subset of the copies drifted the digest at equal cell count.  Tiles whose
padded sub-domain contains no own parcel are skipped (the build would
otherwise reach `h3.geo_to_cells(None)`).  Header `stats` differences:
`slivers_omitted` / `no_own_share` / `kcells_withheld_coverage` are summed
over the sub-builds (double-count the overlap bands), `cells_outside_domain`
is omitted, `stats.tiled=N` is added — none of it is in the digest or the
/contrib report.  Small KGs keep the monolithic build (bit-identical header
too).  Rows and the `vct_pilot:` lines now carry `rss=<peak MB>` (helper
`ru_maxrss`) and `tiled=N`; a parent-side `rc=-9` row carries the scope's
`cgroup oom_kill=… peak=… max=…` instead of an empty stderr.

**NEC1 reuse with the cadastre.** `build_declared(cadastre=…)` used to
rebuild on every block (the kept `<kg>.nec` was only read when no
cadastre was requested).  Now a fresh (<48 h) kept NEC1 makes the helper
run `--cadastre-only`: fetch the (warm) cells, stream parcels + footprints
over the pipe, skip the build and the report (`CAD-ONLY(nec reused)` in
the fleet line, `skipped_report="nec reused (cadastre only)"`).  Sibling
blocks of a split parent on the same peer thus cost the fetch only.
