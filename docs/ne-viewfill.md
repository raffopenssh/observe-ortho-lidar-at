# NE view-fill — every cell of the NE server's `/declared` view observed

**Why.** Since 2026-10-27 NEO1 is posted re-keyed onto the NE server's
`GET /contrib/api/v1/ne/{kg}/declared` *view* (chunk-log cell set). View cells
our product never observed were padded as *blank* rows (obs_lu 0, h_mean 255,
flight_years 0) — NE server lists them under `revisit?kind=obs_missing`
(`blank_n`). On 2026-11-04: 2 782 KGs / 1.13 M cells. Three causes, three fixes:

| cause | example | fix |
|---|---|---|
| boundary cell moved to this KG, a neighbour observed it | most KGs, a few hundred cells | **corpus borrow** at post time (`ne_h3.neo1_section_on_view(borrow=True)`, also for cells our file holds *unseen* since 2026-11-04) |
| our own products hold the cell but the live file lost it (block merge overwrote an own unsplit product: 05003; a block missing at merge time: 30109; old-layout `-east-1` blocks: 83103) | 05003 31 k, 30109 27 k, 83103 64 k | **staged union merge** of `[live file] + own `_ne_h3b` (re-pulled) + blocks + sweeps` (`ne_ingest.merge_blocks`); `merge_dirty_parents` now keeps the own product in the union (`own_in_merge`) |
| nobody observed it (view grew inside the bbox: 16114, 23457 ~5 k each) | residual | **sweep units on the view** (`sliver_sweep.plan_cells`, `declared: "view"`), built by the cache-only sweepers like the bbox-revision slivers |

## Pieces

* `ne_viewfill.py` — the builder. Own process (`ne-viewfill.service`, hourly
  `ne-viewfill.timer` at :20, nice 15 / idle IO / 2 GB / 40 min budget) so the
  view fetches, corpus look-ups and merges never run inside gunicorn or the
  director tick. Candidates = NE server `obs_missing` ∪ local scan of
  coverage-complete parents with unseen rows (cached 24 h) ∪ mixed parents;
  **largest gap first**. Never writes `data/ne/index.json` or a live file:
  results go to `data/ne/viewfill/staged/<kg>.json` (+ `.h3b`); state in
  `data/ne/viewfill/state.json`; pulled own products in `viewfill/pulled/`.
  Inbound budget channel `viewfill` (knob `viewfill_mb_per_h`, default 5000 —
  effectively unlimited: at 400 the hourly run hit the budget after ~13 min /
  ~100 KGs, so the 1 765-KG walk would have taken ~18 h; 2026-11-04). `python3 ne_viewfill.py plan <kg>` = dry run. The CLI sets `SRTM_APP_LIB_ONLY=1`
  before touching `app.py` (`ne_ingest._coverage_complete` imports it): without it
  the side process also ran app's peer-sync fanout / ingest loops / director
  bootstrap (`app._start_bg`, 2026-11-04).
* `ne_ingest.consume_viewfill` — first step of `run_tick` (≤200 staged/tick — was 40, which starved the sweepers once the walk ran unthrottled):
  swaps the staged h3b **only if** the live file's NEO1 digest still equals
  the builder's `base_digest` (else `rejected`, re-walked next run), re-indexes
  cells (`_index_cells` → bins stale → `cells-bins.timer` rebuilds them),
  updates index rows (`viewfill_at`, `viewfill_from`), queues `plan_cells`,
  sets `ne_server[kg].repost="viewfill"` → `confirm_ne_server` posts it at top
  priority even when NE server already holds our digest (the rows it holds are
  the blank ones).
* Sweep units with `declared: "view"` (spec → `_sweep_declared` → peer
  `fetch_declared_director(layer="view")` → director relay
  `/api/v1/ne/declared/<kg>?layer=view`, cached 6 h in `data/ne/lu_cache/<kg>.view.nec`).
  `merge_blocks` ranks `chunks@…` NEC1 last as reference, so a view-built unit
  joins by union and never redefines the parent's declared layer.

## Observed nodata — `view_rekey.coverage="swept"` (NE server 2026-11-06)

Blank rows that are *genuinely* nothing (open water — 91119 Bodensee holds
~18 k —, ALS seams) would list a KG under `obs_missing` forever.  the NE server now
accepts, inside the NEO1 header's `view_rekey` block, `coverage: "swept"`
(the only value; anything else → 400; absent = unswept): the body's blank rows
then count as `nodata_n` (totals `kgs_obs_nodata` / `cells_obs_nodata`), never
as `blank_n`, and the KG is listed only while `missing_n > 0 || extra_n > 0 ||
blank_n > 0`.  Same rows with a flipped flag = digest-neutral header rewrite
(`coverage_changed:true`); the manifest echoes `obs.coverage`, `obs.blank_n`,
`obs.nodata_n` and the `view_rekey{}` as posted (counts refresh with the NE server's
3-hourly `obs_complete` pass, `?refresh=1` is not wired to it).

Observer side:

* **Blank rule = the NE server's** (2026-11-06): `ne_ingest.row_blank_ne` =
  `obs_lu == 0 & h_mean == 255`, elevation ignored. `neo1_section_on_view`
  counts `blank` and picks borrow candidates with it. Before, `row_observed`
  (elev counts as seen) said `blank=0` for ~2 400 KGs holding 1–6 elev-only
  rows each → no `coverage="swept"` flag → listed forever. Verified by hand:
  03008 (blank_n 3) → 2 rows borrowed, 1 flagged → `blank_n 0 / nodata_n 1`.
* **`ne_h3.view_coverage(kg)`** decides (`ne_h3.coverage_context()` reads
  `sliver_sweep.json` + `viewfill/state.json` + the staged dir once per
  tick): `"swept"` iff the sweep is closed (`state=="done"`, all units `ok`,
  `unswept==0`, `plan_deferred==0` — `docs/sliver-sweep.md → Closed sweep`)
  **and** the view-fill walk left *residual only* (row walked without error,
  nothing staged, `residual_n==0` or handed to the sweep via `planned_ts` /
  `residual_held`).  `neo1_section_on_view(coverage="auto")` stamps it only
  when the body has blank rows; stats carry `blank` + `coverage`, so do
  `idx.ne_server[kg].view` and the `on view … coverage=swept` log line.
* **`ne_ingest.confirm_ne_server`**: `refresh_obs_missing` keeps `blank_n`,
  `nodata_n`, `coverage` per listed KG and the NE server's `kgs_obs_nodata` /
  `cells_obs_nodata` / `kgs_obs_blank` totals.  The obs_missing memo key is
  `view_source:missing_n:extra_n:blank_n` (`_om_key`).  A listed KG is
  **`nodata_held`** when `_nodata_settled` (no drift, `blank_n` 0 or ==
  `nodata_n`) — a re-post would be a no-op; **`unswept_held`** when only
  `blank_n>0` and the sweep is still open — the integrate changes our digest
  and triggers the real re-post.  **`coverage_due`**: the NE server holds our rows
  unflagged (`obs.coverage != "swept"`) while blank rows exist and we would
  flag now → one header re-post per file digest (`idx.ne_server[kg].coverage_post`,
  lowest priority, `why=coverage-swept`).  Forced re-posts (viewfill,
  coverage) reset `post_attempts` like an obs_missing post — the hourly
  viewfill re-stage used to count as a failed-confirm retry and struck ~730
  `view_ok` parents out at `POST_MAX_ATTEMPTS`; `LADDER_RESET` gave them one
  fresh ladder (one-shot, `stats.ladder_reset_n`).
* Dry run: `python3 ne_viewfill.py plan <kg>` (residual) +
  `python3 -c 'import ne_h3;print(ne_h3.view_coverage("<kg>"))'`.

## Reading it

`/process.txt` → `ne_ingest:` block: `ne_server_confirm: … mismatch= nodata_held= unswept_held=
swept_repost(due/posted)= server_nodata=<kgs>kg/<cells>c blank=<kgs>kg` (the NE server totals
from the revisit `obs_complete{}`, `?` until its first pass); `viewfill:` sub-line:
`kgs=<walked>/<parents> left eta=~Nh (runs @walked/run)` — walk ETA, nothing else needed for a progress check — then `walked / blank / borrowable / staged / sweep=<cells>c/<kg>kg / residual / waiting`
(builder totals) · `tick: applied swapped rejected sweep repost errors due`
(ingest side) · `view_pass … viewfill N` = forced re-posts done. Sweep units
show up in `sweep:` as `-sw<k>` units with `source=viewfill`.

```bash
python3 ne_viewfill.py status
journalctl -u ne-viewfill -n 50
curl -s 'http://localhost:8000/process.txt?q=viewfill&log=200'
```

Expect: corpus/union cases converge within a few ticks after each hourly
walk; residual (sweep) cells follow the sweeper cadence (`docs/sliver-sweep.md`).
Throughput chain once the walk is done (2026-11-04 tuning): staged → `consume_viewfill`
(≤200/tick) → `confirm_ne_server` re-POSTs (`ne_post_max_per_tick`, 30→50; the ingest
tick takes ~12 min, so ~250 posts/h; `ne_post_gib_per_day` caps the day) and sweep
results → `sliver_sweep.integrate` (oldest received first, ≤40/tick in 150 s).
A KG walked with nothing left is not re-walked for 7 days (`RECHECK_S`); the
same residual set is not re-queued for 7 days (`REPLAN_S`).
