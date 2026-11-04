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
  ~100 KGs, so the 1 765-KG walk would have taken ~18 h; 2026-11-04). `python3 ne_viewfill.py plan <kg>` = dry run.
* `ne_ingest.consume_viewfill` — first step of `run_tick` (≤40 staged/tick):
  swaps the staged h3b **only if** the live file's NEO1 digest still equals
  the builder's `base_digest` (else `rejected`, re-walked next run), re-indexes
  cells (`_index_cells` → bins stale → `cells-bins.timer` rebuilds them),
  updates index rows (`viewfill_at`, `viewfill_from`), queues `plan_cells`,
  sets `NE server[kg].repost="viewfill"` → `confirm_NE server` posts it at top
  priority even when NE server already holds our digest (the rows it holds are
  the blank ones).
* Sweep units with `declared: "view"` (spec → `_sweep_declared` → peer
  `fetch_declared_director(layer="view")` → director relay
  `/api/v1/ne/declared/<kg>?layer=view`, cached 6 h in `data/ne/lu_cache/<kg>.view.nec`).
  `merge_blocks` ranks `chunks@…` NEC1 last as reference, so a view-built unit
  joins by union and never redefines the parent's declared layer.

## Reading it

`/process.txt` → `ne_ingest:` block, `viewfill:` sub-line:
`walked / blank / borrowable / staged / sweep=<cells>c/<kg>kg / residual / waiting`
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
A KG walked with nothing left is not re-walked for 7 days (`RECHECK_S`); the
same residual set is not re-queued for 7 days (`REPLAN_S`).
