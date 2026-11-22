# Tree apex → cell: ONE rule (`ne_rebin.py`, rule id `rb1`, 2026-11-22)

**Rule.** An apex belongs to the H3 res-12 cell that contains the position we
publish for it: `latLngToCell(round(lat, 6), round(lon, 6), 12)` — after the
metre rounding of the stored word and after every read-time position change
(position frames, `ne_pos_fix`). Every per-cell tree figure derives from that
one set: `/trees/bbox` `cell`, `tree_n`, `tree_h_max` (obs rows, cells API,
posted NEO1 rows), `tree_h` / `tree_h_n` lists (bins, blocks), `canopy_h_p*`,
res-9 `tree_hh` and the national sample.

## Why it was needed

| | before | after |
|---|---|---|
| plain KG | row = cell of the **float** apex (DSM pixel centre, x.5 m); served position = metre-snapped cell centre + whole-metre `dx/dy` (≤ 0.5 m/axis away) → ≈ 5.5 % of apices listed by the neighbour hexagon | 0 |
| KG with a position frame (245 KGs, e.g. 92124, (−5, +5) m) | row = **pre-frame** cell, position shifted at read time → ≈ 47 % in a neighbour | 0 |

## Where it happens (one choke point)

`cells_api._load_chunk` — the only way a stored chunk reaches a reader:

```
inflate → ne_green_fix → ne_pos_fix.fix_words → ne_rebin.rebin → tree_qa.patch → Chunk
          (lu classes)   (positions as served)   (rows as served)  (QA counts/thx)
```

`ne_h3.neo1_rows_from_h3b` (rows posted upstream) runs the same order through
`ne_rebin.FileSource`. Nothing on disk is rewritten, no position changes, no
field / URL / payload change. New builds (`ne_h3.build_observed`) already
store the apex under the cell of its stored position, so `rebin` is a no-op
for them (still needed for frames).

* `_assign(chunk)` — target cell per stored apex; H3 is asked only for apices
  > `SAFE_R` = 6 m from the row centre (hexagon inradius > 8 m). Cached per
  stored chunk (`rebin_assign` ByteLRU, 12 MB, `NE_REBIN_MB`).
* `rebin(chunk)` — apices leave to / arrive from the 6 neighbouring res-10
  chunks **of the same KG file**. Order is deterministic: by cell row, own
  apices first (stored order), arrivals by (source chunk, source row).
* **Off-file apices**: target cell is no row of that KG file (edge of the KG's
  cell set; ≈ 0.05 %, ≈ 0.7 % with a frame) → not served from that file; it
  is a twin of what another file serves or comes back as an added tree
  (next section).
* `Chunk.t_src = (src_r10[], src_idx[])` (qa=False loads) names the stored
  NET1 row of each served apex → `ne_rebin.marker_keep` translates per-stored-row
  verdicts (the `tree_rule` header marker).

## Off-file apices → added trees (`rebin_added.py`, 2026-11-22)

No tree disappears because of rb1. Every off-file apex is decided once, per KG file:

| verdict | meaning | outcome |
|---|---|---|
| `qa` | the static apex rule hides it in its own file (header marker, ≥ 60 m) | not a tree anyway |
| `dup` | **twin rule `tw1`** against an apex another KG file serves | already in the inventory |
| `dup_frame` | files in different position frames, other apex at exactly `p − (frame_src − frame_other)`, dh ≤ 0.25 m | same stored apex |
| `dup_added` | twin (same rule) of an already kept off-file apex of another source file; first in (kg, r10, idx) order stays | counted once |
| `add` | everything else | system-added tree |

Invariant per KG file, asserted by `ne_rebin.check()` and `tests/test_rebin_added.py`:
**stored == served + added + dup + qa (+ capped)**.

**Decision (Check B): Option 1.** Added trees stay out of every cell figure — `tree_h` / `tree_n` /
`canopy_h_p*` remain the measured apex set of the cell's own file. An `add` appears only in the
`/trees/bbox` `added` block, written through the same path as a `missing` report (`tree_user`, op add,
reporter = `tree_user.system_reporter('auto-rebin')`). No new public field: `h_m` (1 dp) is public, the
exact measured attributes are in `note` (`auto-rebin rb1.tw1 <reason> kg=<src> h_m= crown_m2= class=
vitality=`), provenance in `client_id` = `auto-rebin:<kg>:rb1:<no_row|other_file>:<r10 hex>:<idx>:<v>`.
`reason`: `no_row` = the target cell is in no file, `other_file` = another file serves it. The twin rule
runs on `no_row` candidates too (a neighbouring cell of another file can hold the same tree).

### Twin rule `tw1` — height-aware, measured

D = Chebyshev offset (m) of the served positions, dh = |h − h_other|. Twin when dh ≤ the table:

| height of the off-file apex | D = 0 | D = 1 | D = 2 |
|---|---:|---:|---:|
| < 5 m | 0.25 | 0 | 0 |
| 5–10 m | 2 | 0 | 0 |
| 10–20 m | 2 | 0.5 | 0 |
| ≥ 20 m | 2 | 2 | 2 |

plus: where the tolerance is the full 2 m (`TWIN_COVER`), an **at-least-as-tall** other apex is a twin
at any dh — the tall off-file apex is kept only when it is > 2 m taller than what the other file has
there (the taller reading matters for biomass; the lower one is covered). Nothing is a twin at D ≥ 3.

Why it depends on height (`tools/rebin_twin_model.py`, 132 KGs, fixed seeds, 10.3 M stored apices,
13 746 off-file): P(twin) = 1 − chance / observed per (height class, D, dh band). *Chance* = density of
other-file apices 8–12 m around the apex × g(D), g = how often ONE file holds two different apices at
that offset (relative to 6–8 m). g at D = 2 is 1.63 (< 5 m), 0.82, 0.52, 0.15, 0.02 (≥ 30 m): low
trees stand close with similar heights, so a near pair is often two real trees (e.g. < 5 m, D = 2,
dh ≤ 0.25: 49 observed / 54 chance → no signal); next to a tall apex the detector never puts a second
one, so any near pair is the same tree. A cell is a twin cell when P ≥ 0.5 (smallest expected count
error), contiguous from dh = 0. One documented exception: 20–30 m, D = 2, band 0.5–1 m is exactly 50 %
and 1–2 m is 73 % (pooled 62 %) → tolerance 2.

Expected error in the sample (pairs called twin / chance pairs among them = real tree not added /
twins left among the kept = double count): < 5 m 627 / 47 / 4 · 5–10 m 1791 / 70 / 49 ·
10–20 m 4194 / 137 / 134 · 20–30 m 4214 / 134 / 51 · ≥ 30 m 868 / 2.6 / 15.5. Both errors are a few
per cent of the off-file set, i.e. ~10⁻⁵ of the inventory, and smallest where it matters (tall trees).

### Store, idempotency, re-runs

`data/rebin_added.sqlite`: `cand` (one row per off-file apex, key (kg, r10, idx), verdict + the
**evidence**: all other-file apices within 8 m) and `kgs` (counts, invariant, staleness token =
rule + own digest + frame tag + digests of the files compared with). `resolve()` re-decides from the
evidence, so a constant change needs no product read. `sync()` diffs wanted `client_id`s against the
live system rows in `tree_ver`: missing → add, no longer wanted (rule bump, re-upload, verdict change)
→ retract. Append-only, a second run writes nothing. One KG-level `auto-rebin` flag per KG with added
trees (layer trees, claim missing; expected = counts, observed = stored / served / off_file) — KG-level
rows hide no cell and change no ETag. Machine rows are in the `/trees/bbox` ETag revision but do not
shorten its cache lifetime (`tree_user.kgs_rev(human=True)`).

```bash
python3 rebin_added.py --scan [--budget-s N]   # resumable; only unscanned / stale KGs
python3 rebin_added.py --sync [--dry]          # resolve + write added trees / flags (after a FULL scan)
python3 rebin_added.py --status                # = the `rebin:` line on /process.txt
python3 tools/rebin_twin_model.py --report     # the model table behind TWIN_DH
```

Not covered (pre-existing, separate): apices of a file standing in cells `lost` to another file under
the one-file-per-cell rule were never served and are not off-file in the rb1 sense.

The two checks that led here:

### Check A — is an off-file apex a duplicate of what another KG file serves?

`tools/rebin_check_a_report.py` (92124, 01005, 61047, 30003 + 25 frame + 25 plain KGs, seed 1; 3.75 M stored
apices, 7 884 off-file = 0.040 % plain / 0.63 % frame; 191 with the target cell in no file). Nearest apex
served by another file (after the one-file-per-cell rule), 7 693 `other_file` apices:

| distance \ abs(dh) | = 0 | ≤ 0.5 | ≤ 1 | ≤ 2 | > 2 m |
|---|---:|---:|---:|---:|---:|
| = 0 m | 2883 | 175 | 32 | 13 | 11 |
| 0–1 m | 1088 | 128 | 16 | 12 | 20 |
| 1–2 m | 1404 | 206 | 50 | 44 | 41 |
| 2–3 m | 387 | 104 | 39 | 50 | 47 |
| 3–5 m | 41 | 93 | 80 | 91 | 137 |
| > 5 m | 79 | 72 | 44 | 98 | 208 |

Per-axis offset max(abs(dx), abs(dy)): share with abs(dh) ≤ 0.5 m = 98 % (0 m), 95 % (1 m), 88 % (2 m),
then 43 % (3 m), 24 % (4 m). The gap is between 2 and 3 m **per axis**, not at 2 m Euclidean (the 2–3 m row
still holds 387 exact-height twins: (2, 1) and (2, 2) m offsets between two rasters).

* **Threshold**: duplicate = another file serves an apex with abs(dx) ≤ 2 m **and** abs(dy) ≤ 2 m (≤ 2.83 m) and
  abs(dh) ≤ 1 m. Plain KGs: 93.1 % of off-file apices are duplicates (83 % at identical position and height);
  frame KGs: 80.4 %.
* **Frame offset**: where the source file has a position frame and the other file has none, 434 of the 1 037
  remaining apices have an exact-height twin (abs(dh) ≤ 0.25 m, 397 with dh = 0) around *position + (5, −5) m* =
  minus the frame offset; control shifts ((−5, 5), (5, 5), (−5, −5), (7, 0), (0, 7)) give 27–39. Second rule: when the
  two files' frames differ, also match at position − (frame_src − frame_other) with ±2 m per axis and
  abs(dh) ≤ 0.5 m → frame KGs ≈ 87 % duplicates. Not seen between two plain files (0–2 hits) or two frame files (8).
* National estimate (558 M stored apices, 6.6 M in the 245 frame KGs): ≈ 260 k off-file, ≈ 240 k `dup`,
  ≈ 20 k to add (≈ 1–2 k of them `no_row`).

### Check B — where do added trees (claim `missing`) count today?

| figure | added trees | where |
|---|---|---|
| `/trees/bbox` | separate `added` block; not in `count`, rows or `cell` | `ne_trees._user_blocks` → `tree_user.added` |
| `/cells/<h3>` trees list | not counted | `cells_api.table_json` (`Table.gather_trees`) |
| `tree_n`, `tree_h_max` | not counted | `cells_api._load_chunk` (`ne_rebin.rebin` → `tree_qa.patch`) |
| `tree_h` / `tree_h_n`, `canopy_h_p*` (bins, blocks) | not counted | `cells_bins.tree_cols_for` / `patch_tree_cols` |
| res-9 `tree_hh` | not counted | `cells_national` (`gather_trees` → `tree_hh_band`) |
| national sample | not counted | `cells_blocks._build_sample_locked` (bin columns) |
| posted NEO1 rows | not counted | `ne_h3.neo1_rows_from_h3b` |

No cell-figure code imports `tree_user`. Added trees are outside the one apex set, so the auto-rebin trees
must not be counted alone. Open decision: count **all** added trees per cell, or **none** (see the report of
2026-11-22).

## Identity / versioning

`tree_rule.CELL_RULE_ID = 'rb1'` → `tree_qa.THX_ID = 'thx1+rb1'`, which is in:
chunk LRU key, bin / block / national generations (`cells_bins._gen`), per-KG
ETags, published per-KG file names, `stats_unit` fingerprint, posted-rows
token (`ne_ingest`, budgeted re-post `ne_thx_repost_per_tick`). The tree-QA
store keys its entries with `digest + '|rb1'` (`tree_qa._DIG_SUF`): entries
computed against another row order are ignored until phase 0 of
`cells-bins.service` re-evaluates the KG. **Changing the rule = bump
`CELL_RULE_ID`**; everything above refreshes by itself (bins rebuild via the
hourly prewarm, ≈ 3–5 runs for all 23 k bins).

Chunk index `chunk.hmax` (`cells.sqlite`, /trees early exit) must describe the
chunk **as served**: `python3 ne_rebin.py --bound-hmax` (index-only ring max,
safe upper bound, seconds) before a rule change goes live, then
`ne_trees.backfill_hmax(force=True)` for exact values (resumable, meta
`hmax_exact:<kg>`). `index_kg` writes exact values for new files.

## Height lists — rounding

`tree_rule.quant_h`: `q = clip(round_half_even(2·h), 1, 254)`, list value
`q / 2` m. Apex heights are on a 0.25 m grid, so every tie (x.25, x.75) goes
to the **whole metre**: 41.25 → 41.0, 41.75 → 42.0, 41.5 → 41.5. A client
comparing with JS `Math.round(2*h)` (half up) is off by one step on every
x.25 value — use half-even, or compare `floor(2h + 0.5) − (4h mod 4 == 1)`.

## Checklist — adding a new source of position / membership change

1. Does it change the **served position**? Put it *before* `ne_rebin.rebin`
   in `_load_chunk` **and** in `FileSource.fix`; fold its version into the
   chunk LRU key and into `_assign`'s cache key (`Source.key`).
2. Does it **hide / add** apices (QA, linear noise, user reports)? Key it by
   position or by served row and apply it *after* rebin (`tree_qa.patch`,
   `ne_trees` realistic rules). Never by stored row without `t_src`.
3. Per-cell counts are only ever computed from the chunk as served — never
   from the file header `trees` count (use `ne_rebin.live_r10s` to decide
   whether a chunk can carry trees).
4. Bump an id that reaches `cells_bins._gen` so bins/blocks/national refresh.
5. Run `python3 tests/test_tree_cell_rule.py` and `python3 ne_rebin.py --check <kg>`
   (`outside` must be 0).

## Ops

```bash
python3 ne_rebin.py --check 92124          # {"outside": 0, "moved": …, "off_file": …, "capped": 0, "ok": true}
python3 tests/test_tree_cell_rule.py       # synthetic + live (/trees ≡ cells, the 3 reported apices)
python3 tree_qa.py --status                # QA store re-evaluation progress after a rule bump
python3 rebin_added.py --status            # off-file apices: added / dup / qa, invariant
python3 tests/test_rebin_added.py
```
