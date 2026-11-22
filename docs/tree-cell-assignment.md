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
  cell set; ≈ 0.05 %, ≈ 0.7 % with a frame) → not served from that file. An
  apex without a cell row cannot be counted anywhere; where another KG file
  stores that cell, its own inventory covers the spot (one file per cell,
  `cells_api.lost_cells`).
* `Chunk.t_src = (src_r10[], src_idx[])` (qa=False loads) names the stored
  NET1 row of each served apex → `ne_rebin.marker_keep` translates per-stored-row
  verdicts (the `tree_rule` header marker).

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
python3 ne_rebin.py --check 92124          # {"outside": 0, "moved": …, "off_file": …, "ok": true}
python3 tests/test_tree_cell_rule.py       # synthetic + live (/trees ≡ cells, the 3 reported apices)
python3 tree_qa.py --status                # QA store re-evaluation progress after a rule bump
```
