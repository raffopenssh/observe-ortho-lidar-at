# Linear noise in the tree apices (`ne_linear.py`)

Thin elevated linear objects leave a 1–2 m wide ridge in the nDSM; the apex
finder turns it into a chain of small "trees". `ne_linear.py` finds those
chains from the apex columns of the NE corpus alone (no raster), hides
their apices on `/api/v1/trees/bbox` (`realistic=1`) and files one machine
report per run in the public flag store.

**Rule #1: never discard a real tree.** A missed chain is fine. Every gate
in `accept()` exists because a hand-checked false positive (forest aisle
with regrowth, orchard rows, hedge, planted row) looked similar.

## Signature (what the gates test)

| property | gate | why |
|---|---|---|
| straight | lateral rms ≤ 0.8 m over ≥ 80 m, ≥ 15 apices | apex x/y are 1 m quantised |
| spacing | mean 2.5–7.5 m | ridge → one apex per finder window; denser = hedge / fence, wider = planted row |
| crowns | median ≤ 12 m² (thin) or ≤ 22 m² with a clean U | crown = ridge width × spacing |
| smooth height | chord test: median \|apex − chord of its neighbours 6–25 m away\| ≤ 0.25–0.7 m | a hanging line is smooth; tree tops jump by metres |
| U shape | ≥ 1 sag ≥ 1 m between supports ≥ 25 m apart, sag ≤ 5 % of span + 1.5 m; gaps where the line dips under the 3 m apex floor count as sag | spans of overhead networks: 30–60 m, 80–150 m, 250–450 m |
| empty flanks | ≤ 0.15 apices per member in the 3–9 m band | orchard / vineyard rows |
| open ground | ≤ 6 apices ≥ 8 m per 100 m within 40 m (≤ 25 for `strong` runs: ≥ 200 m, ≥ 40 apices, one height quantum of scatter, sag) | forest aisles look alike |
| members | only small-crown apices within 2 m of the axis and 1.5 m of the height profile | a real tree in the corridor stays |

Thin chains without a visible sag pass only when very clean (chord ≤ 0.45 m,
≥ 20 apices). Species / vitality columns were tested and do not separate
(small real trees are mostly `unknown` too).

Sample of 250 random KGs (2026-11): 190 runs, 51 km, 0.05 % of all apices;
24 randomly drawn runs checked by eye, none a row of trees.

## Store, flags, read path

* `data/linear_noise.sqlite` (primary only, **not served by any route**):
  `kg(kg, digest, ver, …)` and `run(kg, cell, x0, y0, x1, y1, n, len_m,
  h_med, sags, sag_m, chord, crown_med, lat_rms, keys)` — endpoints in
  EPSG:3035, `keys` = sorted `uint64 (x << 32 | y)` of the member apices.
  A KG is recomputed only when its `neo1_digest` or `ne_linear.VERSION` changes.
* Flags: one report per run — layer `trees`, claim `spurious`, source
  `auto-linear`, subject = the run's middle apex, `expected:
  {"absent":true,"hint":"linear","n":…,"len_m":…}` — written by
  `public_flags.auto_sync` in one transaction per KG (no snapshot, no rate
  limit). A run seen in two overlapping KG files shares one report. Runs
  that vanish go `stale`; a human verdict (`confirmed` / `rejected`) is
  never overwritten.
* **Overrule**: set the run's report to `rejected`
  (`POST /api/v1/flags/<id>/status`) → its apices are served again within 60 s.
* `ne_trees.collect` hides exactly the member apices (`dropped_linear` in the
  result), for every KG file that carries them. `auto-linear` reports are
  excluded from the whole-cell rule that human `spurious` flags trigger.
  The `/trees/bbox` ETag includes `ne_linear.state_token`.
* Cost on the read path: a serving process holds ONE in-memory snapshot of the
  store (`_snapshot`: `{kg: sorted keys}` + a 256 m grid set per KG), refreshed by
  a `stat()` every 10 s and re-read only when the file changed. Per chunk the
  test is a set lookup against the chunk's cached grid ids; only chunks that can
  hold a hidden apex do array work. The token is a content signature (`kg.sig`),
  so a recompute with the same result changes neither rows, flags nor ETags.
  The hourly job runs `Nice=19`, `CPUQuota=70%`, idle IO, in its own process.
* Humans can report chains we missed: `claim: spurious`, layer `trees`,
  `expected: {"absent": true, "hint": "linear"}`. `python3 ne_linear.py
  --misses` lists those not on any stored run — the tuning set.

## Operating it

```bash
python3 ne_linear.py --status
python3 ne_linear.py --kg 77214 --force      # one KG
python3 ne_linear.py --backfill              # what ne-linear.timer runs hourly
python3 ne_linear.py --misses
python3 tests/test_ne_linear.py
sqlite3 data/linear_noise.sqlite 'SELECT kg, n, len_m, h_med, sags FROM run ORDER BY len_m DESC LIMIT 20'
```

Changing a gate: bump `VERSION` (every KG is redone by the timer, ~1.5 h of
one niced core nationwide) and extend `tests/test_ne_linear.py`.
