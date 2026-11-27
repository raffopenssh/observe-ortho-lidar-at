# User-reported tree species + user-added trees (`tree_user.py`)

Public contract: `public_docs.FLAGS_TXT → #tree-species`. Test: `python3 tests/test_tree_user.py`
(temp DB; apex matching uses the live NE corpus).

## What it is

* **In** through the existing flag door: `POST /api/v1/flags` with `claim: species` (or just
  `{lon, lat, species}`) names the tree at a point; `claim: missing` on layer `trees` with a point
  adds a tree we have no apex for (the classic flag row is still written — old clients see no change).
* **Key = position**: EPSG:3035 integer metres of the apex matched at write time
  (`_match`: nearest apex within its crown radius clamped to 4–10 m, or an added tree ≤ 3 m), or the
  reporter's rounded point for an added tree. No ids. `missing` ≤ 2 m from an apex is refused.
* **Versions**: table `tree_ver` in `data/public_flags.sqlite` is **append-only** (~65 B/row incl.
  index; strings are `dict` ids, kinds `taxon` / `epoch` / `source`). `rev` (rowid) = global revision,
  per-tree row count = `v`. ops: 0 species, 1 add, 2 retract (own), 3 moderated (`ref` = hidden rev;
  `POST /api/v1/trees/taxa/<rev>/moderate`, admin token). Each version stores the product epoch it was
  made against. A reporter repeating the same value writes nothing. Caps: 200 versions/tree, 5 M rows,
  3000 free-form taxa.
* **Measures**: any tree report may carry `dbh_cm` (1–600, stored mm in `dbh_mm`) and `h_m` (the
  reporter's own height, 1–70, `uh_dm`); alone they use `claim: tree` (alias of `species`). Re-sending a
  different value is a new version; current value = median of each reporter's latest. The LiDAR
  `height_m` / `h_m` is never replaced — the reported height is served as `h_user_m` / `height_user_m`
  (`h_dm` in the table is only the apex height at match time, used for the orphan re-attach).
* **Withdraw** = op 2 by the same reporter hash: `"species": null` or `"retract": true` with a tree identity
  (`public_flags._names_tree`: layer / claim optional — never a generic flag row). Nothing of that reporter on the
  tree → 403 `reason: not_yours`; withdrawn before → 200 folded + `tree.warning`.
* **Current answer** (`_derive`): each reporter's latest live version votes; plurality, tie → newest.
  Cached per KG (`_state`), revalidated across workers with one `MAX(rev)` query per request.
* **Out**: `ne_trees.collect` → `_user_blocks`: sparse `user {i, taxon, n, v[, h_user_m, dbh_cm]}` + `added {…}`;
  rows get `taxon/taxon_n/taxon_v[/height_user_m/dbh_cm]`; `fields=taxon,h_user_m,dbh_cm` dense columns. Nothing is emitted for KGs without reports, so
  bodies / ETags / the 1-day cache of untouched KGs are unchanged; touched KGs get `u=<rev>` in the
  ETag, a top-level `rev` (start of a `/trees/taxa?since=` poll) and max-age 300 (`public_api.TREES_USER_MAX_AGE`). Delta feed `/api/v1/trees/taxa?since=`,
  history `/api/v1/trees/taxa/history`, dictionary `/api/v1/trees/taxa/dict` and `/api/v1/trees/taxa/top`
  (next section).
* After a KG rebuild an apex may move: exact (x, y) join first, then ≤ 1.5 m **and** height within 3 m
  of the height stored with the version (`ORPHAN_*`); otherwise the name is simply not shown (still in
  `/trees/taxa` and history).

## Dictionary + resolver (`tree_taxa.py`, rev 2 since 2026-11-11)

Test: `python3 tests/test_tree_taxa.py` (no DB / corpus). Contract: `#tree-species` → dict columns, RESOLVER, `/taxa/top`.

* **Table**: `_S` = species lines **in rank order** (`taxon|de|en|class|codes|alt`), `_G` = one row per genus
  (auto-ranked right after its first species, de shown as `… (Gattung)`), two group rows last. 278 rows.
  `REV` + `DIGEST` are pinned by the test: any edit of the table fails it until `REV` is bumped.
* **Rank** is an ordering for suggestion lists, not a census: forest species by the published ÖWI 2016/21 shares
  (written from the published order, no table was downloaded), street / park trees interleaved by the Vienna
  Baumkataster (`ogdwien:BAUMKATOGD`, 233 k trees, tallied 2026-11 — only Vienna; Graz / Linz / Salzburg /
  Innsbruck were not tallied), orchard trees by the usual Streuobst order.
* **Codes**: forestry abbreviations by hand; EPPO from Wikidata P3031 by taxon name (`tools/taxa_eppo.py --write`
  rewrites the `_EPPO` block; 5 via a listed synonym, 11 rows have none). No numeric ÖWI / BFW codes (no citable source).
* **Collision rule**: `build_index()` → `CLASHES` must be empty. Decisions are listed in the module docstring
  (Ki = Kiefer / Kir = Kirsche, GEr vs GrEr, SPa vs WPa, no SWei, genus vs species meaning of the plain German
  names, Kastanie = Rosskastanie). 8 two-plus-two codes are shared by two species (AcCa, AcSa, CrLa, PlOr, PoCa,
  PrCe, PrSe, TiEu) → 400 `reason: ambiguous`.
* **Keys** (`keys()`): lower-case, ß→ss, diacritics dropped, ä/ö/ü *also* as ae/oe/ue, `×`/` x ` dropped, hyphens /
  dots / spaces removed. Canonical hybrids keep the ` x ` form (`Populus x canadensis`) — stored reports use it.
* **`/taxa/top`** (`tree_user.top`): `named` from the per-KG state cache; `classes` = `ne_trees.collect` over the
  bbox (≤ 0.01 deg², tallest 20 000) or the national estimate (600 random chunks × ratio, cached a week in
  `data/tree_class_tally.json`; first call after expiry takes ~5 s).

## Ops

```bash
curl -s 'localhost:8100/api/v1/flags/stats?pretty=1' | grep -A9 '"trees"'
curl -s 'localhost:8100/api/v1/trees/taxa?pretty=1'            # national, current answers
sqlite3 data/public_flags.sqlite 'SELECT op, COUNT(*) FROM tree_ver GROUP BY 1'
```

Not done yet: feeding confirmed taxa (n ≥ 2) into RF / species-model training labels; a picker in our
own `/segmentation` UI (map clients are the UI).

## Moderation by contributor (`tree_mod.py`, 2026-11-27)

Anybody can report; the only thing we know of a contributor is the 8-byte salted `reporter` hash
(unchanged). `tree_mod.py` reads the reports **by that hash** — internal, CLI only, no route returns a
handle or a label. Test: `python3 tests/test_tree_mod.py`. `/process.txt`: `tree_mod:` (counts only).

```bash
python3 tree_mod.py --list [--days 30]     # one line per contributor: versions, trees, species, measures, adds,
                                           # flags, conflicts (another reporter named the tree differently),
                                           # burst (most versions in 5 min), KGs, taxa, key holder, `near` =
                                           # other hashes reporting ≤ 300 m and ≤ 48 h away (same person, new browser?)
python3 tree_mod.py --show 1a2b3c4d        # every version of a contributor / group, with lon/lat + what is shown now
python3 tree_mod.py --group grp-1 1a2b3c4d 5e6f7a8b      # internal label over several hashes
python3 tree_mod.py --mask grp-1 --note "…" [--all] [--dry]   # species votes (default) or everything stops counting
python3 tree_mod.py --unmask grp-1
```

* **Store**: table `rmod(reporter PK, grp, mask, ts, note, acct)` in `public_flags.sqlite` — one row per
  moderated / labelled contributor, nothing per report. `mask` bits: 1 = species votes ignored
  (`tree_user.MASK_SPECIES`; measures, notes, states, added trees stay), 2 = every tree statement ignored.
* **Nothing is lost**: `tree_ver` rows are untouched; `_derive(rows, machine, masks)` skips the masked votes;
  `/trees/taxa/history` marks them `masked: "taxon"` / `hidden: true`. A mask also covers what the
  contributor sends later (stored, not counted). Unmask = everything counts again.
* **Propagation without a new mechanism**: each mask change appends one op-3 row with no `ref` (hides nothing by
  itself) per tree the contributor wrote on → new `rev` → per-KG state in every worker, `/trees/bbox` ETag
  (`u2=<rev>`) and `/trees/taxa?since=` readers (row comes back with `taxon: null`) follow. ~65 B per touched tree.
* Single versions are still moderated with `POST /trees/taxa/<rev>/moderate`; flag rows with `/flags/<id>/status`.

### Accounts (prepared, nothing collected yet)

The NE server will tie its users to an **e-mail key** and an **export subscription key** (the accounting product).
The link lives in `rmod.acct`, built so that neither side can name a contributor alone:

* We never receive an address or a key. the NE server sends a **pseudonym** of its key id (its own keyed hash, one
  constant per account); we keep only `HMAC(our salt, "a:" + pseudonym)[:12]` (`tree_mod.acct_hash`). A join needs
  the NE server to present the pseudonym — accounting asks "usage of account X", we hash X and look it up.
* A keyed caller's reporter hash is `HMAC(salt, "k:<holder>:<reporter>")` and the NE server keeps sending the same
  per-browser `reporter`, so ONE link row attaches everything that browser reported before and after — no report
  is rewritten or annotated, the NE server re-sends nothing. Several browsers of one account → same `acct`.
* Erasure: `--unlink` drops the link (reports stay as anonymous as before); `rmod` rows carrying nothing are deleted.
  `note` is the operator's reason — no names in it. Handles, labels and `acct` are returned by no route.
* `--list`, `--group`, `--mask` take a pseudonym like a group label (shown as `a:<6 hex>` of our hash).

Today: `python3 tree_mod.py --link <pseudonym> <handle…>` / `--unlink <handle…>` by hand.
Planned wire (NOT read by any route yet): keyed callers add one optional field next to `reporter` on
`POST /api/v1/flags` — `"account": "<pseudonym>"` (body or `X-Reporter-Account`), once or on every call; we upsert
`rmod.acct` for that hash. That is all the NE server has to do; usage per account = `contributors()` grouped by `acct`.

## Machine-added trees are apices, not reports (2026-11-27)

`rebin_added.py` writes off-file apices through the add path (`reporter = system_reporter('auto-rebin')`,
356 k rows). They used to come back in the `/trees/bbox` `added` block and in `/trees/taxa` — clients drew every
one as an edited tree. Now:

* `_derive`: a system writer's add is `sys` (not `added`); its height / note are the apex record, not a
  statement. `_load` parses the note (`h_m= crown_m2= class= vitality=`) into `sys_h / sys_crown / sys_sp / sys_vit`.
* `/trees/bbox`: `tree_user.system_rows` → `ne_trees.collect` appends them as **ordinary rows** (kg_code = the file
  that measured the apex, exact h_m / crown / class / vitality; same limit and height window). `off_file=0` leaves
  them out = exactly the apex set of the cell figures (`tests/test_tree_cell_rule.py`). ~7 B/apex instead of
  ~100 B in `added`. ETag token `u2=`.
* `added` = trees people added, only. `/trees/taxa` lists a machine tree only once a person wrote on it (then as a
  named apex: `added 0`, no `client_id`); `machine=1` lists all (national since=0: 151 trees instead of 356 k).
* A report on one binds like on any apex (`matched: "apex"`, crown reach, `height_m`), by point or by its
  `auto-rebin:…` client_id; it shows in the `user` block at the row. `history?client_id=auto-rebin:…` still works.
* Not applied to these rows: reader `spurious` flags / linear-noise hiding (they are keyed on file chunks).

## Undo (`"retract": "last"`)

Hides only the reporter's latest live version of the tree; stored as an
op-3 row (`ref` = hidden rev) **carrying the reporter hash** (a moderation
row has `b''`), so every `hidden` reader (`_derive`, `record`, `_mine_ok`,
`history`) follows unchanged. `/history` labels it `op: "undo"`. Undoing a
retract restores what was there; undoing the add removes the tree. Answers
201 `tree{op:"undo", undone_rev, undone_op}` / 200 folded / 403 `not_yours`.

Withdrawing an added tree (`retract: true`, or `"last"` when the add was the
latest) also deletes the reporter's `trees` / `missing` flag row that the add
wrote (`public_flags._drop_added_flag`; answer carries `flag_removed` + `tally`).
Undoing such a withdraw restores the tree but not that flag row.

## State since the flight (`"state": "logged" | …`, 2026-11-11)

A tree that was cut / burnt / blown down after the LiDAR date. The apex stays served (true when flown); the state
is a training / change label.

* Vocabulary `public_flags.TREE_STATES` (append only, ≤ 14): standing, logged, windthrow, burnt, dead, broken,
  infested, gone; aliases `TREE_STATE_ALIAS`. `claim: logged|burnt|…` is rewritten to `claim: state` + `state`.
* **Storage = nothing new but one column**: the state is the existing `status` nibble of `tree_ver.meta`
  (index + 1; 15 = `ST_NONE`, the reporter took the statement back); the date is `sdate`, packed by
  `pack_date` ((year − 1990) × 512 + month × 32 + day, 0 = not given → 2 bytes, precision kept). No table, index
  or `report` row; a version row stays ~65 B.
* **Current state** (`_derive`): each reporter's latest status votes (standing too), plurality, tie → newest;
  standing winning = no state. `st['state']` / `st['sdate']` per KG state; a tree with only a state is `live`.
* **Reverse**: `"state": null` or state claim + `retract: true` → `op: unstate` (nibble 15; species / measures stay);
  `retract: "last"` undoes it as any version; `retract: true` without a state claim withdraws everything as before.
* **Wire**: `state` / `state_date` appear in `user` / `added` / `reported` only when a tree of the result has one —
  KGs without reports stay byte-identical (same ETag, 1-day cache). `/trees/taxa?state=logged|any` = training set.
* Linear noise is a different thing (the apex was never a tree): `claim: spurious` + `expected.hint: "linear"`,
  `docs/linear-noise.md`.

## Same tree (`expected.hint: "crown"`, `tree_crown.py`, 2026-11-20)

Several apices on one broad crown. A reader flags the extra (lower) top: layer `trees`, claim `spurious`,
`expected {absent, hint:"crown", of:{tree_id | lon, lat}}`. Contract: `#same-tree`. Test: `python3 tests/test_tree_crown_note.py`.

* **No table, no recompute.** It is a `report` row. Only the subject key differs: `skey = -(x << 32 | y)` of the
  extra apex (EPSG:3035 m; below `-2^32`, KG subjects are `-1…-99999`, cells are positive), so several tops in one
  res-12 cell do not fold into each other. `cell` / `lon_e6` / `lat_e6` are filled as for any point flag, so every
  read (`_where`, heat, stats) follows; `tally()` counts by `cell` for positive ids. The row's `kg` is the KG **file**
  that serves the apex (not the KG of the point — `ne_trees._flags` is per file), `epoch` that file's epoch.
* **Write** (`tree_crown.bind`, called from `public_flags.record`): subject → apex ≤ 2.5 m (1.5 m for a tree_id,
  preferring `observed.h_m`), `of` → apex ≤ 25 m away, not the subject; a parent that is itself an extra top is
  replaced by its root (≤ 8 hops; loop → `bad_of`). Stored `of` = `{tree_id?, lon, lat, x, y}`; `observed` gains
  `h_m` / `crown_m2` of the extra top (what the merge and the label need — no corpus read later).
* **Retract** needs no `expected`: `tree_crown.own_key` finds the reporter's apex-keyed row within 2.5 m of the
  point (own row → own plain cell row → own withdrawn marker → somebody else's = 403).
* **Read** (`ne_trees._flags`, the per-KG flag cache that already exists, 10 min): apex-keyed rows hide exactly that
  apex and yield `tree_crown.resolve` → (parent key, m² to add), the merged tops, the apices to hide, chains resolved again; `collect` adds them to the
  parents present in the result (`tree_crown.apply`, one `searchsorted`, only when a KG of the bbox has such flags).
  The area is added only while `row.epoch` = the epoch served (after a rebuild the top may be gone — hiding still
  follows the usual 2.5 m rule).
* **ETag** (`ne_trees.flag_rev` / `flags_token`): with `realistic=1` the state of the non-machine `report` rows of the
  KGs of the bbox (a hash of subject, claim, status, observed, expected, epoch — a folded repeat changes nothing) joins the `/trees/bbox` ETag — only where a
  KG has such rows, so untouched KGs keep their ETag and their day-long max-age (flagged: 300 s). The same token
  validates the per-KG flag cache (`_flags`), so every gunicorn worker serves the new state on the next request.
  Cost: `PRAGMA data_version` per request; one indexed query per KG only after the DB changed.
* **Still shown** (`tree_crown.merged_block`): `/trees/bbox` `realistic=1` carries `merged` = the hidden tops of the bbox /
  height window with their root (`of_x`, `of_y`, `of_i` = row in the answer), `n` reporters, `against` objections.
  Built from the report rows (`observed` h_m / crown_m2) — no corpus read. Not part of `count`.
* **Objection** (`tree_crown.merged_at`): `looks_right` on layer `trees` at a merged top is stored apex-keyed like the
  flag (same `skey`, KG file). `tree_crown.resolve`: merged while reporters > objections, or status `confirmed`.
* **Labels**: `python3 tree_crown.py --labels [--kg N]` → JSONL `{kg, x, y, h_m, crown_m2, of_x, of_y, dist_m,
  reporters, against, confirmed, superseded}`; `python3 tree_crown.py` = counts. Nothing consumes them automatically yet.

## Note (`"note"`, 2026-11-20)

* Column `tree_ver.note TEXT` (NULL = none, `''` = taken back). No index; a version without a note costs 0 bytes.
* `_derive`: each reporter's latest note (cleared by `''`, a retract, an undone version); the tree shows the newest.
  A tree with only a note is `live`.
* ops in answers: `note` (version carries nothing but a note), `unnote` (identity + `"note": null` only; 403 when the
  reporter has none). Row `op` stays 0 / 1.
* Reads: `_public`, `/history` (`note: null` = taken back), `annotate` / `added` / `reported` (`note` only when a
  tree of the result has one), `/trees/taxa` last column; `notes=0` everywhere. Moderation = the version's
  `/trees/taxa/<rev>/moderate`.
* `/trees/bbox` json / geojson now also passes `reported` through (`landscape_query.trees_bbox` dropped it).

## Position frames (`ne_pos_fix.py`, 2026-11-21)

Tree / structure positions of a KG can change after a client downloaded them
(read-time (−5,+5) m fix of the shifted v2 products, `docs/hillshade-seams.md`).
Rule: **a report made with any copy lands on the same tree; nothing is dropped;
a refresh returns today's positions.**

* **Partly shifted KGs** (verdict `partial`, 2026-11-22). The 5 m shift is a
  property of a *processing tile* (restored without raster sidecar → pasted
  5 m off; its neighbour not), so a parent can be shifted in some ~1.6 km
  tiles only — `mixed` / `unclear` in the whole-KG pass, and the source of
  `rebin_added`'s `dup_shift` pairs. `python3 ne_pos_fix.py --verify-tiles`
  (throttled on host RX, resumable via entry field `tv`; unit
  `ne-pos-tiles`) measures one 250 m BEV window in each tile's *core*
  (inside exactly one tile envelope = no feather blend; windows of the
  earlier pass that lie in a core are reused for free) and stores `tiles:
  [[x0,y0,x1,y1, 1 moved | 0 kept | -1 unmeasured]]` (EPSG:3035 envelopes
  from `kg_v2_store … data_quality.tiles`). Serving: every tree / structure
  word is tested at its **stored** position — owner tile = highest feather
  weight, first tile on ties (the mosaic's own rule, `tile_owner`) — and
  moved only if that tile is `1` (`moved()`, `fix_words(…, cells)`);
  unmeasured tiles stay. All measured tiles shifted → plain `shift`; none →
  `zero`. Frame token is the same `p-5+5` (`partial: true` on
  `/trees/frame` and the flag, which lists `moved_tiles`); the cache tag
  carries a digest of the tile list. `reframe` / `frame_vote` /
  Unmeasured tiles of a `partial` KG can still be decided later (a rebin
  pair that appears after the pass puts the KG back on the to-do list: no
  BEV read, decided tiles are final, only `-1 → 1` moves reports, `ts_fill`).
  `_migrate_stores` / `shift_lonlat_points` / `trees_v3.read_apices` move a
  stale position only where `moved()` says so. KGs already served moved as
  a whole are never re-measured automatically (that would be a second
  frame epoch): `status()['per_tile']['review']`. Test
  `python3 tests/test_pos_fix_partial.py`.
* **Record = flag.** One KG-level machine report per decided KG: layer
  `trees`, claim `misplaced`, source `auto-shift`, `expected {hint:"shift",
  applied, moved_m:[dx,dy], crs, frame, since[, until], verdict}`, `observed
  {windows, shifted, err_m{moved, stored}}`. `GET /api/v1/flags?source=auto-shift`
  (machine source = selector, no bbox needed) lists what was done to the 2.4
  product; `mixed` KGs are listed with `applied:false`. KG-level rows never
  hide a cell. **Overrule**: set the report `rejected` → the hourly
  `ne_pos_fix.py --sync` (`ne-linear.service` ExecStartPost) calls `unfix`:
  shift off, every user row moved back, copies made while it was on resolve
  (`frame p0`, `stale p-5+5`).
* **Frame token**: `p0` = positions as stored, `p-5+5` = moved. `/trees/bbox`
  carries sparse top-level `frames {kg: token}` only for KGs that ever moved
  (zero bytes elsewhere). Clients store it with a download and echo it as
  `"frame"` (or `"as_of"` = download time) on every report.
* **`tree_user.reframe`** (called from `public_flags.record` for tree-layer
  bodies, before subject / matching): `frame` echo → `as_of` → exact evidence
  (the coordinate sits ≤ 0.75 m on an apex in one frame only, or the
  `tree_id` is bound in one) → the reporter's unambiguous reports of the last
  days in that KG (`tree_ver.fr`: 1 stale-moved, 2 current, 3 ambiguous) →
  today's frame + `ambiguous` hint. Moves `lon/lat/tree_id/expected.of` in
  place; the answer carries `frame {frame, sent, by, moved_m, tree_id?}`
  (`tree.frame` on tree reports, top-level on plain flags) + a "refresh"
  warning. Free points (no apex in either frame), added trees and `missing`
  flags are real-world positions and never move.
* **Bulk check before an import**: `POST /api/v1/trees/frame {points:[[lon,lat]…]
  | tree_ids:[…]}` → per KG `{frame, current, stale_hits, current_hits,
  confident}`; `GET` lists all frames + `frames_rev`.
* **Migration** (`ne_pos_fix._migrate_stores`, one transaction per KG): `tree_ver`
  rows (not added trees), tree-layer point flags that name one of the KG's
  apices within 2.5 m (selected spatially — the row's `kg` is not trusted),
  crown flags (`skey` + `expected.of`). Rows move WITHOUT a new rev →
  `/trees/taxa` carries `frames_rev`; when it changes, delta readers re-read
  from `since=0`. `tree_user.state` is also keyed on `ne_pos_fix.tag(kg)`.
* **v3 inventory** (`trees_v3.read_apices`, forestry): serves moved e/n, the
  new `tree_id`, plus `tree_id_prev` (the id of older exports) and
  `tree_id_product`; a stale id posted to `/flags` resolves by evidence.
* **Regeneration**: when a shifted KG's product is rebuilt on the true grid,
  call `ne_pos_fix.retire(kg)` (`--retire`) — shift off, token and frame
  history kept. Not wired into any pipeline yet.
* Not migrated: cell-keyed flags on the `structures` / cells layers (cell ids
  do not move; 7 m vs 16 m cells).
* Tree-layer point flags are filed under the KG whose **file** serves the
  apex (`tree_user.apex_kg`), not the cell's KG — KG rectangles overlap.
  `tree_user.rekey_tree_flags()` repaired older rows once.

Test: `python3 tests/test_tree_frame.py`.
