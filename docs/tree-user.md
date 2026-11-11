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
