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
* **Current answer** (`_derive`): each reporter's latest live version votes; plurality, tie → newest.
  Cached per KG (`_state`), revalidated across workers with one `MAX(rev)` query per request.
* **Out**: `ne_trees.collect` → `_user_blocks`: sparse `user {i, taxon, n, v}` + `added {…}`; rows get
  `taxon/taxon_n/taxon_v`; `fields=taxon` dense column. Nothing is emitted for KGs without reports, so
  bodies / ETags / the 1-day cache of untouched KGs are unchanged; touched KGs get `u=<rev>` in the
  ETag and max-age 300 (`public_api.TREES_USER_MAX_AGE`). Delta feed `/api/v1/trees/taxa?since=`,
  history `/api/v1/trees/taxa/history`, dictionary `/api/v1/trees/taxa/dict` (`TAXA`, ~90 rows with
  de/en names and our model class).
* After a KG rebuild an apex may move: exact (x, y) join first, then ≤ 1.5 m **and** height within 3 m
  of the height stored with the version (`ORPHAN_*`); otherwise the name is simply not shown (still in
  `/trees/taxa` and history).

## Ops

```bash
curl -s 'localhost:8100/api/v1/flags/stats?pretty=1' | grep -A9 '"trees"'
curl -s 'localhost:8100/api/v1/trees/taxa?pretty=1'            # national, current answers
sqlite3 data/public_flags.sqlite 'SELECT op, COUNT(*) FROM tree_ver GROUP BY 1'
```

Not done yet: feeding confirmed taxa (n ≥ 2) into RF / species-model training labels; a picker in our
own `/segmentation` UI (map clients are the UI).
