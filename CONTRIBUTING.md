# Contributing

Thanks for looking. This is a research pipeline that happens to run as a
service; contributions that improve classification quality, data
honesty, or single-machine operability are the most welcome.

## Before you start

* Read `AGENTS.md` (navigation hub + invariants) and the doc for the
  module you touch (`docs/`).
* Open an issue first for anything that changes a product format
  (`docs/v2.1-product-spec.md`), the public API contract
  (`public_docs.LLM_TXT`), or the object-type list.

## Development loop

```bash
python3.12 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
python3 tools/fetch_models.py          # model weights (not in git)
cp .env.example .env                   # defaults are fine for a dev box
make check                             # tests + grep gates
```
<!-- VERIFY: `make check` target name once the Makefile lands -->

Run one unit test directly with `PYTHONPATH=. python3 tests/test_<x>.py`.
Nothing in `tests/` needs network access or secrets.

## Rules of the road

* Keep the invariants in `AGENTS.md` (type/colour sync, feature order,
  `safe_load` for every pickle, operator gate on mutating routes,
  exceptions from upstream credential problems are never swallowed).
* No property-shaped or ownership data anywhere — not in products, not
  in logs, not in tests (`tests/test_cells_vocab.py` enforces the
  vocabulary on the public tier).
* New public endpoints: document in `public_docs.LLM_TXT` and
  `docs/public-api.md`, add to `tools/golden_public.py`.
* Commit messages: imperative, one topic per commit; explain *why* in
  the body when it is not obvious.
* Security issues: see `SECURITY.md`, not the issue tracker.

## Licence

By contributing you agree that your code is released under the MIT
licence (`LICENSE`). Data and model artefacts keep their own licences
(`docs/attributions.md`).
