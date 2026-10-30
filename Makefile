# observe-ortho-lidar-at — developer / operator targets.
#
#   make venv          create .venv (system site-packages for GDAL) + requirements
#   make check         unit tests + release grep gates + pip-audit (if installed)
#   make run-app       app.py on SRTM_APP_BIND:SRTM_APP_PORT (operator tier, loopback)
#   make run-public    public_api.py on SRTM_PUBLIC_BIND:SRTM_PUBLIC_PORT
#   make fetch-models  download classifier models (tools/fetch_models.py)

PY      ?= python3
VENV    ?= .venv
VPY      = $(VENV)/bin/python3

.PHONY: venv check test gates audit run-app run-public fetch-models

venv:
	$(PY) -m venv --system-site-packages $(VENV)
	$(VPY) -m pip install --upgrade pip
	$(VPY) -m pip install -r requirements.txt gunicorn pytest

test:
	$(PY) -m pytest tests -q

# Release hygiene gates (see AGENTS.md).  Each loop fails on a hit outside the
# documented exceptions.
gates:
	@echo '== gate 1: private tier / admin tokens'; \
	hits=$$(grep -rnE '/k/|/k\b|admin_token|umfeld_token|X-Admin-Token|Bearer' --include=*.py --include=*.html --include=*.sh . \
	  | grep -vE 'Authorization.*Bearer \{|^\./(zenodo_[a-z_]+|trees_v3)\.py:.*Bearer' || true); \
	if [ -n "$$hits" ]; then echo "$$hits"; exit 1; fi
	@echo '== gate 2: no hardcoded hosts'; \
	hits=$$(grep -rn 'exe\.xyz' --exclude-dir=.git --exclude-dir=data --exclude-dir=__pycache__ --exclude-dir=$(VENV) . \
	  | grep -vE '^\./(\.env\.example|README\.md):' || true); \
	if [ -n "$$hits" ]; then echo "$$hits"; exit 1; fi
	@echo '== gate 3: no cadastre / property-register code paths'; \
	hits=$$(grep -rniE 'parcel|cadastre|kataster|grundst|umfeld' --include=*.py --include=*.html . \
	  | grep -vE '^\./(vct_pilot|ne_h3|config|attributions|public_docs|cells_api)\.py:' \
	  | grep -vE '^\./tests/' || true); \
	if [ -n "$$hits" ]; then echo "$$hits"; exit 1; fi
	@echo 'gates: ok'

audit:
	@if command -v pip-audit >/dev/null 2>&1; then pip-audit -r requirements.txt; \
	else echo 'pip-audit not installed — skipping (pip install pip-audit)'; fi

check: test gates audit

run-app:
	$(PY) app.py

run-public:
	$(PY) public_api.py

fetch-models:
	$(PY) tools/fetch_models.py
