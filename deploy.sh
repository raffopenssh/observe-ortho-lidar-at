#!/bin/bash
set -euo pipefail

# =============================================================================
# deploy.sh — Deploy srtm-lidar-at to a new exe.dev VM instance
#
# Usage (from a fresh exe.dev VM):
#   GITHUB_TOKEN=<read-only PAT> \
#   curl -sL -H "Authorization: Bearer $GITHUB_TOKEN" \
#     https://raw.githubusercontent.com/raffopenssh/srtm-lidar-at/main/deploy.sh | bash
#
# Or clone first and run locally:
#   git clone https://github.com/raffopenssh/srtm-lidar-at.git
#   cd srtm-lidar-at && bash deploy.sh
#
# Environment variables (set before running, or edit below):
#   PEER_URL     — URL of the primary instance for coordination
#                  (e.g. https://example.invalid:8000)
#   SELF_URL     — Public URL of THIS instance (e.g. https://example.invalid:8000)
#                  Required for auto-registration with the director.
#   INSTANCE_ID  — Name for this instance (default: derived from SELF_URL or hostname)
#   GIT_REPO     — Git clone URL (default: the project repo with token)
# =============================================================================

PEER_URL="${PEER_URL:-https://example.invalid:8000}"
SELF_URL="${SELF_URL:-}"
# Repo is PRIVATE (Oct 2026). Clone needs a read-only GitHub token
# (fine-grained PAT, Contents: read). Pass it as GITHUB_TOKEN; it is saved to
# data/github_token (0600) and used by scripts/git_credential_helper.sh for
# every later `git fetch`. Never commit it. Existing peers get it pushed by
# the director automatically (/api/v1/admin/install_git_token).
GITHUB_TOKEN="${GITHUB_TOKEN:-}"
if [ -n "${GITHUB_TOKEN}" ]; then
    GIT_REPO="${GIT_REPO:-https://x-access-token:${GITHUB_TOKEN}@github.com/raffopenssh/srtm-lidar-at.git}"
else
    GIT_REPO="${GIT_REPO:-https://github.com/raffopenssh/srtm-lidar-at.git}"
fi
# Admin token for director coordination — fetch from primary if not provided.
# Required to call protected admin endpoints (e.g. peer registration). Without
# it the VM can run a web server but cannot join the cluster.
ADMIN_TOKEN="${ADMIN_TOKEN:-}"   # this peer's UNIQUE token (minted on the primary: dashboard "+ Add Peer"
                                 # or `python3 peer_tokens.py --new-peer atN`). Never the primary's master token.
MESH_TOKEN="${MESH_TOKEN:-}"     # fleet-shared, cache-manifest sync only

# Derive INSTANCE_ID from SELF_URL if not set
# e.g. https://example.invalid:8000 -> srtm-lidar-at3
if [ -n "${SELF_URL}" ]; then
    _DERIVED_HOST=$(echo "${SELF_URL}" | sed 's|https://||;s|:.*||;s|\.exe\.xyz||')
    INSTANCE_ID="${INSTANCE_ID:-${_DERIVED_HOST}}"
else
    INSTANCE_ID="${INSTANCE_ID:-$(hostname)}"
fi
PROJECT_DIR="/home/exedev/srtm-lidar"

echo "══════════════════════════════════════════════════════"
echo "  srtm-lidar-at deployment"
echo "  Instance: ${INSTANCE_ID}"
echo "  Self:     ${SELF_URL:-<not set>}"
echo "  Peer:     ${PEER_URL}"
echo "══════════════════════════════════════════════════════"

# ── 1. Clone repo ────────────────────────────────────────
if [ ! -d "${PROJECT_DIR}/.git" ]; then
    echo "\n[1/7] Cloning repository..."
    git clone "${GIT_REPO}" "${PROJECT_DIR}"
else
    echo "\n[1/7] Repository exists — pulling latest..."
    cd "${PROJECT_DIR}" && git pull --ff-only || true
fi
cd "${PROJECT_DIR}"
# Strip the token from the remote URL again; persist it for the helper.
git remote set-url origin https://github.com/raffopenssh/srtm-lidar-at.git
if [ -n "${GITHUB_TOKEN}" ]; then
    mkdir -p data && (umask 077; printf '%s\n' "${GITHUB_TOKEN}" > data/github_token)
fi
chmod +x scripts/git_credential_helper.sh
git config --local --get-all credential.helper 2>/dev/null | grep -qF git_credential_helper.sh \
  || git config --local --add credential.helper "!${PROJECT_DIR}/scripts/git_credential_helper.sh"
export GIT_TERMINAL_PROMPT=0
# Hidden tier: this peer's app.py (dashboards, docs, cadastre relay) must
# never answer unauthenticated requests — same guard as the primary. The
# director also enforces it fleet-wide via /api/v1/admin/set_private_only.
mkdir -p data && touch data/private_only

# ── 2. System dependencies ───────────────────────────────
echo "\n[2/7] Installing system packages..."
sudo apt-get update -qq
sudo apt-get install -y -qq \
    python3-pip python3-venv python3-dev \
    libgdal-dev gdal-bin \
    libgeos-dev libproj-dev \
    libspatialindex-dev \
    vnstat \
    xz-utils \
    > /dev/null 2>&1

# ── 3. Python dependencies ───────────────────────────────
echo "\n[3/7] Installing Python packages..."
pip install --quiet --break-system-packages -r requirements.txt 2>/dev/null \
  || pip install --quiet -r requirements.txt

# ── 4. Decompress RF model ───────────────────────────────
echo "\n[4/7] Preparing RF model..."
if [ ! -f data/best_model/rf_model.joblib ]; then
    if [ -f data/best_model/rf_model.joblib.xz ]; then
        echo "  Decompressing rf_model.joblib.xz (103 MB → 364 MB)..."
        xz -dk data/best_model/rf_model.joblib.xz
    else
        echo "  WARNING: No RF model found! Check git checkout."
    fi
else
    echo "  RF model already exists."
fi

# ── 5. Create data directories ───────────────────────────
echo "\n[5/7] Creating data directories..."
mkdir -p data/austria_processor/{json,gpkg,logs,tile_checkpoints,zenodo_zip_index}
mkdir -p data/shares data/gpkg_cache
mkdir -p /tmp/learned_classifier /tmp/segment_progress /tmp/segment_results
mkdir -p /tmp/copernicus_cache /tmp/hansen_cache

# Initialize empty files if missing
[ -f data/austria_processor/retry_queue.json ] || echo '[]' > data/austria_processor/retry_queue.json
[ -f data/austria_processor/failed_kgs.json ]  || echo '[]' > data/austria_processor/failed_kgs.json

# Install this peer's own token (docs/security-tokens.md). The director
# uses it for protected calls into this peer; the primary only grants it
# the peer scope. Without ADMIN_TOKEN the peer boots token-less and the
# director seeds one on "+ Add Peer" (first-write-wins bootstrap).
if [ -n "${ADMIN_TOKEN}" ]; then
    echo "${ADMIN_TOKEN}" > data/admin_token
    chmod 600 data/admin_token
    echo "  ✓ Peer token installed (provided via env)"
elif [ ! -f data/admin_token ]; then
    echo "  ℹ ADMIN_TOKEN not provided — the director will seed one when you add this peer."
fi
if [ -n "${MESH_TOKEN}" ]; then
    echo "${MESH_TOKEN}" > data/mesh_token
    chmod 600 data/mesh_token
    echo "  ✓ Mesh token installed"
fi

# Write peer URLs for the web API's peer sync thread
echo "${PEER_URL}" > data/austria_processor/peer_urls.txt
echo "  ✓ Peer URL written to data/austria_processor/peer_urls.txt"

# Point this peer's processor at the primary's Zenodo upload-lock broker.
# All peers serialise their Zenodo writes (single API token) via this URL.
echo "${PEER_URL}" > data/austria_processor/zenodo_lock_url.txt
echo "  ✓ Zenodo lock URL written to data/austria_processor/zenodo_lock_url.txt"

# ── 6. Install systemd services ──────────────────────────
echo "\n[6/7] Installing systemd services..."

# Generate processor service with peer coordination
cat > /tmp/austria_processor.service << EOSVC
[Unit]
Description=Austria Landscape Processor (${INSTANCE_ID})
After=network.target srv.service

[Service]
Type=simple
User=exedev
WorkingDirectory=${PROJECT_DIR}
ExecStart=/usr/bin/python3 austria_processor.py --mark-uncertain --peers ${PEER_URL} --instance-id ${INSTANCE_ID}
Restart=on-failure
RestartSec=60
MemoryMax=8G
MemoryHigh=7G
OOMScoreAdjust=100
OOMPolicy=stop

Environment=PYTHONUNBUFFERED=1
Environment=INSTANCE_ID=${INSTANCE_ID}

StandardOutput=append:${PROJECT_DIR}/data/austria_processor/logs/processor.log
StandardError=append:${PROJECT_DIR}/data/austria_processor/logs/processor.log

[Install]
WantedBy=multi-user.target
EOSVC

sudo cp srv.service /etc/systemd/system/srv.service
sudo cp /tmp/austria_processor.service /etc/systemd/system/austria_processor.service
sudo cp srv-watchdog.service /etc/systemd/system/srv-watchdog.service
sudo cp srv-watchdog.timer   /etc/systemd/system/srv-watchdog.timer
chmod +x srv_watchdog.sh
sudo systemctl daemon-reload
sudo systemctl enable --now srv-watchdog.timer
echo "  ✓ srv liveness watchdog installed"
# primary only: daily Austria-wide cells aggregate (cells_national.py, needs data/ne)
if [ -f "${PROJECT_DIR}/data/austria_processor/is_director" ]; then
  sudo cp cells-national.service cells-national.timer /etc/systemd/system/
  sudo systemctl daemon-reload
  sudo systemctl enable --now cells-national.timer
  echo "  ✓ cells-national.timer installed (primary)"
fi

# ── 7. Start services ────────────────────────────────────
echo "\n[7/7] Starting services..."
sudo systemctl enable --now srv
echo "  ✓ Web API started on port 8000"

# Test peer connectivity before starting processor
echo "\n  Testing peer connectivity..."
if curl -sf "${PEER_URL}/api/v1/processing/peers" > /dev/null 2>&1; then
    echo "  ✓ Peer reachable at ${PEER_URL}"
    PEER_DATA=$(curl -sf "${PEER_URL}/api/v1/processing/peers" 2>/dev/null)
    PEER_COMPLETED=$(echo "${PEER_DATA}" | python3 -c "import json,sys; print(len(json.load(sys.stdin).get('completed',[])))" 2>/dev/null || echo "?")
    echo "  ✓ Peer reports ${PEER_COMPLETED} completed KGs"
else
    echo "  ⚠ Peer unreachable — processor will run independently"
fi

# Throttle OFF by default — with many peers, every peer must upload its
# own GPKGs (the primary is on cooldown and can't pick up the slack).
rm -f data/austria_processor/upload_throttle
echo "  ✓ Throttle mode disabled (full + light GPKGs uploaded to Zenodo)"

# Install the processor service but do NOT enable/start it.
# The director on the primary instance will start it via the API when needed.
# This avoids the peer running independently and conflicting with the director.
sudo systemctl disable austria_processor 2>/dev/null || true
echo "  ✓ Processor service installed (will be started by director)"

# ── 8. Auto-register with director ─────────────────────────
echo "\n[8/8] Auto-registering with director..."

if [ -z "${SELF_URL}" ]; then
    echo "  ⚠ SELF_URL not set — cannot auto-register."
    echo "    Register manually via the director dashboard."
else
    # Derive peer ID from SELF_URL: https://example.invalid:8000 → at3
    PEER_ID=$(echo "${SELF_URL}" | sed 's|https://||;s|:.*||;s|\.exe\.xyz||;s|^srtm-lidar-||')

    # Check if this IS the director (don't self-register)
    DIRECTOR_HOST=$(echo "${PEER_URL}" | sed 's|https://||;s|:.*||;s|\.exe\.xyz||;s|^srtm-lidar-||')
    if [ "${PEER_ID}" = "${DIRECTOR_HOST}" ]; then
        echo "  ✓ This IS the director instance — skipping self-registration"
    else
        echo "  Registering as '${PEER_ID}' at ${SELF_URL} with director ${PEER_URL}..."
        # Wait for web server to be ready
        sleep 3
        TOK="${ADMIN_TOKEN}"
        if [ -z "${TOK}" ] && [ -f data/admin_token ]; then
            TOK=$(cat data/admin_token)
        fi
        REG_RESP=$(curl -sf -X POST \
          -H "Content-Type: application/json" \
          -H "X-Admin-Token: ${TOK}" \
          -d "{\"id\": \"${PEER_ID}\", \"url\": \"${SELF_URL}\"}" \
          "${PEER_URL}/api/v1/director/peers/add" 2>&1) && \
          echo "  ✓ Registered with director: ${REG_RESP}" || \
          echo "  ⚠ Auto-registration failed (director may be down or peer already exists): ${REG_RESP}"
    fi
fi

# Determine display URL
DISPLAY_URL="${SELF_URL:-https://$(hostname).exe.xyz:8000}"

echo ""
echo "══════════════════════════════════════════════════════"
echo "  Deployment complete!"
echo ""
echo "  Web UI:     ${DISPLAY_URL}/"
echo "  Dashboard:  ${DISPLAY_URL}/process.html"
echo "  Peer:       ${PEER_URL}"
echo "  Throttle:   ON (toggle via dashboard)"
if [ -n "${SELF_URL}" ]; then
    echo ""
    echo "  ❗ Make the VM public so the director can reach it:"
    echo "    ssh exe.dev share set-public $(echo "${SELF_URL}" | sed 's|https://||;s|:.*||;s|\.exe\.xyz||')"
fi
echo ""
echo "  Monitor:    journalctl -u srv -f"
echo "              tail -f data/austria_processor/logs/processor.log"
echo "══════════════════════════════════════════════════════"
