#!/usr/bin/env bash
# Fleet-wide pause / resume of KG processing. Run on the primary.
#
#   scripts/fleet_pause.sh pause            # director→paused, hard-stop every running peer
#   scripts/fleet_pause.sh pause --graceful # director→paused, peers finish current KG then exit
#   scripts/fleet_pause.sh resume           # director→auto (it restarts peers itself, wave-paced)
#   scripts/fleet_pause.sh status           # mode + per-state peer counts
#
# Why two steps: director mode=paused only stops *dispatching* new work;
# running processors keep going until told otherwise. The per-peer stop
# MUST go through /api/v1/director/peers/<id>/cmd/stop — the
# `/api/v1/processing/stop?peer_id=` form silently ignores peer_id and
# stops the primary's (non-existent) processor instead.
set -euo pipefail
cd "$(dirname "$0")/.."
TOKEN=$(cat data/admin_token)
BASE=http://localhost:8000
H=(-H "X-Admin-Token: $TOKEN")
act=${1:-status}; shift || true

running_peers() {
  curl -s -m 20 "$BASE/api/v1/director/status?slim=1" | python3 -c '
import json,sys
st=json.load(sys.stdin)
print(" ".join(p["id"] for p in st["peers"]
      if p.get("processor_state") not in ("stopped","idle","complete",None)
      and p["id"]!="primary"))'
}

case "$act" in
  pause)
    q=""; [[ "${1:-}" == "--graceful" ]] && q="?graceful=1"
    curl -s -X POST "${H[@]}" "$BASE/api/v1/director/mode?mode=paused"; echo
    for p in $(running_peers); do
      printf "%s: " "$p"
      curl -s -m 40 -X POST "${H[@]}" "$BASE/api/v1/director/peers/$p/cmd/stop$q" | tr -d '\n'; echo
    done
    echo "re-run '$0 status' in ~1 min; peer states are cached ~30 s" ;;
  resume)
    curl -s -X POST "${H[@]}" "$BASE/api/v1/director/mode?mode=auto"; echo
    echo "director will restart peers itself over the next few ticks" ;;
  status)
    curl -s -m 20 "$BASE/api/v1/director/status?slim=1" | python3 -c '
import json,sys,collections
st=json.load(sys.stdin)
print("mode:", st.get("mode"))
print(dict(collections.Counter(p.get("processor_state") for p in st["peers"])))' ;;
  *) echo "usage: $0 pause [--graceful] | resume | status"; exit 1 ;;
esac
