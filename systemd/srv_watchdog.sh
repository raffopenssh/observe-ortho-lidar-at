#!/usr/bin/env bash
# srv / srv_public liveness watchdog (runs every 60 s via srv-watchdog.timer).
#
# Why: when all gunicorn worker threads wedge (outbound HTTPS stalls, or the
# cgroup MemoryHigh reclaim-throttle parking workers in D state) the socket
# still listens but every request hangs and the listen backlog silently
# fills.  systemd's Restart= can't see that (the process is alive).  A
# wall-clock probe + 3 strikes trips a forced restart.
#
# Ports come from config.py (SRTM_APP_PORT / SRTM_PUBLIC_PORT, .env honoured).
# SYSTEMCTL defaults to "systemctl --user"; set SYSTEMCTL="sudo systemctl"
# for a system-wide install.  srv_public is only probed when that unit is
# enabled.  Its probe hits /api/v1/health, which answers 503 once a worker's
# own memory watchdog (service_guard.py) has flagged it — a strike too.
set -u

MAX_FAILURES=3
PING_TIMEOUT=8      # seconds for the whole probe
MIN_RESTART_INTERVAL=600    # seconds — don't loop-restart
LOG_TAG=srv-watchdog
SYSTEMCTL="${SYSTEMCTL:-systemctl --user}"
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
eval "$(cd "$REPO_DIR" && python3 -c 'import config; print(f"APP_PORT={config.APP_PORT} PUBLIC_PORT={config.PUBLIC_PORT}")')"
APP_PORT=${APP_PORT:-8000}; PUBLIC_PORT=${PUBLIC_PORT:-8100}
mkdir -p "${HOME}/.cache"
now=$(date +%s)

probe() {   # unit url
    local unit=$1 url=$2
    local state="${HOME}/.cache/${unit}_watchdog.state"
    local failures=0 last_restart=0
    # shellcheck disable=SC1090
    [[ -f $state ]] && . "$state"
    local http_code
    http_code=$(curl -s -o /dev/null -w '%{http_code}' \
        --connect-timeout 2 --max-time "$PING_TIMEOUT" "$url" 2>/dev/null)
    http_code=${http_code:-000}
    if [[ $http_code == 200 ]]; then
        (( failures > 0 )) && logger -t "$LOG_TAG" "$unit recovered after $failures failures"
        failures=0
    else
        failures=$((failures + 1))
        logger -t "$LOG_TAG" "$unit probe failed (http=$http_code, strike $failures/$MAX_FAILURES)"
    fi
    if (( failures >= MAX_FAILURES )); then
        if (( now - last_restart < MIN_RESTART_INTERVAL )); then
            logger -t "$LOG_TAG" "$unit: would restart but last restart was $((now - last_restart))s ago — waiting"
        else
            logger -t "$LOG_TAG" "restarting $unit (gunicorn wedged; $($SYSTEMCTL show -p MemoryCurrent --value "$unit" 2>/dev/null) bytes)"
            $SYSTEMCTL restart "$unit" || logger -t "$LOG_TAG" "systemctl restart $unit failed"
            last_restart=$now
            failures=0
        fi
    fi
    printf 'failures=%s\nlast_restart=%s\n' "$failures" "$last_restart" >"$state"
}

probe srv.service "http://127.0.0.1:${APP_PORT}/api/v1/ping"
if $SYSTEMCTL is-enabled srv_public.service >/dev/null 2>&1; then
    probe srv_public.service "http://127.0.0.1:${PUBLIC_PORT}/api/v1/health"
fi
