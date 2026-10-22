#!/usr/bin/env bash
# srv / srv_public liveness watchdog (runs every 60 s via srv-watchdog.timer).
#
# Why: when all gunicorn worker threads wedge (outbound HTTPS stalls, or —
# Oct 2026 — the cgroup MemoryHigh reclaim-throttle parking workers in D
# state) the socket still listens but every request hangs and the listen
# backlog silently fills.  systemd's Restart= can't see that (the process is
# alive).  A wall-clock probe + 3 strikes trips a forced restart.
#
# srv_public is only probed when the unit exists (primary host).  Its probe
# hits /api/v1/health, which answers 503 once a worker's own memory watchdog
# (service_guard.py) has flagged it — that is treated as a strike too.
set -u

MAX_FAILURES=3
PING_TIMEOUT=8      # seconds for the whole probe
MIN_RESTART_INTERVAL=600    # seconds — don't loop-restart
LOG_TAG=srv-watchdog
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
            logger -t "$LOG_TAG" "restarting $unit (gunicorn wedged; $(systemctl show -p MemoryCurrent --value "$unit" 2>/dev/null) bytes)"
            sudo /bin/systemctl restart "$unit" || logger -t "$LOG_TAG" "systemctl restart $unit failed"
            last_restart=$now
            failures=0
        fi
    fi
    printf 'failures=%s\nlast_restart=%s\n' "$failures" "$last_restart" >"$state"
}

probe srv.service http://127.0.0.1:8000/api/v1/ping
if systemctl list-unit-files srv_public.service >/dev/null 2>&1 \
   && systemctl is-enabled srv_public.service >/dev/null 2>&1; then
    probe srv_public.service http://127.0.0.1:8100/api/v1/health
fi
