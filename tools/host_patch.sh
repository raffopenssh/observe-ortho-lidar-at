#!/bin/bash
# Host patch: apt security/stable upgrades + requirements.txt `==` pins.
# Launched by app._host_patch_startup_thread as a transient systemd unit
# (`srtm-hostpatch`, root) so it survives the srv restart it ends with.
# Never run on the director / single-VM host (the operator patches it by hand).
# State: <repo>/data/host_patch_state.json (read by /api/v1/admin/pkgstat).
# Usage: host_patch.sh <repo> <service-user> [<pip-pin> ...]
set -u
REPO="$1"; SVC_USER="$2"; shift 2
STATE="$REPO/data/host_patch_state.json"
LOG=/tmp/host_patch.log
exec >>"$LOG" 2>&1
echo "=== host_patch $(date -u +%FT%TZ) user: $SVC_USER pins: $*"
export DEBIAN_FRONTEND=noninteractive
# needrestart would otherwise auto-restart austria_processor.service mid-KG
export NEEDRESTART_SUSPEND=1 NEEDRESTART_MODE=l
APT_OPTS=(-y -q -o DPkg::Lock::Timeout=600
          -o Dpkg::Options::=--force-confdef -o Dpkg::Options::=--force-confold)
apt_rc=0; pip_rc=0
nice -n 10 apt-get -q -o DPkg::Lock::Timeout=600 update || apt_rc=$?
if [ "$apt_rc" = 0 ]; then
  nice -n 10 apt-get "${APT_OPTS[@]}" upgrade || apt_rc=$?
fi
dpkg --configure -a || true
apt-get clean || true
if [ "$#" -gt 0 ]; then
  # Same interpreter the service runs with (.venv when present, else the
  # service user's python3 + --user), as the service user, never root.
  PY="$REPO/.venv/bin/python3"
  if [ -x "$PY" ]; then
    sudo -u "$SVC_USER" -H nice -n 10 "$PY" -m pip install -q "$@" || pip_rc=$?
  else
    sudo -u "$SVC_USER" -H nice -n 10 python3 -m pip install --user -q \
        --break-system-packages "$@" || pip_rc=$?
  fi
fi
sec_left=$(apt list --upgradable 2>/dev/null | grep -c -- '-security' || true)
printf '{"at": "%s", "apt_rc": %s, "pip_rc": %s, "security_left": %s, "pins": "%s"}\n' \
  "$(date -u +%FT%TZ)" "$apt_rc" "$pip_rc" "${sec_left:-0}" "$*" > "$STATE.tmp"
chown "$SVC_USER:" "$STATE.tmp" && mv "$STATE.tmp" "$STATE"
echo "apt_rc=$apt_rc pip_rc=$pip_rc security_left=$sec_left -> restart srv"
# Load the new libssl/libc/python + pip packages. The processor lives in its
# own unit and is untouched; it picks the new libs up at its next KG start.
# srv is either a system unit or the service user's user unit (systemd/).
systemctl restart srv 2>/dev/null \
  || systemctl --machine="$SVC_USER@.host" --user restart srv
