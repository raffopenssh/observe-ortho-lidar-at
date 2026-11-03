# Host maintenance — patch audit

Read-only visibility of the OS / pip patch level of every machine running
`app.py`.  Nothing here runs `apt` or `pip`; it only reports.

## Host patch audit

`GET /api/v1/admin/pkgstat` (operator gate, every VM, 10 min cache,
`?fresh=1` bypasses) reports dpkg versions of a security watch-list
(openssl, openssh, sudo, libc6, python3.12, nginx, curl, git, …), pending
`-security` upgrades, `reboot_required` and the `==` pins of
`requirements.txt` vs what is importable (`pip_drift`).  Read-only — it
never runs apt.  Fleet view from the director (single VM: just the local
instance):

    python3 tools/fleet_pkgstat.py [--fresh] [--json]

lists only peers (`SRTM_PEERS`) that deviate from the director's package
versions, have pending security updates or pip drift.  Peers are reached
with the `X-Operator-Token` header (`SRTM_OPERATOR_TOKEN`, same value on
every VM); the local instance over loopback needs none.  On hosted VMs
the kernel is usually managed by the provider (no `linux-image-*`
package); `reboot_required` from libc6/apparmor is satisfied by restarting
`srv`/`srv_public`.

### Auto-apply on rollout (peers only)

Every **peer** srv start (= every rollout wave restart, so paced by the
director) runs `app._host_patch_startup_thread`: after 2 min + 0–10 min
jitter, if pkgstat shows pending security updates, `requirements.txt` pin
drift or apt lists > 72 h old, it launches `tools/host_patch.sh` as
transient root unit `srtm-hostpatch` (`apt-get update && apt-get upgrade`
non-interactive, confold, `NEEDRESTART_SUSPEND=1` so
`austria_processor.service` is never restarted mid-KG; `pip install` of
the drifted `==` pins as the service user into `.venv` — or `--user`
without one; `apt-get clean`; then `systemctl restart srv`).  Needs
passwordless `sudo systemd-run` for the service user, like the processor
launcher.  The processor keeps running and picks up the new libs at its
next KG.  Guards: never on the director / in single-VM mode
(`config.single_vm()`), one launch per 6 h (`data/host_patch_launch`),
flock across gunicorn workers, ≥ 2 GB free disk, off switch
`data/host_patch_off`.  Result in `data/host_patch_state.json` →
`host_patch` field of `/admin/pkgstat`; log `/tmp/host_patch.log` on the
peer; `?q=host_patch` in `/process.txt`.

## Host reboot (peers only, graceful)

`app._host_reboot_thread` (every srv start, first check after ~15 min,
then every 10 min; 1 min while draining) reboots a **peer** when
`/var/run/reboot-required` is set (kernel / libc from `host_patch.sh`),
never a hard stop:

1. hold the processor-start gate (`data/host_reboot_pending` →
   `POST /api/v1/processing/start` answers 503 `retry` on the peer, so the
   director just tries again next tick) and SIGTERM the processor python
   parent once = "exit after the current KG" (a frontier KG may take
   hours — it waits);
2. once no processor / `srtm-hostpatch` / apt / dpkg is running, ask the
   director for a fleet-wide reboot slot: `POST /api/v1/director/reboot_slot`
   (`X-Operator-Token`, body `peer_id` = `config.self_id()`), knobs
   `host_reboot_max_parallel` (default 2) and `host_reboot_lease_s`
   (default 1200) in `director_config.json`, leases in
   `data/austria_processor/host_reboot_slots.json`, `GET` = current leases;
   `data/host_reboot_off` on the director refuses every slot;
3. `sudo -n systemctl reboot`; srv is enabled → comes back, the director
   restarts the processor on its next tick (resume from checkpoints).

Guards: never on the director / in single-VM mode (`config.single_vm()`),
≤ 1 reboot per 24 h per peer (`data/host_reboot_last`), uptime ≥ 30 min, no
slot within 3 h → gate released and retried after 24 h, off switch
`data/host_reboot_off` (also clears the gate); a stale pending file
(> 30 min, thread died) no longer gates starts.  State shows up in
`/admin/pkgstat` (`host_reboot_pending`, `host_reboot_last`, `uptime_s`).
