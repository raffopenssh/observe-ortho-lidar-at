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
