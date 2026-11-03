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
