#!/bin/bash
# One-shot reporter for the running grid25-registration.service (2026-11-21 run).
# Opens a FRESH Shelley conversation per milestone — never wakes an old one
# (every resume of a long conversation re-sends its whole context).
#   A: hillshade steps done (ne_pos_fix starts, or the unit ends)
#   B: unit ended after ne_pos_fix ran
# Start: sudo systemd-run --collect --unit shelley-grid25-report --uid exedev \
#          --setenv HOME=/home/exedev tools/grid25_report_wake.sh <originating-conv-id>
# DRY=1 prints the prompts instead of sending.
U=grid25-registration.service; ORIGIN=${1:-?}; cd /home/exedev/srtm-lidar || exit 1
SINCE=$(systemctl show -p InactiveExitTimestamp --value $U | sed 's/^[A-Za-z]* //; s/ UTC$//')
active() { [ "$(systemctl is-active $U)" = activating ]; }
send() { if [ -n "$DRY" ]; then printf '%s\n----\n' "$1"; else
  shelley client chat -model claude-fable-5.2 -reasoning medium -tag wake -cwd /home/exedev/srtm-lidar -p "$1"; fi; }
COMMON="Rules: fresh, self-contained wake-up — do NOT continue or read conversation cC2RWYL (cost); no sleep/poll loops; no fleet actions. Scheduled from conversation $ORIGIN. Background only if needed: docs/hillshade-seams.md (sections 'National mask', 'Stale tiles', 'Trees / structures of shifted KGs').
Step log (summaries + errors only): journalctl -u $U --since '$SINCE' --no-pager -o cat | grep -E '^\\{|Traceback|Error|rror:|exited|Failed|Finished|Deactivated|Consumed' | cut -c1-400 | tail -40"

if [ -z "$DRY" ]; then while active && ! pgrep -f 'ne_pos_fix.py --verify' >/dev/null; do sleep 60; done; fi
STILL=$(active && echo "The unit is STILL RUNNING its last step (ne_pos_fix --verify, >1 day, 120 MB/h): do not restart or stop it; a second wake-up reports it." || echo "The unit has ended: $(systemctl show -p Result --value $U) — ne_pos_fix did not run or is already done; check the step log.")
send "srtm-lidar-at wake-up A: hillshade repair run finished — verify and report (no code changes unless a step failed for a trivial, safe-to-fix reason).
$STILL
Job = $U, steps: outline fetch -> grid25 registration -> grid25_patch --edge-scan (cliff-like internal tile edges, all products) -> grid25_patch --scan (verify suspects against BEV DTM, rebuild bad tiles, one tile-set version bump if anything changed) -> z15 country seam check on the new tile set.
Baseline before the run: version 5.1795274706.1-0818165f; 7 tiles patched in 1 product (92116 Nofels); seam list = stale v4 run, 107 seams at 47 % coverage.
Collect (4 commands):
1. the step log below — which steps failed / were skipped
2. python3 grid25_patch.py --status
3. curl -s localhost:8100/tiles/hillshade.json -> version, tile_edge_check, seam_check (complete? tileset_version == version? seam_count)
4. curl -s localhost:8100/tiles/hillshade/seams.json -> count; group by product/region, top 5 by step
Spot-check: /tiles/hillshade/12/2156/1436.png still answers 200 with X-Tile headers of the new version.
Judge: version bumped? tiles/products rebuilt, MB used, edges_open (should be 0), seams left on the new tile set and whether they cluster on a few products (-> candidates for 'grid25_patch.py --tile'; list them, do not run a mass rebuild).
Reply with: (1) owner report, <= 15 lines, numbers first, open problems last; (2) a paste-ready note for umfeld-at, <= 8 lines: final tile-set version, seam list final or not + count, anything they must change (expected: nothing beyond re-fetching on version change).
$COMMON"

[ -n "$DRY" ] || { active || exit 0; while active; do sleep 120; done; }
send "srtm-lidar-at wake-up B: tree/structure position verify finished — report only.
$U ended: $(systemctl show -p Result --value $U). Last step 'ne_pos_fix.py --verify' measured ~483 candidate parent KGs against BEV nDSM and switched on the read-time (-5,+5) m correction of tree apices / structures per confirmed KG (verdicts: shift = corrected, zero / mixed / unclear = left alone).
Collect: python3 ne_pos_fix.py --status; the step log below; curl -s localhost:8100/api/v1/health.
Pre-run test: 10 parents -> 7 shift, 3 zero controls untouched, 1 mixed left alone. Expect mostly shift among the 416 shifted products' parents; mixed (166 products) mostly left alone.
Reply with: owner report <= 10 lines — verdict histogram, MB used, failures, KGs left uncorrected (mixed/unclear count; these need the reprocess noted under 'Open: products that need a real regeneration') — and one line for umfeld-at only if tree positions changed (trees moved 7 m NW in N KGs; etags changed, re-fetch).
$COMMON"
