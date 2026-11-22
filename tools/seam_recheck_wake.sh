#!/bin/bash
# One-shot: z15 country seam check, then a FRESH Shelley conversation reports.
# sudo systemd-run --collect --unit shelley-seam-recheck --uid exedev --setenv HOME=/home/exedev \
#   --property Nice=15 --property MemoryMax=2500M tools/seam_recheck_wake.sh <originating-conv-id>
cd /home/exedev/srtm-lidar || exit 1
python3 tools/hillshade_seam_check.py --country --z 15 > /tmp/seam_recheck.log 2>&1; RC=$?
shelley client chat -model claude-fable-5.2 -reasoning medium -tag wake -cwd /home/exedev/srtm-lidar -p "srtm-lidar-at wake-up: z15 country hillshade seam re-check finished (exit $RC, log /tmp/seam_recheck.log, tail only) — report only, no code changes, no fleet actions, no sleep loops. Scheduled from conversation ${1:-?} (do not read it; cost).
Context: grid25_patch.py gained flood fill + '--scan --seams' on 2026-11-22 (docs/hillshade-seams.md -> 'Stale tiles / grid25 patches', read only that section). Before: tile-set 5.1795274706.2-0818165f, 191 tiles / 44 products patched, 61 seams in 45 tiles (copy: /tmp/seam/report_before.json, may be gone). After the repair: patch generation 4, 274 tiles / 49 products.
Collect: (1) python3 grid25_patch.py --status  (2) curl -s localhost:8100/tiles/hillshade.json -> version, seam_check (complete? tileset_version == version? seam_count)  (3) curl -s localhost:8100/tiles/hillshade/seams.json -> count, top 10 by step with product codes; which of them touch a patched product.
Judge: seams before vs after; are Paznaun (84005/84006/84010/84012/84114) and 85106 gone; are the rest < 2 m rim steps or real terrain. Do NOT start another scan.
Reply: owner report <= 12 lines, numbers first; then, only if umfeld-at must change something, a paste-ready note <= 6 lines (expected: none beyond re-fetching tiles + seams.json on version change)."
