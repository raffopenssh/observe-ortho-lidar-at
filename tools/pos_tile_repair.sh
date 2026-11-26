#!/usr/bin/env bash
# Partly shifted KGs: decide the tiles no tree window could measure (terrain rule,
# ne_pos_fix --verify-tiles --terrain), rebuild the ledger, then put every product that
# holds a 5 m-shifted tile on the reprocess list (fresh v2.4 in place, keep_products:
# the old product and its read-time fix stay served until the new one is ingested).
# One-off, batch tier:  systemd-run --user --slice=batch.slice --unit=pos-tile-repair tools/pos_tile_repair.sh
# docs/qc-lessons.md -> "Terrain rule + auto repair (2026-11-26)"
set -u
cd "$(dirname "$0")/.."
LOG=data/pos_tile_repair.log
MAX_MB=${MAX_MB:-10000}; MB_H=${MB_H:-700}
say() { echo "$(date -u +%FT%TZ) $*" >> "$LOG"; }
say "start: terrain check, cap ${MAX_MB} MB at ${MB_H} MB/h"
python3 ne_pos_fix.py --verify-tiles --terrain --budget-mb-h "$MB_H" --max-mb "$MAX_MB" >> "$LOG" 2>&1 || say "verify-tiles rc=$?"
say "ledger build"
python3 qc_ledger.py --build --force >> "$LOG" 2>&1 || say "ledger rc=$?"
if [ "${ENROLL:-1}" = 1 ]; then
  say "enrol products with a moved tile"
  python3 qc_generation.py --enroll-fault pos_shift --verdict moved \
    --why 'pos_shift: product holds 5 m-shifted tiles of a partly shifted KG (read-time move until replaced); fresh v2.4 in place (conv cAEZDGH)' >> "$LOG" 2>&1
  python3 qc_generation.py --sweep >> "$LOG" 2>&1 || true      # wakes the director out of the run-complete hold
fi
say "done: $(python3 ne_pos_fix.py --status 2>/dev/null | tail -1 | cut -c1-400)"
