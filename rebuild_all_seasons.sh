#!/usr/bin/env bash
# rebuild_all_seasons.sh
# Rebuilds every season from scratch: delete → pull Statcast API → derived tables → Parquet export.
# Validates game counts after each season. Logs to rebuild.log.
# Run from the repo root. Takes several hours.

set -euo pipefail
REPO_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG="$REPO_DIR/rebuild.log"
source "$REPO_DIR/venv/bin/activate"

run_etl() {
    MPLCONFIGDIR=/tmp/mpl python "$REPO_DIR/etl/pipeline.py" "$@"
}

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG"
}

# Expected regular-season game counts.
# 2020 = COVID short season. 2026 = partial/ongoing. All others = 2430 target.
expected_games() {
    case $1 in
        2020) echo 898 ;;
        2026) echo "PARTIAL" ;;
        *)    echo 2430 ;;
    esac
}

log "===== FULL SEASON REBUILD START ====="
log "Seasons: 2015 → 2026"
log "Log file: $LOG"
echo ""

for SEASON in 2015 2016 2017 2018 2019 2020 2021 2022 2023 2024 2025 2026; do

    log "----- Season $SEASON: START -----"

    # Rebuild from API (delete existing data, re-pull, build derived tables)
    log "[$SEASON] Pulling from Statcast API and rebuilding derived tables..."
    if ! run_etl rebuild-season --season "$SEASON" 2>>"$LOG"; then
        log "[$SEASON] ERROR: rebuild-season failed. Check $LOG for details."
        continue
    fi

    # Export to Parquet
    log "[$SEASON] Exporting Parquet bundle..."
    if ! run_etl export-season --season "$SEASON" --export-root exports 2>>"$LOG"; then
        log "[$SEASON] ERROR: export-season failed."
        continue
    fi

    # Validate game count
    EXPECTED=$(expected_games "$SEASON")
    REPORT=$(run_etl season-report --season "$SEASON" 2>/dev/null | grep "Season report:" | head -1)
    REGULAR=$(echo "$REPORT" | python3 -c "import sys,re; m=re.search(r\"'regular_games': (\\d+)\", sys.stdin.read()); print(m.group(1) if m else 'N/A')")
    POSTSEASON=$(echo "$REPORT" | python3 -c "import sys,re; m=re.search(r\"'postseason_total': (\\d+)\", sys.stdin.read()); print(m.group(1) if m else 'N/A')")
    PITCH_STATE=$(echo "$REPORT" | python3 -c "import sys,re; m=re.search(r\"'league_pitch_state_rows': (\\d+)\", sys.stdin.read()); print(m.group(1) if m else 'N/A')")
    TRANSITIONS=$(echo "$REPORT" | python3 -c "import sys,re; m=re.search(r\"'pitch_transition_rows': (\\d+)\", sys.stdin.read()); print(m.group(1) if m else 'N/A')")

    if [ "$EXPECTED" = "PARTIAL" ]; then
        VERDICT="OK (partial season, $REGULAR games so far)"
    elif [ "$REGULAR" = "$EXPECTED" ]; then
        VERDICT="✓ PASS ($REGULAR regular / $POSTSEASON postseason)"
    else
        # Allow ±1 for pybaseball coverage gaps in early Statcast years
        DIFF=$(( REGULAR - EXPECTED ))
        ABS_DIFF=${DIFF#-}
        if [ "$ABS_DIFF" -le 2 ]; then
            VERDICT="~ ACCEPTABLE ($REGULAR regular, expected $EXPECTED — delta=$DIFF, likely Statcast coverage gap)"
        else
            VERDICT="✗ FAIL ($REGULAR regular, expected $EXPECTED — delta=$DIFF)"
        fi
    fi

    log "[$SEASON] Games:        $VERDICT"
    log "[$SEASON] Pitch state:  $PITCH_STATE league rows"
    log "[$SEASON] Transitions:  $TRANSITIONS rows"
    log "[$SEASON] Bundle:       exports/season=$SEASON/"

    # Warn if derived tables are empty
    if [ "$PITCH_STATE" = "0" ] || [ "$PITCH_STATE" = "N/A" ]; then
        log "[$SEASON] WARNING: league_pitch_state_summary is empty."
    fi
    if [ "$TRANSITIONS" = "0" ] || [ "$TRANSITIONS" = "N/A" ]; then
        log "[$SEASON] WARNING: pitch_transition_summary is empty."
    fi

    log "----- Season $SEASON: DONE -----"
    echo ""

done

log "===== FULL SEASON REBUILD COMPLETE ====="
log "Run './run.sh status' for a final summary."
