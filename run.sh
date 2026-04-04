#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV_DIR="$REPO_DIR/venv"

# ---------------------------------------------------------------------------
# Bootstrap: create venv, install deps
# ---------------------------------------------------------------------------
if [ ! -f "$VENV_DIR/bin/activate" ]; then
    echo "Creating virtual environment..."
    python3 -m venv "$VENV_DIR"
fi
source "$VENV_DIR/bin/activate"
pip install --quiet --upgrade pip
pip install --quiet -r "$REPO_DIR/requirements.txt"
cd "$REPO_DIR"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
run_etl() {
    MPLCONFIGDIR=/tmp/mpl python etl/pipeline.py "$@"
}

print_usage() {
    cat <<'EOF'
Usage: ./run.sh <command> [args]

  setup              First-time load. Imports from Parquet if bundles exist,
                     pulls from Statcast API otherwise. Safe to re-run after gaps.

  restore            Rebuild the database from existing Parquet bundles.
                     No API calls. Use this on a new machine after git lfs pull.

  update             Pull games played since the last load for the current season.
                     Refreshes derived tables and overwrites the current season bundle.

  api                Start the API server on http://localhost:8000.

  status             Show loaded seasons, completeness, row counts, player coverage.

  export [year]      Export Parquet bundles. Omit year to export all loaded seasons.
                     Example: ./run.sh export 2025

  rebuild <year>     Force re-pull one season from the Statcast API, replacing
                     whatever is in the database and Parquet bundle.
                     Example: ./run.sh rebuild 2023

  report <year>      Season validation report: game counts, postseason breakdown,
                     derived-table row counts.
                     Example: ./run.sh report 2025

  enrich             Resolve missing player names, handedness, and position.
                     Safe to run multiple times.
EOF
}

# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------
MODE="${1:-}"

case "$MODE" in

    setup)
        echo "Loading full Statcast history (Parquet-first, API fallback)..."
        run_etl ensure-history --export-root exports
        ;;

    restore)
        echo "Rebuilding database from Parquet bundles (no API calls)..."
        run_etl import-all --export-root exports
        ;;

    update)
        echo "Pulling new games for the current season..."
        run_etl auto-update --export-root exports
        ;;

    api)
        echo "Starting API server on http://localhost:8000 ..."
        python -m uvicorn api.main:app --host 0.0.0.0 --port 8000
        ;;

    status)
        run_etl status
        ;;

    export)
        SEASON="${2:-}"
        if [ -n "$SEASON" ]; then
            echo "Exporting season ${SEASON} to exports/..."
            run_etl export-season --season "$SEASON" --export-root exports
        else
            echo "Exporting all loaded seasons to exports/..."
            run_etl export-all --export-root exports
        fi
        ;;

    rebuild)
        SEASON="${2:-}"
        if [ -z "$SEASON" ]; then
            echo "Error: rebuild requires a year.  Example: ./run.sh rebuild 2023"
            exit 1
        fi
        echo "Force re-pulling season ${SEASON} from Statcast API..."
        run_etl rebuild-season --season "$SEASON"
        ;;

    report)
        SEASON="${2:-}"
        if [ -z "$SEASON" ]; then
            echo "Error: report requires a year.  Example: ./run.sh report 2025"
            exit 1
        fi
        run_etl season-report --season "$SEASON"
        ;;

    enrich)
        echo "Resolving missing player names and metadata..."
        run_etl enrich
        ;;

    help|-h|--help)
        print_usage
        ;;

    "")
        echo "No command given."
        echo ""
        print_usage
        exit 1
        ;;

    *)
        echo "Unknown command: $MODE"
        echo ""
        print_usage
        exit 1
        ;;
esac
