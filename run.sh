#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV_DIR="$REPO_DIR/venv"

# ---------------------------------------------------------------------------
# 1. Create venv if it doesn't exist
# ---------------------------------------------------------------------------
if [ ! -f "$VENV_DIR/bin/activate" ]; then
    echo "Creating virtual environment..."
    python3 -m venv "$VENV_DIR"
fi

# ---------------------------------------------------------------------------
# 2. Activate venv
# ---------------------------------------------------------------------------
echo "Activating virtual environment..."
source "$VENV_DIR/bin/activate"

# ---------------------------------------------------------------------------
# 3. Install / upgrade requirements
# ---------------------------------------------------------------------------
echo "Installing requirements..."
pip install --quiet --upgrade pip
pip install --quiet -r "$REPO_DIR/requirements.txt"

cd "$REPO_DIR"

print_usage() {
    cat <<'EOF'
Usage: ./run.sh <mode> [args]

General:
  ./run.sh api
  ./run.sh all
  ./run.sh ensure-history
  ./run.sh current-season-update [days]
  ./run.sh status
  ./run.sh enrich
  ./run.sh export-season <year> [export_root]
  ./run.sh import-season <bundle_dir>

ETL:
  ./run.sh recent [days]
  ./run.sh season <year> [chunk_days]
  ./run.sh range <start_year> <end_year> [chunk_days]
  ./run.sh all-history [chunk_days]
  ./run.sh ensure-history [chunk_days]
  ./run.sh current-season-update [days]
  ./run.sh export-season <year> [export_root]
  ./run.sh import-season <bundle_dir>

Legacy aliases:
  ./run.sh etl        -> ./run.sh recent
  ./run.sh api        -> start API only
  ./run.sh all        -> ensure full history then start API
EOF
}

run_etl() {
    MPLCONFIGDIR=/tmp/mpl python etl/pipeline.py "$@"
}

MODE="${1:-all}"

case "$MODE" in
    recent)
        DAYS="${2:-7}"
        echo "Loading recent Statcast data (${DAYS} days)..."
        run_etl recent --days "$DAYS"
        ;;

    season)
        SEASON="${2:-}"
        CHUNK_DAYS="${3:-7}"
        if [ -z "$SEASON" ]; then
            echo "Season mode requires a year."
            print_usage
            exit 1
        fi
        echo "Backfilling season ${SEASON}..."
        run_etl season --season "$SEASON" --chunk-days "$CHUNK_DAYS"
        ;;

    range)
        START_SEASON="${2:-}"
        END_SEASON="${3:-}"
        CHUNK_DAYS="${4:-7}"
        if [ -z "$START_SEASON" ] || [ -z "$END_SEASON" ]; then
            echo "Range mode requires start and end seasons."
            print_usage
            exit 1
        fi
        echo "Backfilling seasons ${START_SEASON}-${END_SEASON}..."
        run_etl range --season-start "$START_SEASON" --season-end "$END_SEASON" --chunk-days "$CHUNK_DAYS"
        ;;

    all-history)
        CHUNK_DAYS="${2:-7}"
        echo "Backfilling full Statcast history..."
        run_etl all-history --chunk-days "$CHUNK_DAYS"
        ;;

    ensure-history)
        CHUNK_DAYS="${2:-7}"
        echo "Ensuring full Statcast history is loaded..."
        run_etl ensure-history --chunk-days "$CHUNK_DAYS"
        ;;

    current-season-update)
        DAYS="${2:-2}"
        echo "Updating current season with the last ${DAYS} day(s) of regular-season/postseason data..."
        run_etl current-season-update --days "$DAYS"
        ;;

    export-season)
        SEASON="${2:-}"
        EXPORT_ROOT="${3:-exports}"
        if [ -z "$SEASON" ]; then
            echo "Export-season mode requires a year."
            print_usage
            exit 1
        fi
        echo "Exporting season ${SEASON} to ${EXPORT_ROOT}..."
        run_etl export-season --season "$SEASON" --export-root "$EXPORT_ROOT"
        ;;

    import-season)
        IMPORT_DIR="${2:-}"
        if [ -z "$IMPORT_DIR" ]; then
            echo "Import-season mode requires a bundle directory."
            print_usage
            exit 1
        fi
        echo "Importing season bundle from ${IMPORT_DIR}..."
        run_etl import-season --import-dir "$IMPORT_DIR"
        ;;

    enrich)
        echo "Enriching player metadata..."
        run_etl enrich
        ;;

    status)
        echo "Inspecting loaded database status..."
        run_etl status
        ;;

    etl)
        echo "Running ETL pipeline (recent mode)..."
        run_etl recent
        ;;

    api)
        echo "Starting API server on http://localhost:8000 ..."
        python -m uvicorn api.main:app --host 0.0.0.0 --port 8000
        ;;

    all)
        CHUNK_DAYS="${2:-7}"
        echo "Ensuring full Statcast history is loaded..."
        run_etl ensure-history --chunk-days "$CHUNK_DAYS"
        echo ""
        echo "Starting API server on http://localhost:8000 ..."
        python -m uvicorn api.main:app --host 0.0.0.0 --port 8000
        ;;

    help|-h|--help)
        print_usage
        ;;

    *)
        echo "Unknown mode: $MODE"
        echo ""
        print_usage
        exit 1
        ;;
esac
