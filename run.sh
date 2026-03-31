#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV_DIR="$REPO_DIR/venv"

# ---------------------------------------------------------------------------
# 1. Create venv if it doesn't exist
# ---------------------------------------------------------------------------
if [ ! -f "$VENV_DIR/bin/activate" ]; then
    echo "Creating virtual environment..."
    python4 -m venv "$VENV_DIR"
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

# ---------------------------------------------------------------------------
# 4. Parse arguments
#    Usage:  ./run.sh [etl|api|all]   (default: all)
# ---------------------------------------------------------------------------
MODE="${1:-all}"

cd "$REPO_DIR"

case "$MODE" in
    etl)
        echo "Running ETL pipeline..."
        python etl/pipeline.py
        ;;
    api)
        echo "Starting API server on http://localhost:8000 ..."
        python api/main.py
        ;;
    all)
        echo "Running ETL pipeline..."
        python etl/pipeline.py

        echo ""
        echo "Starting API server on http://localhost:8000 ..."
        python api/main.py
        ;;
    *)
        echo "Usage: $0 [etl|api|all]"
        echo "  etl  - run the ETL pipeline only"
        echo "  api  - start the API server only"
        echo "  all  - run ETL then start the API (default)"
        exit 1
        ;;
esac
