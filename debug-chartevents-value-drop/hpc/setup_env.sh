#!/bin/bash
# Build the virtualenv for chartevents_value_drop.py.
#
# Run this on the HPC LOGIN node. Compute nodes have no outbound
# network, so the environment has to exist before the job starts.
set -euo pipefail

PREFIX="${PREFIX:-${SCRATCH:?}/debug-chartevents-value-drop}"
VENV="${VENV:-$PREFIX/.venv}"
PYTHON_MODULE="${PYTHON_MODULE:-python/3.12.3}"
DUCKDB_VERSION="${DUCKDB_VERSION:-1.5.5}"

module purge
module load "$PYTHON_MODULE"

echo "Python: $(python3 -V) ($(command -v python3))"

mkdir -p "$PREFIX"

if [ ! -d "$VENV" ]; then
    python3 -m venv "$VENV"
fi

# shellcheck disable=SC1091
source "$VENV/bin/activate"

python -m pip install --quiet --upgrade pip
# duckdb is the only runtime dependency. Pinned to the version that wrote
# ${SCRATCH:?}/oracle/mimic4-full.db, so ATTACH does not hit a storage
# format mismatch.
python -m pip install --quiet "duckdb==$DUCKDB_VERSION"

python - <<'PY'
import duckdb
print(f"duckdb {duckdb.__version__} ready")
PY

echo "Environment ready: $VENV"
