#!/bin/bash
# Build the virtualenv for patient_birthdate_anchor.py.
#
# Run this on the HPC LOGIN node. Compute nodes have no outbound
# network, so the environment has to exist before the job starts.
set -euo pipefail

PREFIX="${PREFIX:-${SCRATCH:?}/debug-patient-birthdate-anchor}"
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
# duckdb does the work; pandas is only used to write the CSVs. duckdb is
# pinned to the version that wrote ${SCRATCH:?}/oracle/mimic4-full.db, so
# the ATTACH for the cross-check does not hit a storage format mismatch.
python -m pip install --quiet "duckdb==$DUCKDB_VERSION" pandas

python - <<'PY'
import duckdb, pandas
print(f"duckdb {duckdb.__version__}, pandas {pandas.__version__} ready")
PY

echo "Environment ready: $VENV"
