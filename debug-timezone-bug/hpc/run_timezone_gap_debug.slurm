#!/bin/bash
#SBATCH --job-name=tz-gap-debug
#SBATCH --partition=defq
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=64G
#SBATCH --time=01:00:00
#SBATCH --output=logs/%x-%j.out
#SBATCH --error=logs/%x-%j.err
#
# Full-cohort MIMIC to MIMIC-on-FHIR DST-gap comparison.
#
#   sbatch run_timezone_gap_debug.slurm
#
# setup_env.sh must have been run on the login node first.
set -euo pipefail

PREFIX="${PREFIX:-${SCRATCH:?}/debug-timezone-bug}"
VENV="${VENV:-$PREFIX/.venv}"
PYTHON_MODULE="${PYTHON_MODULE:-python/3.12.3}"

MIMIC_ROOT="${MIMIC_ROOT:-${SCRATCH:?}/original-mimic-iv/mimic-iv-2.2}"
MIMIC_FHIR_WAREHOUSE="${MIMIC_FHIR_WAREHOUSE:-${SCRATCH:?}/mimic-on-fhir-delta/spark_warehouse}"
OUT_DIR="${OUT_DIR:-$PREFIX/results/job-${SLURM_JOB_ID:-manual}}"

# Deliberately no `module purge`: on the HPC cluster it also drops
# /cm/shared/apps/slurm/current/bin from PATH, which removes srun/sacct.
module load "$PYTHON_MODULE"
# shellcheck disable=SC1091
source "$VENV/bin/activate"

# Keep DuckDB inside the cgroup: one thread per allocated CPU, and a memory
# limit comfortably under --mem so the OOM killer is never the thing that
# stops the job. Spill goes to node-local scratch, not to Lustre.
THREADS="${SLURM_CPUS_PER_TASK:-8}"
MEMORY_LIMIT="${DUCKDB_MEMORY_LIMIT:-48GB}"
TEMP_DIR="${TMPDIR:-${SCRATCH:?}/tmp}/duckdb-${SLURM_JOB_ID:-manual}"

mkdir -p "$OUT_DIR" "$TEMP_DIR"

echo "host:        $(hostname)"
echo "job:         ${SLURM_JOB_ID:-manual}"
echo "cpus:        $THREADS"
echo "mem limit:   $MEMORY_LIMIT"
echo "mimic root:  $MIMIC_ROOT"
echo "warehouse:   $MIMIC_FHIR_WAREHOUSE"
echo "out dir:     $OUT_DIR"
echo "temp dir:    $TEMP_DIR"
echo

cleanup() { rm -rf "$TEMP_DIR"; }
trap cleanup EXIT

# Single task on a single node, so this runs directly rather than under srun.
python "$PREFIX/timezone_gap_debug.py" \
    --mimic-root "$MIMIC_ROOT" \
    --fhir-warehouse "$MIMIC_FHIR_WAREHOUSE" \
    --out-dir "$OUT_DIR" \
    --threads "$THREADS" \
    --memory-limit "$MEMORY_LIMIT" \
    --temp-dir "$TEMP_DIR"

echo
echo "Done. Results in $OUT_DIR"
ls -la "$OUT_DIR"
