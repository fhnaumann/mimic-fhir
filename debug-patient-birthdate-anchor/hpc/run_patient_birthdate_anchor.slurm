#!/bin/bash
#SBATCH --job-name=birthdate-anchor
#SBATCH --partition=defq
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=128G
#SBATCH --time=02:00:00
#SBATCH --output=logs/%x-%j.out
#SBATCH --error=logs/%x-%j.err
#
# Full-cohort verification that the MIMIC-on-FHIR Patient ETL derives
# `Patient.birthDate` from the earliest transfer rather than the anchor pair,
# making every age computed from the published resource wrong wherever
# year(MIN(transfers.intime)) != anchor_year.
#
#   sbatch run_patient_birthdate_anchor.slurm
#
# setup_env.sh must have been run on the login node first.
#
# The published side is reproduced from sql/fhir_patient.sql itself rather than
# read out of a warehouse, so the result is reproducible by anyone holding the
# PhysioNet files and nothing else. The oracle is opened READ_ONLY and used
# only to assert the canonical age formula was transcribed correctly.
set -euo pipefail

PREFIX="${PREFIX:-${SCRATCH:?}/debug-patient-birthdate-anchor}"
VENV="${VENV:-$PREFIX/.venv}"
PYTHON_MODULE="${PYTHON_MODULE:-python/3.12.3}"

MIMIC_ROOT="${MIMIC_ROOT:-${SCRATCH:?}/original-mimic-iv/mimic-iv-2.2}"
ORACLE_DB="${ORACLE_DB:-${SCRATCH:?}/oracle/mimic4-full.db}"
OUT_DIR="${OUT_DIR:-$PREFIX/results/job-${SLURM_JOB_ID:-manual}}"

# Deliberately no `module purge`: on the HPC cluster it also drops
# /cm/shared/apps/slurm/current/bin from PATH, which removes srun/sacct.
module load "$PYTHON_MODULE"
# shellcheck disable=SC1091
source "$VENV/bin/activate"

# Keep DuckDB inside the cgroup: one thread per allocated CPU, and a memory
# limit comfortably under --mem so the OOM killer is never the thing that stops
# the job. This job only aggregates transfers (2.2M rows) and admissions
# (431k), so it is far smaller than the chartevents sweep next door.
THREADS="${SLURM_CPUS_PER_TASK:-16}"
MEMORY_LIMIT="${DUCKDB_MEMORY_LIMIT:-96GB}"
TEMP_DIR="${TMPDIR:-${SCRATCH:?}/tmp}/duckdb-${SLURM_JOB_ID:-manual}"

mkdir -p "$OUT_DIR" "$TEMP_DIR"

echo "host:        $(hostname)"
echo "job:         ${SLURM_JOB_ID:-manual}"
echo "cpus:        $THREADS"
echo "mem limit:   $MEMORY_LIMIT"
echo "mimic root:  $MIMIC_ROOT"
echo "oracle db:   $ORACLE_DB"
echo "out dir:     $OUT_DIR"
echo "temp dir:    $TEMP_DIR"
echo

cleanup() { rm -rf "$TEMP_DIR"; }
trap cleanup EXIT

# Single task on a single node, so this runs directly rather than under srun.
python "$PREFIX/patient_birthdate_anchor.py" \
    --mimic-root "$MIMIC_ROOT" \
    --oracle-db "$ORACLE_DB" \
    --out-dir "$OUT_DIR" \
    --threads "$THREADS" \
    --memory-limit "$MEMORY_LIMIT" \
    --temp-dir "$TEMP_DIR"

echo
echo "Done. Results in $OUT_DIR"
ls -la "$OUT_DIR"
