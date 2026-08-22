#!/bin/bash
# Verify the three bug fixes against the completed full `mimic4` build.
#
#   sbatch hpc/psql/verify_fixes.sh
#
# Runs hpc/psql/verify_fixes.sql read-only (TEMP tables only) and tees the
# output to $METRICS_DIR/verify-fixes-<job>.txt as well as the job log.
#
# Sizing differs from run_psql_step.sh on purpose. Every query here is a pure
# SELECT, so unlike the build steps -- where a data-modifying statement forbids
# any parallel plan -- these DO get parallel workers. 16 CPUs is therefore
# useful rather than idle. --mem must still clear shared_buffers = 128GB or the
# postmaster cannot allocate its shared memory segment.
#
# Dominant cost is one sequential scan of mimic_fhir.observation_chartevents
# (313M rows, ~400 GB) plus one of mimiciv_icu.chartevents. Expect well under an
# hour; the 6h wall time is headroom, not an estimate.
#
#SBATCH --job-name=psql-verify-fixes
#SBATCH --partition=defq
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=192G
#SBATCH --time=06:00:00
#SBATCH --output=logs/%x-%j.out
#SBATCH --error=logs/%x-%j.err
set -euo pipefail

ROOT="${ROOT:-${SCRATCH:?}/psql-baseline/mimic-iv-fhir}"
PSQL_HOME="${PSQL_HOME:-${DATASETS:?}/reference/apps/postgresql/15.2}"
export PGDATA="${PGDATA:-${SCRATCH:?}/psql-baseline/pgdata}"
METRICS_DIR="${METRICS_DIR:-${SCRATCH:?}/psql-baseline/metrics}"
DB="${DB:-mimic4}"

# Optional arg: another .sql to run instead, resolved against hpc/psql/.
SQL="${ROOT}/mimic-fhir/hpc/psql/${1:-verify_fixes.sql}"
JOB="${SLURM_JOB_ID:-manual}"
OUT="${METRICS_DIR}/$(basename "$SQL" .sql)-${JOB}.txt"

export PATH="${PSQL_HOME}/bin:${PATH}"
export PSQLRC="${ROOT}/mimic-fhir/hpc/psql/psqlrc"

mkdir -p "$METRICS_DIR"
[ -f "$SQL" ] || { echo "sql not found: $SQL"; exit 1; }

echo "host: $(hostname)  job: ${JOB}  cpus: ${SLURM_CPUS_ON_NODE:-?}"
psql --version
date -Is

# Same guard as run_psql_step.sh: a pid file left by a previous step records a
# PID from a different node, so pg_ctl status would misreport. pg_controldata is
# authoritative.
if [ -f "${PGDATA}/postmaster.pid" ] \
   && pg_controldata -D "$PGDATA" | grep -q '^Database cluster state: *shut down'; then
    echo "removing stale postmaster.pid (cluster state: shut down)"
    rm -f "${PGDATA}/postmaster.pid"
fi

pg_ctl -D "$PGDATA" -l "${PGDATA}/server-verify-${JOB}.log" -w start
trap 'pg_ctl -D "$PGDATA" -m fast -t 600 stop || true' EXIT

SECONDS=0
# -v ON_ERROR_STOP=1 so a broken check fails the job rather than scrolling past.
psql -d "$DB" -v ON_ERROR_STOP=1 -f "$SQL" 2>&1 | tee "$OUT"
rc=${PIPESTATUS[0]}
echo
echo "elapsed=${SECONDS}s  psql_rc=${rc}"
echo "results: $OUT"

echo
echo "================ verdicts ================"
grep -nE '\|\s+(PASS|FAIL)\s*$|SHIFTED|MISSING' "$OUT" || echo "(no verdict lines matched -- read $OUT)"

date -Is
exit "$rc"
