#!/bin/bash
# Demo-data smoke test: import -> build -> validate -> export, end to end.
#
#   sbatch hpc/psql/run_demo_smoke.sh
#
# Exercises every mapping file, the config, the locale and the uuid-ossp
# extension in ~10 minutes against the 100-patient demo, on database
# `mimic4-demo`. It does not touch the full `mimic4` database.
#
# NOTE: the demo CANNOT validate the birthDate fix (#126) -- all 100 demo
# patients satisfy year(MIN(transfers.intime)) == anchor_year, so the defect
# measures 0 there. See debug-patient-birthdate-anchor/README.md. Plumbing
# test only; do not quote demo numbers as evidence.
#
#SBATCH --job-name=psql-demo
#SBATCH --partition=defq
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=160G
#SBATCH --time=02:00:00
#SBATCH --output=logs/%x-%j.out
#SBATCH --error=logs/%x-%j.err
set -euo pipefail

# --mem must exceed shared_buffers (128GB) with room to spare, or the postmaster
# fails to allocate its shared memory segment.

ROOT="${ROOT:-${SCRATCH:?}/psql-baseline/mimic-iv-fhir}"
PSQL_HOME="${PSQL_HOME:-${DATASETS:?}/reference/apps/postgresql/15.2}"
export PGDATA="${PGDATA:-${SCRATCH:?}/psql-baseline/pgdata}"

export PATH="${PSQL_HOME}/bin:${PATH}"
export PSQLRC="${ROOT}/mimic-fhir/hpc/psql/psqlrc"
JOB="${SLURM_JOB_ID:-manual}"

echo "host: $(hostname)  job: ${JOB}  cpus: ${SLURM_CPUS_ON_NODE:-?}"
psql --version
date -Is

pg_ctl -D "$PGDATA" -l "${PGDATA}/server-demo-${JOB}.log" -w start
trap 'pg_ctl -D "$PGDATA" -m fast stop || true' EXIT

for step in demo_psql_import demo_psql_fhir demo_psql_validate demo_psql_fhir_export; do
    echo
    echo "================ ${step} ================"
    SECONDS=0
    bash "${ROOT}/scripts/${step}.sh"
    echo ">>> ${step} completed in ${SECONDS}s"
done

echo
echo "================ summary ================"
psql -d mimic4-demo -c "
  SELECT table_name, pg_size_pretty(pg_total_relation_size('mimic_fhir.'||table_name)) AS size
  FROM information_schema.tables WHERE table_schema='mimic_fhir' ORDER BY table_name;"
ls -l "${MIMIC4_OUT_DIR:-${SCRATCH:?}/psql-baseline/out}/mimic4-demo-export" | head -40
date -Is
