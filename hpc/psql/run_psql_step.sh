#!/bin/bash
# Run one step of the PostgreSQL MIMIC-on-FHIR build inside a SLURM allocation,
# with PostgreSQL started and stopped around it.
#
#   sbatch -J psql-fhir run_psql_step.sh scripts/psql_fhir.sh
#
# STEP is a path relative to $ROOT (the mimic-iv-fhir harness checkout).
# See PSQL_BUILD_HPC.md for the full sequence.
#
# Sizing: PostgreSQL generates NO parallel plan for a query containing a
# data-modifying operation, so every `INSERT INTO mimic_fhir.x SELECT ...` in the
# build is fully serial -- both the insert and the scan feeding it. CLUSTER is
# single-process too. 8 CPUs is therefore ample; 64 would sit idle.
#
# Memory is the real constraint: shared_buffers = 128GB is the floor, and across
# a 300M-row build those buffers do fill, so the cgroup sees ~128GB resident.
# 192G leaves comfortable headroom for maintenance_work_mem and the OS. Do not
# drop --mem below ~160G without also lowering shared_buffers.
#
#SBATCH --partition=defq
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=192G
#SBATCH --time=24:00:00
#SBATCH --signal=B:TERM@900
#SBATCH --output=logs/%x-%j.out
#SBATCH --error=logs/%x-%j.err
#
# Mail on completion and on failure. TIME_LIMIT_90 is included deliberately: the
# build step is non-resumable, so a warning at 21.6h is the only chance to see a
# 24h overrun coming before the job is killed.
set -euo pipefail

# ---- the only paths that need editing -------------------------------------
ROOT="${ROOT:-${SCRATCH:?}/psql-baseline/mimic-iv-fhir}"
PSQL_HOME="${PSQL_HOME:-${DATASETS:?}/reference/apps/postgresql/15.2}"
export PGDATA="${PGDATA:-${SCRATCH:?}/psql-baseline/pgdata}"
METRICS_DIR="${METRICS_DIR:-${SCRATCH:?}/psql-baseline/metrics}"
SAMPLE_INTERVAL="${SAMPLE_INTERVAL:-60}"   # seconds; 0 disables cgroup sampling
# ---------------------------------------------------------------------------

STEP="${1:?usage: sbatch run_psql_step.sh <step, e.g. scripts/psql_fhir.sh>}"
JOB="${SLURM_JOB_ID:-manual}"
TAG="${SLURM_JOB_NAME:-psql}-${JOB}"

export PATH="${PSQL_HOME}/bin:${PATH}"
# \timing on for every statement, without modifying any SQL in the repo.
export PSQLRC="${PSQLRC:-${ROOT}/mimic-fhir/hpc/psql/psqlrc}"

mkdir -p "$METRICS_DIR"

[ -d "$PGDATA" ]          || { echo "PGDATA not initialised: $PGDATA (run setup_db.sh)"; exit 1; }
[ -f "${ROOT}/${STEP}" ]  || { echo "step not found: ${ROOT}/${STEP}"; exit 1; }

echo "=== $TAG ==="
echo "host:        $(hostname)"
echo "step:        ${STEP}"
echo "cpus:        ${SLURM_CPUS_ON_NODE:-?} (SLURM_JOB_CPUS_PER_NODE=${SLURM_JOB_CPUS_PER_NODE:-?})"
echo "pgdata:      ${PGDATA}"
echo "postgres:    $(psql --version)"
date -Is

# --- cgroup sampler --------------------------------------------------------
# sacct's MaxRSS is the peak RSS of a *single* process. PostgreSQL maps its
# 128 GB shared_buffers into every backend, so MaxRSS both double-counts shared
# pages and misses the sum across parallel workers. The cgroup counters are the
# honest whole-job numbers, so sample them too. memory.peak is not exposed on
# this kernel, hence the polling.
SAMPLE_FILE="${METRICS_DIR}/${TAG}.samples.csv"
SAMPLER_PID=""
start_sampler() {
    local cg
    cg=$(awk -F: '/^0::/{print $3}' /proc/self/cgroup 2>/dev/null) || return 0
    cg="/sys/fs/cgroup${cg%%/step_*}"          # climb to the job-level cgroup
    [ -r "$cg/memory.current" ] || { echo "cgroup counters unreadable, skipping sampler"; return 0; }
    echo "sampling:    $cg every ${SAMPLE_INTERVAL}s -> $SAMPLE_FILE"
    echo "epoch,mem_current_bytes,cpu_usage_usec" > "$SAMPLE_FILE"
    (
        while :; do
            printf '%s,%s,%s\n' \
                "$(date +%s)" \
                "$(cat "$cg/memory.current" 2>/dev/null || echo NA)" \
                "$(awk '/^usage_usec/{print $2}' "$cg/cpu.stat" 2>/dev/null || echo NA)" \
                >> "$SAMPLE_FILE"
            sleep "$SAMPLE_INTERVAL"
        done
    ) &
    SAMPLER_PID=$!
}
[ "$SAMPLE_INTERVAL" -gt 0 ] && start_sampler

cleanup() {
    [ -n "$SAMPLER_PID" ] && kill "$SAMPLER_PID" 2>/dev/null || true
    # -t 600: pg_ctl's default 60s wait is far too short here. The shutdown
    # checkpoint has to flush a full 128GB buffer pool -- it took 64s after the
    # build step -- and when pg_ctl gives up it leaves postmaster.pid behind.
    # Since PGDATA is on Lustre and each step runs on a different node, a stale
    # pid file whose PID is live-but-unrelated on the next node makes PostgreSQL
    # refuse to start ("Is another postmaster running?").
    pg_ctl -D "$PGDATA" -m fast -t 600 stop || true
}
trap cleanup EXIT

# --- run -------------------------------------------------------------------
# Clear a stale lock file left by a previous step. `pg_ctl status` is not a safe
# test here: on a different node the recorded PID may belong to an unrelated live
# process, and pg_ctl would wrongly report "server is running". pg_controldata is
# authoritative -- "shut down" means no postmaster is attached to this PGDATA.
if [ -f "${PGDATA}/postmaster.pid" ] \
   && pg_controldata -D "$PGDATA" | grep -q '^Database cluster state: *shut down'; then
    echo "removing stale postmaster.pid (cluster state: shut down)"
    rm -f "${PGDATA}/postmaster.pid"
fi

pg_ctl -D "$PGDATA" -l "${PGDATA}/server-${TAG}.log" -w start

SECONDS=0
bash "${ROOT}/${STEP}"
ELAPSED=$SECONDS

echo
date -Is
echo "step_elapsed_seconds=${ELAPSED}  ($((ELAPSED/3600))h $(((ELAPSED%3600)/60))m)"
if [ -s "$SAMPLE_FILE" ]; then
    awk -F, 'NR>1 && $2!="NA" { if ($2>p) p=$2 } END {
        printf "peak_cgroup_memory_bytes=%d (%.1f GiB)\n", p, p/1073741824 }' "$SAMPLE_FILE"
fi
echo "collect full metrics with: hpc/psql/collect_metrics.sh ${JOB}"
