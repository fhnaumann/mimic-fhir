#!/bin/bash
# Run one step of the DuckDB MIMIC-on-FHIR build inside a SLURM allocation.
#
#   sbatch -J duckdb-stage hpc/duckdb/run_duckdb_step.sh stage
#   sbatch -J duckdb-build hpc/duckdb/run_duckdb_step.sh build gzip
#   sbatch -J duckdb-build hpc/duckdb/run_duckdb_step.sh build zstd
#
#   sbatch -J duckdb-probe hpc/duckdb/run_duckdb_step.sh build gzip patient,specimen_lab
#
# The resource list is the THIRD POSITIONAL ARGUMENT, not an environment variable.
# `sbatch --export=ALL,ONLY=a,b` silently splits on commas: Slurm reads `ONLY=a` and
# then a bare `b` meaning "inherit b from my environment", which does not exist. The
# job then runs a valid, smaller request and exits 0. That cost one probe run
# (30369161 exported 1 resource instead of 2). A positional argument cannot be
# split this way. ONLY= is still honoured as a fallback, but do not use it.
#
# The cgroup sampler below is copied VERBATIM from hpc/psql/run_psql_step.sh. That
# is the point: mean_cores_busy and peak cgroup memory have to be measured the same
# way on both backends, or the comparison this port exists to make is not a
# comparison. Do not "improve" it here without changing it there too.
#
# Sizing is per step and is argued in the SIZING block of each case below. Nothing
# here asks for a whole node.
#
#SBATCH --partition=defq
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --output=logs/%x-%j.out
#SBATCH --error=logs/%x-%j.err
set -euo pipefail

STEP="${1:?usage: run_duckdb_step.sh stage|build [gzip|zstd|none] [only,list]}"
COMPRESSION="${2:-gzip}"
ONLY_ARGV="${3:-}"

PORT_ROOT="${PORT_ROOT:-${SCRATCH:?}/duckdb-port}"
REPO="${REPO:-${PORT_ROOT}/repo}"
VENV="${VENV:-${PORT_ROOT}/venv}"
DATA="${DATA:-${SCRATCH:?}/original-mimic-iv}"
PARQUET="${PARQUET:-${PORT_ROOT}/parquet}"
METRICS_DIR="${METRICS_DIR:-${PORT_ROOT}/metrics}"
SAMPLE_INTERVAL="${SAMPLE_INTERVAL:-60}"
# Comma-separated resource names, or empty for all. The scale probe uses this: it
# still LOADS all 94 files -- which is the full-data binding check that
# `transpile.py --explain` cannot do without staged tables -- but exports only the
# named resources, so it measures throughput without committing to a full run.
ONLY="${ONLY_ARGV:-${ONLY:-}}"

JOB="${SLURM_JOB_ID:-nojob}"
TAG="duckdb-${STEP}-${JOB}"
mkdir -p "$METRICS_DIR" logs

module load python/3.12.3
PY="${VENV}/bin/python"

CPUS="${SLURM_CPUS_PER_TASK:-4}"

# DuckDB reads /proc/meminfo, NOT the cgroup: on a partial allocation it sees the
# whole node, defaults memory_limit to ~400 GB, and gets OOM-killed by Slurm rather
# than raising a clean error. So both threads and memory_limit are always set
# explicitly, from the allocation.
#
# 60% of --mem, not 100%: memory_limit governs DuckDB's buffer manager only. JSON
# string building and the writer's buffers sit outside the tracked pool, and RSS
# was measured at 2.4 GB under a 1 GB limit.
MEM_MB="${SLURM_MEM_PER_NODE:-16384}"
MEM_LIMIT="$(( MEM_MB * 60 / 100 ))MB"

# Node-local scratch. Left at the default, spills land on Lustre and are slow and
# silent. /tmp is per-job, which is also why staging writes to $SCRATCH instead:
# the Parquet has to outlive this job so the gzip and zstd builds read identical
# input.
TEMP_DIR="${TMPDIR:-/tmp}/duckdb-${JOB}"
mkdir -p "$TEMP_DIR"

echo "host:        $(hostname)"
echo "step:        ${STEP} ${COMPRESSION}"
echo "only:        ${ONLY:-<all resources>}"
echo "compression: ${COMPRESSION}  ${SINGLE_STREAM:+(single-stream)}"
echo "cpus:        ${CPUS}   mem: ${MEM_MB}MB   duckdb memory_limit: ${MEM_LIMIT}"
echo "temp_dir:    ${TEMP_DIR}"
echo "python:      $($PY -VV)"
$PY -c 'import duckdb; print("duckdb:     ", duckdb.__version__)'
date -Is

# --- cgroup sampler (verbatim from hpc/psql/run_psql_step.sh) ---------------
SAMPLE_FILE="${METRICS_DIR}/${TAG}.samples.csv"
SAMPLER_PID=""
start_sampler() {
    local cg
    cg=$(awk -F: '/^0::/{print $3}' /proc/self/cgroup 2>/dev/null) || return 0
    cg="/sys/fs/cgroup${cg%%/step_*}"
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

# --- spill sampler (DuckDB only -- deliberately NOT part of the block above) ---
# Answers "was this run memory-constrained?" with evidence instead of inference.
# DuckDB spills to temp_directory when the buffer manager exceeds memory_limit;
# if this stays at 0 for the whole run, the allocation was genuinely sufficient
# and the timings are not hiding disk traffic. Build 30374970 peaked at 247.2 GiB
# against a 288 GiB memory_limit -- 86%, close enough that the question is real.
#
# It is a SEPARATE loop and a separate file on purpose: the sampler above is
# copied verbatim from hpc/psql/run_psql_step.sh so that memory and CPU are
# measured identically on both backends, and adding a column here would break
# that. Postgres has no equivalent knob anyway -- its spilling is governed by
# work_mem per operation and is only visible via log_temp_files or
# pg_stat_database.temp_bytes.
SPILL_FILE="${METRICS_DIR}/${TAG}.spill.csv"
SPILL_PID=""
if [ "$SAMPLE_INTERVAL" -gt 0 ]; then
    echo "spill:       $TEMP_DIR every ${SAMPLE_INTERVAL}s -> $SPILL_FILE"
    echo "epoch,temp_dir_bytes" > "$SPILL_FILE"
    (
        while :; do
            printf '%s,%s\n' "$(date +%s)" \
                "$(du -sb "$TEMP_DIR" 2>/dev/null | cut -f1 || echo NA)" \
                >> "$SPILL_FILE"
            sleep "$SAMPLE_INTERVAL"
        done
    ) &
    SPILL_PID=$!
fi

cleanup() {
    [ -n "$SAMPLER_PID" ] && kill "$SAMPLER_PID" 2>/dev/null || true
    [ -n "$SPILL_PID" ] && kill "$SPILL_PID" 2>/dev/null || true
    # Record the final state BEFORE deleting the evidence.
    if [ -d "$TEMP_DIR" ]; then
        echo "temp_dir_final_bytes=$(du -sb "$TEMP_DIR" 2>/dev/null | cut -f1)"
    fi
    rm -rf "$TEMP_DIR"
}
trap cleanup EXIT

# --- the step ---------------------------------------------------------------
# cd out of the repo: `import duckdb` from the repo root finds the repo's own
# duckdb/ namespace package instead of the PyPI module (HANDOFF.md trap 16).
cd /tmp

case "$STEP" in
  stage)
    # SIZING: 30 tables csv.gz -> Parquet. A .csv.gz is not seekable, so DuckDB
    # reads each file on ONE thread; all the parallelism is across tables and the
    # job cannot finish faster than its largest file (chartevents, 2.5 GB gzip).
    # 12 CPUs covers 8 concurrent readers plus Parquet compression; more would
    # idle. Memory is small and bounded -- CSV->Parquet streams, it does not
    # accumulate -- so 32 GB is headroom, not a requirement.
    "$PY" "${REPO}/duckdb/stage.py" \
        --mimic-root "${DATA}/mimic-iv-2.2" \
        --mimic-root "${DATA}/mimic-iv-ed-2.2" \
        --out "$PARQUET" --jobs 8
    ;;
  build)
    # SIZING: this is the measurement. Threads and memory_limit come from the
    # allocation above.
    OUT="${OUT_DIR:-${PORT_ROOT}/fhir-${COMPRESSION}}"
    EXTRA=()
    # SINGLE_STREAM=1 reproduces psql's one-compressor-per-resource behaviour, for
    # a like-for-like baseline. The default spreads gzip across all threads.
    [ -n "${SINGLE_STREAM:-}" ] && EXTRA+=(--single-stream)
    ONLY_ARG=()
    if [ -n "$ONLY" ]; then
        # A probe writes somewhere else on purpose: a partial export must never be
        # mistaken for, or overwrite, the run parity.py compares against.
        ONLY_ARG=(--only "$ONLY")
        OUT="${PORT_ROOT}/fhir-probe"
    fi
    "$PY" "${REPO}/duckdb/driver.py" \
        --parquet "$PARQUET" --out "$OUT" "${ONLY_ARG[@]}" "${EXTRA[@]}" \
        --compression "$COMPRESSION" \
        --threads "$CPUS" --memory-limit "$MEM_LIMIT" --temp-dir "$TEMP_DIR" \
        --metrics "${METRICS_DIR}/${TAG}.resources.json"
    ;;
  *)
    echo "unknown step: $STEP" >&2; exit 2 ;;
esac

date -Is
if [ -s "$SAMPLE_FILE" ]; then
    # n is reported alongside the peak on purpose. A job shorter than a few sample
    # intervals peaks at whatever startup happened to be, and prints a confident,
    # wrong number: probe 30369161 reported 0.0 GiB from 1 sample while sacct
    # recorded MaxRSS 4.0 GB. Do not size an allocation from a peak with n < 5;
    # re-run with SAMPLE_INTERVAL=5 (no comma, so it is --export safe) instead.
    awk -F, 'NR>1 && $2 != "NA" { n++; if ($2+0 > p) p = $2+0 }
             END { printf "peak_cgroup_memory_bytes=%d (%.1f GiB) from n=%d samples", p, p/1073741824, n
                   if (n < 5) printf "  <-- TOO FEW SAMPLES TO SIZE FROM; cross-check sacct MaxRSS"
                   printf "\n" }' \
        "$SAMPLE_FILE"
fi
if [ -s "$SPILL_FILE" ]; then
    # Peak spill, not final: DuckDB deletes temp files as operators finish, so a
    # run that spilled 200 GB mid-way can end at 0 and look clean.
    awk -F, 'NR>1 && $2 != "NA" { n++; if ($2+0 > p) p = $2+0 }
             END { printf "peak_spill_bytes=%d (%.2f GiB) from n=%d samples", p, p/1073741824, n
                   if (p == 0) printf "  <-- NO SPILLING: memory_limit was sufficient"
                   printf "\n" }' "$SPILL_FILE"
fi
echo "sacct MaxRSS cross-check: sacct -j ${JOB} --format=JobID,MaxRSS,Elapsed"
echo "collect full metrics with: hpc/psql/collect_metrics.sh ${JOB}"
