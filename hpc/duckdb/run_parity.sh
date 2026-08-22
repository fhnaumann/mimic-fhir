#!/bin/bash
# Compare a DuckDB export against the psql baseline, one shard per job.
#
#   sbatch -J parity-rest        hpc/duckdb/run_parity.sh rest
#   sbatch -J parity-labevents   hpc/duckdb/run_parity.sh labevents
#   sbatch -J parity-chartevents hpc/duckdb/run_parity.sh chartevents
#
# THREE JOBS, NOT ONE. Two resources are 82% of the volume; running all 30 in
# one process means the whole comparison is hostage to chartevents and any
# failure loses everything else with it. Splitting also lets each shard be
# sized honestly instead of every resource paying chartevents' memory bill.
#
# The shards PARTITION the 30 resources -- `rest` is defined by --exclude, not
# by a hand-written list of 28 names, so a resource cannot be dropped from all
# three shards by a typo and leave a partial run that still exits 0. Check the
# union after the fact:
#   jq -r '.resources|keys[]' ${METRICS_DIR}/parity-*.json | sort -u | wc -l   # 30
#
# parity.py is single-threaded by construction, so every shard asks for one
# CPU. The user's constraint on this step is a 24 hour window, not throughput:
# it runs once, and once it passes the DuckDB build never has to be re-proved.
#
#SBATCH --partition=defq
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --output=logs/%x-%j.out
#SBATCH --error=logs/%x-%j.err
set -euo pipefail

SHARD="${1:?usage: run_parity.sh chartevents|labevents|rest}"

PORT_ROOT="${PORT_ROOT:-${SCRATCH:?}/duckdb-port}"
REPO="${REPO:-${PORT_ROOT}/repo}"
VENV="${VENV:-${PORT_ROOT}/venv}"
METRICS_DIR="${METRICS_DIR:-${PORT_ROOT}/metrics}"
SAMPLE_INTERVAL="${SAMPLE_INTERVAL:-60}"

# A = the psql baseline, uncompressed ndjson, 516 GB, COMPLETE and not ours to
# touch. B = the DuckDB export under test.
A="${A:-${SCRATCH:?}/psql-baseline/out/mimic4-export}"
B="${B:-${PORT_ROOT}/fhir-gzip-par}"

JOB="${SLURM_JOB_ID:-nojob}"
TAG="parity-${SHARD}-${JOB}"
mkdir -p "$METRICS_DIR" logs

module load python/3.12.3
PY="${VENV}/bin/python"

# hasMember is an ACCEPTED difference, not a bug. Both backends aggregate panel
# members with an unordered aggregate (jsonb_agg / json_group_array), so element
# order inside the array is unspecified on BOTH sides -- psql does not promise
# it either, and two psql runs need not agree. Ignoring the ordering still
# reports any difference in the SET of members, which is the part that means
# something. It is scoped to that one path on purpose: a bare --ignore
# array_order would hide a genuinely mis-sorted array anywhere else.
IGNORE=(--ignore 'array_order:$.hasMember')

case "$SHARD" in
  chartevents)
    # SIZING: 313.6 M records, the digest path. The id->digest dict is the
    # floor: ~85 B per interned-nothing UUID string, 56 B per 16-byte digest
    # object, ~42 B of dict table = ~183 B x 313.6 M = 57 GB. The B-side
    # duplicate-id set adds 112 B per record but the dict frees 141 B per
    # record as it drains, so total memory FALLS during pass 2 and the peak
    # sits at the start of it. 128 GB is that 57 GB plus room for the
    # fragmentation this allocation pattern causes; it is not 480 GB.
    ONLY=(--only Chartevents)
    ;;
  labevents)
    # SIZING: 118.2 M records, same arithmetic scaled -- ~22 GB of digests.
    ONLY=(--only Labevents)
    ;;
  rest)
    # SIZING: the other 28, compared one at a time, so the peak is the largest
    # single resource and not their sum. Two candidates: medication_administration
    # (27.8 M records, digest path, ~5 GB) and specimen_lab (7.1 GB of text,
    # which lands UNDER the digest threshold and so is held in memory at ~2.5x
    # = 18 GB). The in-memory one is the binding constraint.
    ONLY=(--exclude Chartevents --exclude Labevents)
    ;;
  *)
    echo "unknown shard: $SHARD (want chartevents|labevents|rest)" >&2; exit 2 ;;
esac

echo "host:        $(hostname)"
echo "shard:       ${SHARD}"
echo "A:           ${A}"
echo "B:           ${B}"
echo "filter:      ${ONLY[*]}"
echo "ignore:      ${IGNORE[*]}"
echo "python:      $($PY -VV)"
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
trap '[ -n "$SAMPLER_PID" ] && kill "$SAMPLER_PID" 2>/dev/null || true' EXIT

# cd out of the repo: `import duckdb` from the repo root would find the repo's
# own duckdb/ package instead of the PyPI module (HANDOFF.md trap 16). parity.py
# does not import duckdb, but it lives in that package and the next edit might.
cd /tmp

# parity.py exits 1 on differences, which is a RESULT, not a failure. Under
# `set -e` that would kill the job before the summary below ever prints.
set +e
# -u is not optional. Python block-buffers stdout when it is a file rather than a
# tty, so a multi-hour job emits NOTHING until ~8 KB of report has accumulated.
# The first run of this looked hung for an hour; the only reason it was visibly
# alive is that the cgroup sampler above is bash and writes independently. Per
# resource progress on a 20 hour job is worth one flag.
"$PY" -u "${REPO}/duckdb/parity.py" "$A" "$B" \
    "${ONLY[@]}" "${IGNORE[@]}" \
    --json "${METRICS_DIR}/${TAG}.json"
RC=$?
set -e

date -Is
echo "parity exit status: ${RC}   (0 identical, 1 differences, 2 could not run)"
if [ -s "$SAMPLE_FILE" ]; then
    awk -F, 'NR>1 && $2 != "NA" { n++; if ($2+0 > p) p = $2+0 }
             END { printf "peak_cgroup_memory_bytes=%d (%.1f GiB) from n=%d samples", p, p/1073741824, n
                   if (n < 5) printf "  <-- TOO FEW SAMPLES TO SIZE FROM; cross-check sacct MaxRSS"
                   printf "\n" }' "$SAMPLE_FILE"
fi
echo "report:      ${METRICS_DIR}/${TAG}.json"
exit "$RC"
