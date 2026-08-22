#!/bin/bash
# Collect resource metrics for a finished psql build job.
#
#   bash hpc/psql/collect_metrics.sh <jobid> [<jobid> ...]
#
# Writes one text file per job into $METRICS_DIR and echoes a one-line summary.
set -euo pipefail

METRICS_DIR="${METRICS_DIR:-${SCRATCH:?}/psql-baseline/metrics}"
mkdir -p "$METRICS_DIR"

[ $# -ge 1 ] || { echo "usage: collect_metrics.sh <jobid> [<jobid> ...]"; exit 1; }

for job in "$@"; do
    out="${METRICS_DIR}/job-${job}.metrics.txt"
    {
        echo "### sacct ###"
        sacct -j "$job" --units=G -o \
            JobID%20,JobName%18,Partition,NodeList%12,AllocCPUS,ReqMem,Elapsed,TotalCPU,UserCPU,SystemCPU,MaxRSS,MaxVMSize,AveRSS,State,ExitCode
        echo
        echo "### seff ###"
        seff "$job" 2>&1 || echo "(seff unavailable)"
        echo
        echo "### derived ###"
        # TotalCPU/Elapsed = mean cores actually busy. On a 64-CPU exclusive node
        # a value near 1-2 means the pipeline is serial -- which is the claim
        # PORT_NOTES_DUCKDB_SPARK.md section 2 makes about INSERT ... SELECT.
        sacct -n -j "${job}.batch" -o Elapsed,TotalCPU,AllocCPUS -P 2>/dev/null | awk -F'|' '
            function secs(t) {
                n = split(t, a, ":")
                if (n == 3) { split(a[1], d, "-"); return (length(d)>1 ? d[1]*86400 + d[2]*3600 : a[1]*3600) + a[2]*60 + a[3] }
                if (n == 2) return a[1]*60 + a[2]
                return t + 0
            }
            NR==1 {
                e = secs($1); c = secs($2)
                if (e > 0) printf "elapsed_s=%d  totalcpu_s=%d  mean_cores_busy=%.2f  allocated_cpus=%s\n", e, c, c/e, $3
            }'

        # Whole-job cgroup peak, from the sampler in run_psql_step.sh. sacct's
        # MaxRSS is per-process, so it double-counts the 128 GB shared_buffers
        # mapped into every backend and misses the sum across parallel workers.
        s=$(ls "${METRICS_DIR}"/*-"${job}".samples.csv 2>/dev/null | head -1 || true)
        if [ -n "${s:-}" ]; then
            echo
            echo "### cgroup samples ($(basename "$s")) ###"
            awk -F, 'NR>1 && $2!="NA" {
                        n++; if ($2>pm) pm=$2; sum+=$2
                        if (first=="") first=$3; last=$3
                     } END {
                        if (n) {
                            printf "samples=%d\n", n
                            printf "peak_memory=%.1f GiB\n", pm/1073741824
                            printf "mean_memory=%.1f GiB\n", (sum/n)/1073741824
                            printf "cpu_seconds=%.0f\n", (last-first)/1e6
                        }
                     }' "$s"
        else
            echo "(no cgroup sample file found for job ${job})"
        fi
    } > "$out" 2>&1

    echo "--- job ${job} -> ${out}"
    grep -E '^(elapsed_s|peak_memory|mean_memory)' "$out" || true
done
