#!/bin/bash
# Rebuild the Pathling Delta warehouse from a published NDJSON build.
#
#   D=$(sbatch --parsable -J warehouse-decompress --cpus-per-task=16 --mem=32G \
#       --time=06:00:00 hpc/warehouse/rebuild_delta_warehouse.sh decompress)
#   sbatch -J warehouse-ingest --dependency=afterok:$D --exclusive \
#       --cpus-per-task=64 --mem=480G --time=12:00:00 \
#       hpc/warehouse/rebuild_delta_warehouse.sh ingest
#
# Writes to ${WAREHOUSE}.new and never touches ${WAREHOUSE}. The swap is a
# separate, deliberate step performed by a human after looking at the numbers:
#
#   mv spark_warehouse spark_warehouse.old-<date> && mv spark_warehouse.new spark_warehouse
#
# WHY THE DECOMPRESS STEP EXISTS. Spark cannot read .zst at all -- the pyspark
# wheel ships no native Hadoop codecs, so a read fails with FAILED_READ_FILE
# (verified, not assumed). And .gz, which it can read, is not splittable: the
# whole of MimicObservationChartevents (250 GB uncompressed) would be one task on
# one core. Plain NDJSON is line-splittable, so Spark parallelises across the
# corpus. The cost is 470 GiB of scratch, which is a rounding error against 29 TB
# free and is deleted by the caller once the warehouse is verified.
#
#SBATCH --partition=defq
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --output=logs/%x-%j.out
#SBATCH --error=logs/%x-%j.err
set -euo pipefail

STEP="${1:?usage: rebuild_delta_warehouse.sh decompress|ingest}"

SRC="${SRC:-${DATASETS:?}/work/mimic4/mimic-iv-on-fhir-1.1.3-fixes_2026-08-22/duckdb-zstd/fhir}"
ROOT="${ROOT:-${SCRATCH:?}/mimic-on-fhir-delta}"
STAGING="${STAGING:-${ROOT}/staging-ndjson}"
WAREHOUSE="${WAREHOUSE:-${ROOT}/spark_warehouse}"
TARGET="${TARGET:-${WAREHOUSE}.new}"
# Per-file uncompressed byte counts, measured during the build (BENCHMARK.md §7).
# Decompression is checked against these rather than against "it exited 0".
EXPECTED_SIZES="${EXPECTED_SIZES:-${ROOT}/raw-zstd.tsv}"

CPUS="${SLURM_CPUS_PER_TASK:-4}"
JOB="${SLURM_JOB_ID:-nojob}"
mkdir -p logs

echo "host:  $(hostname)   job: ${JOB}   step: ${STEP}   cpus: ${CPUS}"
date -Is
START=$(date +%s)

case "$STEP" in
  decompress)
    # SIZING: 30 files, 24 GiB in / 470 GiB out, pure I/O. zstd -d is single
    # threaded per file, so parallelism is across files and 16 covers it -- the
    # job cannot finish faster than chartevents (250 GB) on one core regardless.
    # Memory is a fixed decompression window per stream, not a function of size.
    [ -d "$SRC" ] || { echo "no such source: $SRC" >&2; exit 2; }
    mkdir -p "$STAGING"

    # A previous partial run is the dangerous case: a truncated .ndjson reads as
    # valid but short, and the warehouse silently loses rows. The size check at
    # the end catches it, but starting clean means never relying on that.
    if [ -n "$(ls -A "$STAGING" 2>/dev/null)" ]; then
        echo "staging is not empty: $STAGING" >&2
        echo "remove it deliberately before re-running -- a partial decompress" >&2
        echo "produces short files that parse fine and lose rows silently." >&2
        exit 2
    fi

    echo "decompressing ${SRC} -> ${STAGING}"
    ls -1 "$SRC"/*.ndjson.zst | xargs -P "$CPUS" -I{} bash -c '
        f="$1"; out="'"$STAGING"'/$(basename "$f" .zst)"
        zstd -dq --force -o "$out" "$f"
    ' _ {}

    echo
    echo "=== size verification against ${EXPECTED_SIZES}"
    FAIL=0
    total=0
    while read -r want name; do
        plain="${STAGING}/$(basename "$name" .zst)"
        got=$(stat -c%s "$plain" 2>/dev/null || echo missing)
        total=$((total + ${got//missing/0}))
        if [ "$got" != "$want" ]; then
            echo "    MISMATCH $(basename "$name" .zst): expected ${want}, got ${got}" >&2
            FAIL=1
        fi
    done < "$EXPECTED_SIZES"
    echo "    total staged bytes: ${total}"
    [ "$FAIL" -eq 0 ] || { echo "decompression verification failed" >&2; exit 3; }
    echo "    OK -- all 30 files match their measured uncompressed size"
    ;;

  ingest)
    # SIZING: whole node. This is a parse-and-encode pass over 470 GiB with a
    # write at the end; it is CPU bound on FHIR encoding and benefits from every
    # core. 480G is not a measured requirement -- it is what an exclusive node
    # has, and Spark will use what it is given for its parse buffers.
    [ -d "$STAGING" ] || { echo "no staging dir: $STAGING -- run the decompress step" >&2; exit 2; }
    n=$(ls -1 "$STAGING"/*.ndjson 2>/dev/null | wc -l)
    [ "$n" -eq 30 ] || { echo "expected 30 staged files, found ${n}" >&2; exit 2; }
    if [ -e "$TARGET" ]; then
        echo "target already exists: $TARGET" >&2
        echo "refusing to write into it -- move it aside first." >&2
        exit 2
    fi

    module load python/3.12.3
    module load amazon-corretto/21.0.0.35.1
    cd "$ROOT"

    # Node-local scratch for shuffle and spill. Left at the default these land on
    # Lustre, where they are slow and shared with everyone else on the filesystem.
    SPARK_TMP="${TMPDIR:-/tmp}/spark-${JOB}"
    mkdir -p "$SPARK_TMP"

    export PYSPARK_SUBMIT_ARGS="--master local[${CPUS}] \
      --conf spark.driver.memory=380g \
      --conf spark.driver.maxResultSize=8g \
      --conf spark.local.dir=${SPARK_TMP} \
      --conf spark.sql.shuffle.partitions=$((CPUS * 4)) \
      --conf spark.default.parallelism=$((CPUS * 4)) \
      pyspark-shell"

    echo "spark:  local[${CPUS}]  driver 380g  tmp ${SPARK_TMP}"
    echo "source: ${STAGING}"
    echo "target: ${TARGET}"
    echo

    # The venv interpreter directly, not `uv run`: uv lives in an interactive
    # shell's PATH only, and this script runs under sbatch. The venv is the one
    # that wrote the existing warehouse -- pathling 9.6.0, Spark 4.0.2 -- so the
    # rebuild lands the same encoder schema the HPC analysis code already reads.
    "${ROOT}/.venv/bin/python" step1_ndjson_to_delta.py \
        --source "$STAGING" --target "$TARGET" --extension ndjson

    echo
    echo "=== result"
    du -sh "$TARGET"
    ls -1 "$TARGET"
    rm -rf "$SPARK_TMP"
    ;;

  *)
    echo "unknown step: $STEP" >&2; exit 2 ;;
esac

END=$(date +%s)
echo
echo "elapsed_seconds=$((END-START))"
date -Is
