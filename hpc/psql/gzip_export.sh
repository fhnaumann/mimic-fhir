#!/bin/bash
# Compress the psql ndjson export into the shape the MIMIC-on-FHIR IG distributes:
# one <Resource>.ndjson.gz per resource, nothing else.
#
#   sbatch -J psql-gzip --cpus-per-task=32 --mem=16G --time=02:00:00 hpc/psql/gzip_export.sh
#
# WHY: the psql pipeline writes plain ndjson (create_fhir_jsons.sql uses a bare
# \copy), while the published distribution at
#   ${DATASETS:?}/work/mimic4/mimic-iv-on-fhir-1.1.3_af91a8f/fhir
# ships .ndjson.gz. Without this step the psql build cannot be compared to either
# the published artifact or the DuckDB builds on size -- 492 GiB of text against
# 29 GiB of gzip is not a comparison, it is a difference in what was asked for.
#
# THE SOURCE DIRECTORY IS NEVER MODIFIED. It is written to a NEW directory and the
# originals are left byte-for-byte alone, because (a) the full-scale parity jobs
# read those exact files and a half-rewritten export would corrupt a run that has
# hours invested in it, and (b) re-deriving 528 GB costs ~15 h of cluster time.
# The script refuses to start if the destination already holds files.
#
#SBATCH --partition=defq
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --output=logs/%x-%j.out
#SBATCH --error=logs/%x-%j.err
set -euo pipefail

SRC="${SRC:-${SCRATCH:?}/psql-baseline/out/mimic4-export}"
DST="${DST:-${SCRATCH:?}/psql-baseline/out/mimic4-export-gz}"
LEVEL="${LEVEL:-6}"          # gzip default, which is what the IG distribution used
CPUS="${SLURM_CPUS_PER_TASK:-8}"

[ -d "$SRC" ] || { echo "no such source: $SRC" >&2; exit 2; }
mkdir -p "$DST"
if [ -n "$(ls -A "$DST" 2>/dev/null)" ]; then
    echo "destination is not empty: $DST" >&2
    echo "refusing to run -- remove it deliberately or set DST elsewhere" >&2
    exit 2
fi

echo "host:   $(hostname)"
echo "src:    $SRC   (READ ONLY)"
echo "dst:    $DST"
echo "pigz:   $(pigz -V 2>&1 | head -1)   level=${LEVEL}  threads=${CPUS}"
date -Is
START=$(date +%s)

# pigz -c to a new path rather than in place: pigz's default replaces the input.
# One file at a time with all threads, not N files at once -- pigz scales across
# threads on a single file, the step is Lustre-read-bound, and doing them
# sequentially keeps the per-resource timings meaningful.
printf 'file\tlines\traw_bytes\tgz_bytes\tratio\tseconds\n' > "$DST/manifest-gz.tsv"
for f in "$SRC"/*.ndjson; do
    b=$(basename "$f")
    t0=$(date +%s)
    pigz -${LEVEL} -p "$CPUS" -c "$f" > "$DST/${b}.gz"
    t1=$(date +%s)
    raw=$(stat -c %s "$f")
    gz=$(stat -c %s "$DST/${b}.gz")
    printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$b" "" "$raw" "$gz" \
        "$(awk -v a="$raw" -v b="$gz" 'BEGIN{printf "%.2f", a/b}')" "$((t1-t0))" \
        >> "$DST/manifest-gz.tsv"
    printf '  %-45s %14s -> %13s  %5.1fx  %5ds\n' "$b" "$raw" "$gz" \
        "$(awk -v a="$raw" -v b="$gz" 'BEGIN{print a/b}')" "$((t1-t0))"
done

END=$(date +%s)
echo
echo "elapsed_seconds=$((END-START))"
echo "files:      $(ls -1 "$DST"/*.ndjson.gz | wc -l)"
echo "raw_bytes:  $(du -sb "$SRC" | cut -f1)"
echo "gz_bytes:   $(du -sb --exclude=manifest-gz.tsv "$DST" | cut -f1)"
date -Is

# The source must be exactly as it was found. Cheap to assert, and the assertion
# is the point: everything above is a read of files another job depends on.
echo
echo "source unchanged check: $(ls -1 "$SRC"/*.ndjson | wc -l) ndjson files still present, \
$(du -sb "$SRC" | cut -f1) bytes"
