#!/bin/bash
# Verify and inventory the ndjson export.
#
#   sbatch hpc/psql/export_manifest.sh
#
# For each of the 30 Mimic<Resource>.ndjson files: count lines, record bytes, and
# compare the line count against the expected row count for its source table.
# One JSON object per line, and jsonb serialisation never emits a literal newline,
# so `wc -l` == row count exactly.
#
# Writes <export dir>/manifest.tsv -- the comparison target for any future DuckDB
# backend. Line counts and byte sizes, NOT checksums: PostgreSQL orders jsonb keys
# by length-then-bytes while DuckDB preserves insertion order, so byte-identical
# FHIR content hashes differently between engines.
#
# Reads ~500 GB, hence a job rather than a login-node command.
#
#SBATCH --partition=defq
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=8G
#SBATCH --time=02:00:00
#SBATCH --output=logs/%x-%j.out
#SBATCH --error=logs/%x-%j.err
set -euo pipefail

ROOT="${ROOT:-${SCRATCH:?}/psql-baseline/mimic-iv-fhir}"
EXPORT_DIR="${EXPORT_DIR:-${SCRATCH:?}/psql-baseline/out/mimic4-export}"
SQL_DIR="${ROOT}/mimic-fhir/sql"
MANIFEST="${EXPORT_DIR}/manifest.tsv"

[ -d "$EXPORT_DIR" ] || { echo "export dir not found: $EXPORT_DIR"; exit 1; }

echo "host: $(hostname)  job: ${SLURM_JOB_ID:-manual}"
echo "export dir: $EXPORT_DIR"
date -Is

# --- file -> table, derived from the export SQL so it cannot drift -------------
MAP=$(grep -oE "mimic_fhir\.[a-z_]+|Mimic[A-Za-z]+\.ndjson" \
        "${SQL_DIR}/create_fhir_jsons.sql" | paste - - | sed 's/mimic_fhir\.//')
n_map=$(echo "$MAP" | wc -l)
[ "$n_map" -eq 30 ] || { echo "ERROR: expected 30 file/table pairs, derived ${n_map}"; exit 1; }

# --- table -> expected row count, from the validation SQL ---------------------
EXPECTED=$(grep -oE "SELECT '[a-z_]+' AS tbl, +[0-9]+ AS row_count" \
             "${SQL_DIR}/validate_fhir_tables.sql" \
           | sed -E "s/SELECT '([a-z_]+)' AS tbl, +([0-9]+) AS row_count/\1 \2/")
n_exp=$(echo "$EXPECTED" | wc -l)
[ "$n_exp" -eq 30 ] || { echo "ERROR: expected 30 row counts, derived ${n_exp}"; exit 1; }

# --- count lines, 8 files at a time ------------------------------------------
echo "counting lines in $(ls "${EXPORT_DIR}"/*.ndjson | wc -l) files ..."
SECONDS=0
COUNTS=$(cd "$EXPORT_DIR" && ls -1 *.ndjson | xargs -P 8 -n 1 wc -l \
         | awk '{print $2, $1}')
echo "counted in ${SECONDS}s"

# --- join and report ---------------------------------------------------------
printf 'file\ttable\tlines\texpected\tbytes\tstatus\n' > "$MANIFEST"
fail=0
while read -r file table; do
    lines=$(echo "$COUNTS"   | awk -v f="$file"  '$1==f {print $2}')
    exp=$(echo "$EXPECTED"   | awk -v t="$table" '$1==t {print $2}')
    bytes=$(stat -c%s "${EXPORT_DIR}/${file}")
    if [ -z "$lines" ] || [ -z "$exp" ]; then
        status=MISSING; fail=$((fail+1))
    elif [ "$lines" -eq "$exp" ]; then
        status=PASSED
    else
        status=FAILED; fail=$((fail+1))
    fi
    printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$file" "$table" "${lines:-NA}" "${exp:-NA}" "$bytes" "$status" >> "$MANIFEST"
done <<< "$MAP"

echo
column -t -s $'\t' "$MANIFEST"
echo
awk -F'\t' 'NR>1 {l+=$3; b+=$5} END {
    printf "total_lines=%d  total_bytes=%d (%.1f GiB)\n", l, b, b/1073741824 }' "$MANIFEST"
echo "passed=$(grep -c PASSED "$MANIFEST")  problems=${fail}"
echo "manifest: $MANIFEST"
date -Is
[ "$fail" -eq 0 ] || exit 1
