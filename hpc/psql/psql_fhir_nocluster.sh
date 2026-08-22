#!/bin/bash
# Build the mimic_fhir tables WITHOUT the final CLUSTER pass.
#
# create_fhir_tables.sql ends with `\i create_table_clusters.sql`, which does a
# full rewrite + sort of all 24 tables including the 313M-row
# observation_chartevents. Because the script starts with DROP SCHEMA, the build
# is not resumable: a wall-time kill costs the entire job. Splitting the CLUSTER
# into its own job keeps each one comfortably inside the 24h cap.
#
# The output is identical either way -- CLUSTER is a physical reorganisation, it
# does not change any row. Run psql_cluster.sh afterwards.
set -euo pipefail

DATABASE="${DATABASE:-mimic4}"
MIMIC_FHIR_DIR="${MIMIC_FHIR_DIR:-$(cd "$(dirname "$0")/../.." && pwd)}"
SQL_DIR="${MIMIC_FHIR_DIR}/sql"
WORK="${TMPDIR:-/tmp}/mimic-fhir-build-$$"

[ -f "${SQL_DIR}/create_fhir_tables.sql" ] || { echo "not found: ${SQL_DIR}/create_fhir_tables.sql"; exit 1; }

mkdir -p "$WORK"
trap 'rm -rf "$WORK"' EXIT

# Comment out only the clustering include; everything else is byte-identical.
sed 's|^\\i create_table_clusters.sql|-- (split out) &|' \
    "${SQL_DIR}/create_fhir_tables.sql" > "${WORK}/create_fhir_tables_nocluster.sql"

if ! grep -q '^-- (split out)' "${WORK}/create_fhir_tables_nocluster.sql"; then
    echo "ERROR: did not find the create_table_clusters.sql include -- has create_fhir_tables.sql changed?"
    exit 1
fi
echo "build script: $(wc -l < "${WORK}/create_fhir_tables_nocluster.sql") lines, CLUSTER include disabled"

# DEVIATION: ON_ERROR_STOP. The coworker's scripts/psql_fhir.sh omits it, so a
# failing mapping is skipped and the build "succeeds" with a table missing --
# which is the last thing you want from a 12h baseline. Set ERROR_STOP=0 to
# restore the original behaviour.
cd "$SQL_DIR" && psql -v ON_ERROR_STOP="${ERROR_STOP:-1}" -d "$DATABASE" \
    -f "${WORK}/create_fhir_tables_nocluster.sql"
