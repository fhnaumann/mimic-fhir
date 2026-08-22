#!/bin/bash
# Run the CLUSTER pass that psql_fhir_nocluster.sh deferred.
#
# Clusters all 24 mimic_fhir tables on their patient_id index, which is what
# gives the ndjson export its patient-grouped ordering.
set -euo pipefail

DATABASE="${DATABASE:-mimic4}"
MIMIC_FHIR_DIR="${MIMIC_FHIR_DIR:-$(cd "$(dirname "$0")/../.." && pwd)}"
SQL_DIR="${MIMIC_FHIR_DIR}/sql"

cd "$SQL_DIR" && psql -v ON_ERROR_STOP=1 -d "$DATABASE" -f create_table_clusters.sql
