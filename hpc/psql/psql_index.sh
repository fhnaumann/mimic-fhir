#!/bin/bash
# Optional: build the indexes on the SOURCE MIMIC tables.
#
# sql/create_mimic_index.sql is referenced by nothing in the repo or the harness
# -- not create_fhir_tables.sql, not any scripts/*.sh. It indexes the join keys
# the mappings use most (labevents.specimen_id, pharmacy_id, emar.poe_id, ...),
# so it plausibly helps a lot. Treat a run with this step as a VARIANT, not as
# the baseline.
set -euo pipefail

DATABASE="${DATABASE:-mimic4}"
MIMIC_FHIR_DIR="${MIMIC_FHIR_DIR:-$(cd "$(dirname "$0")/../.." && pwd)}"

cd "${MIMIC_FHIR_DIR}/sql" && psql -v ON_ERROR_STOP=1 -d "$DATABASE" -f create_mimic_index.sql
