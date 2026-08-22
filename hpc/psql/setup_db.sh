#!/bin/bash
# One-time PostgreSQL cluster setup for the MIMIC-on-FHIR psql build.
# Run on the HPC LOGIN node. Takes about a minute.
#
#   bash hpc/psql/setup_db.sh
#
# Creates the cluster, installs the tuned config, creates the databases and
# proves uuid-ossp works before any 24h job is spent on it.
set -euo pipefail

ROOT="${ROOT:-${SCRATCH:?}/psql-baseline/mimic-iv-fhir}"
PSQL_HOME="${PSQL_HOME:-${DATASETS:?}/reference/apps/postgresql/15.2}"
export PGDATA="${PGDATA:-${SCRATCH:?}/psql-baseline/pgdata}"
CONF_SRC="${CONF_SRC:-${ROOT}/hpc/env/postgresql.conf}"

export PATH="${PSQL_HOME}/bin:${PATH}"

# uuid_generate_v5(uuid_ns_oid(), 'MIMIC-IV') -- the root namespace that every
# resource id in sql/fhir_etl/uuid_namespace.sql derives from. RFC 4122 v5.
EXPECT_UUID="24ba6d92-ae8e-56f9-8898-873d8cba02da"

[ -f "$CONF_SRC" ] || { echo "config not found: $CONF_SRC"; exit 1; }
[ -e "$PGDATA" ] && { echo "PGDATA already exists: $PGDATA -- remove it first to re-init"; exit 1; }

echo "postgres:  $(psql --version)  ($(command -v psql))"
echo "pgdata:    $PGDATA"

mkdir -p "$(dirname "$PGDATA")"
initdb -A trust -E UTF8 -D "$PGDATA"

# The coworker's tuned config, verbatim except for the timezone. en_AU.UTF-8 is
# present on the defq nodes, so the lc_* lines in it are safe to take as-is.
cp "$CONF_SRC" "${PGDATA}/postgresql.conf"

# DEVIATION: cluster timezone UTC rather than Australia/Sydney. MIMIC timestamps
# are timezone-less wall clock values; generating under a DST zone shifts values
# in the spring-forward gap forward by an hour, unrecoverably (see README.md and
# sql/create_fhir_tables.sql:8). That file already does SET TimeZone='UTC' per
# session, so this is defence in depth, not a functional change.
sed -i "s/^timezone = .*/timezone = 'UTC'/;       s/^log_timezone = .*/log_timezone = 'UTC'/" \
    "${PGDATA}/postgresql.conf"
grep -E "^(timezone|log_timezone|shared_buffers|max_parallel_workers) " "${PGDATA}/postgresql.conf"

pg_ctl -D "$PGDATA" -l "${PGDATA}/server-setup.log" -w start
trap 'pg_ctl -D "$PGDATA" -m fast stop || true' EXIT

createdb mimic4
createdb mimic4-demo

for db in mimic4 mimic4-demo; do
    psql -q -d "$db" -c 'CREATE EXTENSION IF NOT EXISTS "uuid-ossp";'
    got=$(psql -At -d "$db" -c "SELECT uuid_generate_v5(uuid_ns_oid(),'MIMIC-IV');")
    if [ "$got" != "$EXPECT_UUID" ]; then
        echo "FAIL ($db): uuid_generate_v5 gave $got, expected $EXPECT_UUID"
        exit 1
    fi
    echo "OK   $db: uuid-ossp present, v5 namespace matches ($got)"
done

echo
echo "Cluster ready. Next: sbatch the import step (see PSQL_BUILD_HPC.md)."
