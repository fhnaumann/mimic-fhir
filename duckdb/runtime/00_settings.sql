-- Session settings. Placeholders in ${...} are substituted by driver.py from the
-- SLURM allocation -- NOT from the node. This file is not runnable as-is.
--
-- Why substitute rather than let DuckDB choose: DuckDB reads /proc/meminfo and
-- /proc/cpuinfo, not the cgroup. On a partial-node allocation it sees all 64 cores
-- and 503 GB and defaults memory_limit to ~400 GB, then gets OOM-killed by Slurm
-- rather than raising a clean DuckDB error. Set both explicitly, always.

SET threads = ${THREADS};

-- Must be WELL BELOW --mem, not equal to it. memory_limit governs the buffer
-- manager only; JSON string building and the CSV writer's buffers sit outside the
-- tracked pool, so process RSS exceeds the limit. driver.py uses ~60% of --mem.
-- Under-provisioning costs speed (spill pressure), over-provisioning costs nothing.
SET memory_limit = '${MEMORY_LIMIT}';

-- ndjson line order carries no meaning, and this is a large win for a parallel
-- scan -> write pipeline. parity.py sorts by resource id before diffing.
SET preserve_insertion_order = false;

-- Node-local scratch. If left at the default, spills land on Lustre and are slow
-- and silent.
SET temp_directory = '${TEMP_DIR}';

-- Defence in depth. fhir_instant() casts to a naive TIMESTAMP and does not depend
-- on this, but anything that slips past the rewriter should still land in UTC
-- rather than picking up Australia/Sydney and reintroducing issue #124.
SET TimeZone = 'UTC';
