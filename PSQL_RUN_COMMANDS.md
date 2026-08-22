# psql build — copy-paste commands

> **Temporary scratch doc.** Working command list for the full PostgreSQL baseline run.
> Delete once the run is done; the durable version is `PSQL_BUILD_HPC.md`.

## Setup (once per shell)

```bash
ssh <hpc-login>

BASE=$SCRATCH/psql-baseline
ROOT=$BASE/mimic-iv-fhir
W=$ROOT/mimic-fhir/hpc/psql/run_psql_step.sh
LOGS=$ROOT/mimic-fhir/hpc/psql/logs

export PATH=$DATASETS/reference/apps/postgresql/15.2/bin:$PATH
export PGDATA=$BASE/pgdata

cd $ROOT/mimic-fhir/hpc/psql      # sbatch MUST run from here: --output=logs/%x-%j.out is relative
```

## The sequence

Submit **one at a time**, in this order, waiting for each to finish. Only one postmaster can
hold `$PGDATA`, so overlapping jobs will collide.

The wrapper now requests `--cpus-per-task=8 --mem=192G --time=24:00:00` (not a full exclusive
node): the build is fully serial because PostgreSQL generates no parallel plan for a query that
modifies data, so CPUs beyond ~8 sit idle. Memory is the binding constraint —
`shared_buffers = 128GB` is the floor.

```bash
# 1. import
sbatch -J psql-import   $W scripts/psql_import.sh

# 2. build the mappings  <- the long one, non-resumable
sbatch -J psql-fhir     $W mimic-fhir/hpc/psql/psql_fhir_nocluster.sh

# 3. the deferred CLUSTER pass
sbatch -J psql-cluster  $W mimic-fhir/hpc/psql/psql_cluster.sh

# 4. row-count validation
sbatch -J psql-validate $W scripts/psql_validate.sh

# 5. ndjson export
sbatch -J psql-export   $W scripts/psql_fhir_export.sh
```

`psql_index.sh` is deliberately **not** in the sequence — kept out of the baseline.

## Checking on a job

```bash
squeue -u $USER -o "%.10i %.14j %.8T %.11M %.5D %R"

J=30337186                                    # set to the job you care about
tail -f $LOGS/psql-import-$J.out

# real SQL errors only. A bare `grep ERROR` matches the literal ON_ERROR_STOP=1
# in every harness script's `set -x` trace and gives false positives.
grep -E "^psql:.*(ERROR|FATAL|PANIC)" $LOGS/*-$J.err

# per-step elapsed + peak memory printed by the wrapper
grep -E "step_elapsed_seconds|peak_cgroup_memory" $LOGS/*-$J.out
```

## Metrics, after each job

```bash
bash $ROOT/mimic-fhir/hpc/psql/collect_metrics.sh $J
cat $BASE/metrics/job-$J.metrics.txt
```

Watch `mean_cores_busy` — on a 64-CPU exclusive node, a value near 1–2 confirms the
`INSERT ... SELECT` serialisation that motivates the DuckDB port.

## Capture after the run completes

```bash
# correctness gate: expect 30/30 PASSED, 0 FAILED
grep -c PASSED $BASE/out/mimic4-validation.txt
grep FAILED    $BASE/out/mimic4-validation.txt

# psql-side storage
psql -d mimic4 -f $ROOT/mimic-fhir/tests/test_mimic_fhir_storage_size.sql

# ndjson manifest -- the comparison target for any future DuckDB backend.
# Counts and sizes, NOT checksums: jsonb key ordering differs between engines.
cd $BASE/out/mimic4-export
for f in *.ndjson; do printf '%s\t%s\t%s\n' "$f" "$(wc -l < "$f")" "$(stat -c%s "$f")"; done \
  | tee manifest.tsv
du -sh .
```

## If something goes wrong

```bash
scancel $J

# A killed job can leave a stale postmaster.pid that blocks the next start.
# Check nothing is actually running on the node first, then:
pg_ctl -D $PGDATA status || rm -f $PGDATA/postmaster.pid

# server-side log for the failing job
ls -t $PGDATA/server-*.log | head -3
```

The build step is not resumable — `create_fhir_tables.sql` opens with
`DROP SCHEMA ... CASCADE`, so a wall-time kill means restarting it from zero.

## Paths

| | |
|---|---|
| PGDATA | `$SCRATCH/psql-baseline/pgdata` |
| output | `$SCRATCH/psql-baseline/out` |
| metrics | `$SCRATCH/psql-baseline/metrics` |
| logs | `$SCRATCH/psql-baseline/mimic-iv-fhir/mimic-fhir/hpc/psql/logs` |
| source data | `$DATASETS/source/mimic-iv` (read-only by discipline) |
| databases | `mimic4` (full), `mimic4-demo` (smoke test, done) |
