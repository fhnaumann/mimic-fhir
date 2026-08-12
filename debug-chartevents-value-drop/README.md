# debug-chartevents-value-drop

Verification of an upstream `mimic-fhir` defect: the chartevents ETL discards
`chartevents.value` text whenever `valuenum` is set, so numeric observations
that carry a distinct textual meaning are served as a bare number.

Sibling of `debug-timezone-bug/`, same shape, same HPC conventions.

## Layout

    BUG_chartevents_value_text_dropped.md   working doc: mechanism, method, results
    ISSUE_DRAFT.md                          the report to file upstream
    hpc/setup_env.sh                        login-node venv build (duckdb only)
    hpc/chartevents_value_drop.py           the analysis
    hpc/run_chartevents_value_drop.slurm    batch runner
    hpc/results/job-<id>/                   report.txt, summary.json, CSVs

## Running it

    scp hpc/{chartevents_value_drop.py,run_chartevents_value_drop.slurm,setup_env.sh} \
        <hpc-login>:$SCRATCH/debug-chartevents-value-drop/
    ssh <hpc-login> 'cd $SCRATCH/debug-chartevents-value-drop && bash setup_env.sh'
    ssh <hpc-login> 'cd $SCRATCH/debug-chartevents-value-drop && sbatch run_chartevents_value_drop.slurm'

`setup_env.sh` must run on the login node — compute nodes have no outbound
network. DuckDB is pinned to 1.5.5, the version that wrote
`$SCRATCH/oracle/mimic4-full.db`, so `ATTACH` does not hit a storage
format mismatch.

The script also runs on a laptop against any small fixture; it takes the same
arguments and needs nothing but `duckdb`.

## Why there is no warehouse in here

The evidence has to survive contact with upstream, so the served side is
*projected* from `sql/fhir_observation_chartevents.sql` itself rather than
measured against our Delta/Pathling build. Everything else is read from the
PhysioNet distribution. A reader with the MIMIC-IV 2.2 files and DuckDB can
reproduce every number in the report; nothing depends on an artefact only we
hold.

The source is read twice — once from `chartevents.csv.gz`, once from the
prebuilt DuckDB — and the two readings are asserted to agree before any result
is believed, so no number can be attributed to the convenience database.

## Runs

| job | date | note |
|---|---|---|
| `29804197` | 2026-08-12 | first full-cohort run. Findings good, but the source-agreement check reported a spurious 49,071-group mismatch: the prebuilt DuckDB stores `valuenum` single precision, the CSV reads double. Kept for provenance. |
| `29804685` | 2026-08-12 | authoritative. Agreement exact once compared at the coarser precision (and the cast merges 0 groups). Every other number byte-identical to `29804197`. |

Runtime is about 90 seconds — DuckDB aggregates the 313M-row CSV faster than
expected, so the 8-hour wall time in the slurm script is enormously generous.
