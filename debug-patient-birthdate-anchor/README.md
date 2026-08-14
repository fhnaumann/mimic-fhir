# debug-patient-birthdate-anchor

Verification of an upstream `mimic-fhir` defect: the Patient ETL synthesises
`Patient.birthDate` from the patient's earliest *transfer* instead of the
MIMIC-IV anchor pair, so ages computed from the published resource disagree
with the age MIMIC itself defines — and neither `anchor_age` nor `anchor_year`
is published anywhere, so the error is neither correctable nor detectable by a
consumer.

Sibling of `debug-timezone-bug/` and `debug-chartevents-value-drop/`, same
shape, same HPC conventions.

## Layout

    BUG_patient_birthdate_anchor.md         working doc: mechanism, method, results
    ISSUE_DRAFT.md                          the report to file upstream
    hpc/setup_env.sh                        login-node venv build (duckdb + pandas)
    hpc/patient_birthdate_anchor.py         the analysis
    hpc/run_patient_birthdate_anchor.slurm  batch runner
    hpc/results/job-<id>/                   report.txt, summary.json, CSVs

## Running it

    scp hpc/{patient_birthdate_anchor.py,run_patient_birthdate_anchor.slurm,setup_env.sh} \
        <hpc-login>:$SCRATCH/debug-patient-birthdate-anchor/
    ssh <hpc-login> 'cd $SCRATCH/debug-patient-birthdate-anchor && bash setup_env.sh'
    ssh <hpc-login> 'cd $SCRATCH/debug-patient-birthdate-anchor && sbatch run_patient_birthdate_anchor.slurm'

`setup_env.sh` must run on the login node — compute nodes have no outbound
network. DuckDB is pinned to 1.5.5, the version that wrote
`$SCRATCH/oracle/mimic4-full.db`, so `ATTACH` does not hit a storage
format mismatch.

The job only aggregates `transfers` (2.2M rows) and `admissions` (431k), so it
is small — the two-hour wall time is generous by an order of magnitude.

### Running it on a laptop

Omit `--mimic-root` and the source tables are read from the DuckDB instead,
which is how this was developed against the demo build:

    python hpc/patient_birthdate_anchor.py \
        --oracle-db ~/warehouses/mimic4-demo.db \
        --out-dir /tmp/demo-run --threads 4 --memory-limit 8GB

## Why there is no warehouse in here

Same reason as the sibling folders: the evidence has to survive contact with
upstream. The published `birthDate` is *projected* from `sql/fhir_patient.sql`
itself rather than measured against our Delta/Pathling build, and everything
else is read from the PhysioNet distribution. A reader holding the MIMIC-IV
v2.2 files and DuckDB can reproduce every number; nothing depends on an
artefact only we hold.

The canonical age formula is likewise transcribed from
`mimic-iv/concepts/demographics/age.sql:30` and then **asserted row-for-row
against `mimiciv_derived.age`** in the prebuilt oracle before any result is
believed. If that check fails the script aborts rather than reporting, because
a mis-transcribed formula would produce a plausible-looking bug that is not
there.

## Status

| step | state |
|---|---|
| mechanism identified | done — `sql/fhir_patient.sql:15` |
| analysis written | done |
| demo-build smoke run | done — cross-check 0/275; tier 1 invisible on demo, so not evidence |
| full-cohort HPC run | done — job `29971523` |
| issue draft | complete, all figures filled from `job-29971523` |

The demo build cannot show the headline defect: all 100 demo patients happen to
satisfy `year(MIN(transfers.intime)) == anchor_year`, so tier 1 measures 0
there. That is exactly the trap that made this bug survive a demo-based probe
during the concept port. Do not quote demo numbers.

## Runs

| job | date | note |
|---|---|---|
| `29971161` | 2026-08-14 | first full-cohort run. Findings good, but elapsed-time age was computed as `floor(days/365.2425)`, which drifts a year at high ages for 1 January birth dates and put a spurious 823-row residual on the proposed fix. Kept for provenance. |
| `29971523` | 2026-08-14 | authoritative. Elapsed-time age computed by the anniversary rule; the fix now returns 0 wrong on both computations. Every other number identical to `29971161`. |

Runtime is 10 seconds — DuckDB aggregates `transfers` and `admissions` far
faster than the two-hour wall time in the slurm script allows for.

## Headline result

    cross-check vs mimiciv_derived.age : 0 mismatches / 431,231

    tier 1 (calendar-year age)  :     460 / 431,231  (0.1067%),  87 patients, error −3..+13
    tier 2 (elapsed-time age)   : 118,225 / 431,231 (27.4157%), 47,899 patients
    after the one-line fix      :       0 wrong on both computations

    charlson 61 · sapsii 69 · oasis 39 admissions change age_score
    creatinine_baseline: 460 mdrd_est rows change

The tier 1 figure and `charlson`'s 61 **independently reproduce the concept-port
runs** in `mimic-code/mimic-iv/concepts_fhir/`, from the source distribution
alone, with no Delta, Pathling or Spark in the path.
