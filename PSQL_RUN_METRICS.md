# psql baseline run — metrics log

> **Temporary scratch doc.** Records the measured cost of each step of the PostgreSQL
> MIMIC-IV-on-FHIR baseline build, so the later DuckDB comparison has a real number to
> beat rather than the README's "~12 h on 24 cores".
> Commands that produced these numbers: `PSQL_RUN_COMMANDS.md`. Durable procedure:
> `PSQL_BUILD_HPC.md`. Fold the useful parts into `PSQL_BUILD_HPC.md` §7 and delete this.

## Status

| # | Step | Job | State | Elapsed | Peak mem | Mean cores |
|---|------|-----|-------|---------|----------|------------|
| 0 | demo smoke test | 30336753 | ✅ COMPLETED | 00:01:24 | 4.3 GiB | 0.81 |
| 1 | `psql_import.sh` | 30337394 | ✅ COMPLETED | **00:40:01** | **73.8 GiB** | **1.68** |
| 2 | `psql_fhir_nocluster.sh` | 30339566 | ✅ COMPLETED | **07:16:37** | **135.1 GiB** | **1.19** |
| 3 | `psql_cluster.sh` | 30353020 | ✅ COMPLETED | **05:54:05** | **119.3 GiB** | **0.87** |
| 4 | `psql_validate.sh` | 30362384 | ✅ COMPLETED **30/30 PASSED** | **00:04:17** | **136.3 GiB** | **2.56** |
| 5 | `psql_fhir_export.sh` | 30363082 | ✅ COMPLETED | **00:57:36** | **113.6 GiB** | **1.43** |
| 6 | `export_manifest.sh` (verify) | 30368919 | ✅ COMPLETED **30/30 PASSED** | 00:01:45 | — | 1.88 |
| 7 | `verify_fixes.sh` (fixes #124/#125/#126) | 30372926 | ✅ COMPLETED **all PASS** | 00:05:07 | 72.0 GiB | 7.60 |

**Total for the five build steps: 14h 54m.** Artifact: 30 ndjson files, 492.2 GiB,
555,352,597 lines, every file line-count-verified against its expected row count.

Cancelled / superseded: `30337186` (psql-import, cancelled before start to re-request
8 CPU / 192 G instead of a full exclusive node).

## Allocation under test

Every step runs through `hpc/psql/run_psql_step.sh` with the same request:

```
--nodes=1 --ntasks=1 --cpus-per-task=8 --mem=192G --time=24:00:00   partition defq
```

Node class: 64 CPU / 503 GB. PostgreSQL 15.2 from
`$DATASETS/reference/apps/postgresql/15.2`, config from
`hpc/env/postgresql.conf` (`shared_buffers=128GB`, `work_mem=512MB`,
`maintenance_work_mem=4GB`), `timezone=UTC`, PGDATA on Lustre
(`$SCRATCH/psql-baseline/pgdata`).

---

## Job 30337394 — import (step 1)

Loads MIMIC-IV v2.2 + MIMIC-IV-ED v2.2 CSVs into the `mimiciv_*` source schemas and
self-validates row counts.

```
State           COMPLETED (0:0)        Node        c340 (partition h24)
Submit/Start    2026-08-21T20:51:39    End         2026-08-21T21:31:40
Elapsed         00:40:01               AllocCPUS   8
TotalCPU        01:07:03               ReqMem      192G
  UserCPU         41:57.940
  SystemCPU       25:05.949
```

**Wrapper-reported:** `step_elapsed_seconds=2364`, `peak_cgroup_memory_bytes=79227224064` (73.8 GiB)

**Derived:** `mean_cores_busy = 4023 s CPU / 2401 s wall = 1.68` of 8 allocated
(seff: CPU efficiency 20.95%, memory efficiency 40.42%)

**Correctness:** 34 PASSED / 0 FAILED row-count checks; 0 lines matching
`^psql:.*(ERROR|FATAL|PANIC)` in the `.err` log.

### Where the 40 minutes went

Summed statement time is **2363 s against 2364 s of wall clock** — there is essentially no
dead time in this step; it is SQL end to end.

| | |
|---|---|
| COPY statements | 37 |
| Rows loaded | 645,554,021 |
| Time in COPY | 1573 s (67%) |
| Time in everything else | 790 s (33%) — index builds, PK/FK constraints, validation |
| Mean COPY throughput | ~410k rows/s |

Slowest statements:

| Table | Rows | Time | Rows/s |
|---|---|---|---|
| `chartevents` | 313,645,063 | 626.5 s | 501k |
| `labevents` | 118,171,367 | 362.6 s | 326k |
| `emar_detail` | 54,744,789 | 125.8 s | 435k |
| `emar` | 26,850,359 | 91.3 s | 294k |
| `poe` | 39,366,291 | 83.4 s | 472k |
| `pharmacy` | 13,584,514 | 61.5 s | 221k |
| `prescriptions` | 15,416,708 | 53.7 s | 287k |
| `inputevents` | 8,978,893 | 46.1 s | 195k |

`chartevents` alone is 40% of the COPY time and 49% of the rows — the same table that
dominates the build step (`observation_chartevents`, 313,645,032 expected rows).

### Reading these numbers

**The 8-CPU sizing is confirmed correct.** 1.68 mean cores busy on an 8-CPU allocation
means 62 of a full node's 64 cores would have been idle. This is the expected result, not a
surprise: PostgreSQL generates **no parallel plan at all** for a statement containing a
data-modifying operation, and `COPY` is single-process. The 0.68 above 1.0 is the checkpointer,
WAL writer and background workers, plus the parallel index builds
(`max_parallel_maintenance_workers=16`) in the 790 s tail.

**`mem=192G` should stay.** Peak resident was 73.8 GiB — *below* `shared_buffers=128GB`,
meaning import never touched most of the buffer pool. The build step reads and writes far
more data and will push closer to the 128 GB floor, so this is not headroom to reclaim.

**`sacct` MaxRSS (77.60 G) reads slightly higher than the cgroup peak (73.8 GiB).** Expected,
and the reason the wrapper samples cgroups at all: MaxRSS is the peak RSS of one process and
counts shared pages once per backend, while `memory.current` counts them once for the whole
job. Prefer the cgroup figure. Caveat in the other direction: the sampler polls every 60 s
(`memory.peak` is not exposed on this kernel), so a short spike between samples is invisible.

**This does not predict the build step.** Import is Lustre-write-bound; the build is
CPU-bound inside `jsonb_build_object` over the same 313M chartevents rows. The only thing
import tells us about step 2 is that the source data is complete and correctly loaded.

---

## Job 30339566 — build (step 2)

`create_fhir_tables.sql` with the trailing `\i create_table_clusters.sql` disabled
(`psql_fhir_nocluster.sh`), `ON_ERROR_STOP=1`, database `mimic4`.

```
State           COMPLETED (0:0)        Node        c302
Start           2026-08-21T21:53:27    AllocCPUS   8
End             2026-08-22T05:10:04    ReqMem      192G
Elapsed         07:16:37
TotalCPU        08:37:57
```

| Metric | Value |
|---|---|
| **Wall time** | **07:16:37** (`step_elapsed_seconds=26126`) |
| **Peak memory** | **135.1 GiB** (mean 124.8 GiB over 436 samples) |
| **CPU** | 8:37:57 total → **mean 1.19 cores busy** of 8 allocated |
| CPU efficiency (seff) | 14.83% |
| Memory utilised (seff) | 135.70 GB = 70.68% of 192 G |
| SQL errors | 0 |
| Rows inserted | 555,434,043 across 98 `INSERT` statements |

**Finished in 7h16m against the 24 h cap — 30% of the slot, ~17 h of headroom.** The
non-resumability risk we accepted never came close to biting.

### Where the 7¼ hours went

Summed statement time is 26,126 s — identical to wall clock, so again the step is SQL end to
end with no dead time.

| Resource | Rows | Time | Share |
|---|---|---|---|
| `observation_chartevents` | 313,645,032 | 12,548 s (3h29m) | 48% |
| `observation_labevents` | 118,171,367 | 7,046 s (1h57m) | 27% |
| `medication_administration` | 27,754,178 | 1,701 s | 7% |
| `medication_request` | 13,533,220 | 797 s | 3% |
| `procedure` | 12,689,766 | 582 s | 2% |
| everything else (93 statements) | ~69.6M | ~3,552 s | 14% |

**Two tables are 75% of the entire build.** That is the headline number for the DuckDB
comparison: any port that does not accelerate `observation_chartevents` and
`observation_labevents` cannot move the total much, no matter what it does to the other 28
resources.

### Reading these numbers

**Memory is confirmed as the binding constraint, and 192 G was the right call.** Peak 135.1 GiB
with a mean of 124.8 GiB means the `shared_buffers=128GB` pool did fill and stay full for
essentially the whole run — exactly what import (73.8 GiB peak) failed to predict. Dropping
`--mem` to 160 G would have left only ~25 GiB of margin; anything below `shared_buffers` would
have failed at postmaster start.

**CPU went *down* under load: 1.19 mean cores vs import's 1.68.** Not a measurement error — the
build is one long serial `INSERT ... SELECT` per resource with no parallel index builds to
inflate the average, so it sits closer to a true single core. It also means the effective cost
of this build was ~1.2 cores for 7¼ hours; the 8-CPU request is still the right shape but even
4 would have sufficed.

**`sacct` MaxRSS reports 142287664K (135.7 GiB)** against the cgroup's 135.1 GiB — the two
agree here, unlike the import step, because a single backend holds essentially the whole
buffer pool for the duration.

### Shutdown warning in the log — benign, but it left a stale lock file

The job log ends with `waiting for server to shut down......... failed`. This is **not** a
failure of the build:

- `pg_controldata` reports `Database cluster state: shut down`
- the server log ends with a completed `shutdown immediate` checkpoint (160,656 buffers, 4.3 s)
- the job exited `0:0`

What happened is that `pg_ctl -m fast stop` waits 60 s by default, and the final shutdown
checkpoint took ~64 s (it had to flush a full 128 GB buffer pool), so `pg_ctl` gave up
reporting while the postmaster went on to shut down cleanly a few seconds later.

The side effect: **`postmaster.pid` was left behind** (PID 19728). This matters because PGDATA
is on Lustre and each step runs on a *different* node — if PID 19728 happens to be a live
unrelated process on the next node, PostgreSQL refuses to start with
`Is another postmaster running?`. Removed manually after confirming the cluster state was
`shut down`. **Expect this after every step** and clear it before the next submit:

```bash
pg_ctl -D $PGDATA status || rm -f $PGDATA/postmaster.pid
```

The clean fix, if this run is ever repeated, is `pg_ctl -m fast -t 600 stop` in
`run_psql_step.sh`'s `cleanup()`.

---

## Job 30353020 — CLUSTER (step 3)

The 24 `CLUSTER mimic_fhir.<t> USING idx_<t>_patient_id` statements split out of
`create_fhir_tables.sql`, run as `psql_cluster.sh`.

```
State           COMPLETED (0:0)        Node        c094
Start           2026-08-22T07:02:52    AllocCPUS   8
End             2026-08-22T12:56:57
Elapsed         05:54:05
TotalCPU        05:06:29
```

| Metric | Value |
|---|---|
| **Wall time** | **05:54:05** (`step_elapsed_seconds=21216`) |
| **Peak memory** | **119.3 GiB** (354 samples) |
| **CPU** | 5:06:29 total → **mean 0.87 cores busy** of 8 |
| CPU efficiency (seff) | 10.82% |
| Memory utilised (seff) | 120.13 GB = 62.57% of 192 G |
| SQL errors | 0 |
| Statements | 24 / 24 |

Summed statement time 21,216 s = wall clock exactly. Third step in a row with no dead time.

### Cost per table

| # | Table | Time |
|---|---|---|
| 11 | `observation_chartevents` | 1h 24m 13s |
| 9 | `medication_request` | 1h 08m 20s |
| 14 | `observation_labevents` | 43m 45s |
| 5 | `medication_administration` | 41m 54s |
| 7 | `medication_dispense` | 34m 10s |
| 19 | `observation_vital_signs` | 28m 28s |
| 12 | `observation_datetimeevents` | 19m 52s |
| 6 | `medication_administration_icu` | 18m 37s |
| others (16 tables) | | < 10m each |

**`CLUSTER` cost does not track row count.** `medication_request` (13.5 M rows) took 1h 08m
while `observation_chartevents` (313 M rows — 23× more) took 1h 24m. Cost is driven by heap
byte size and the number of indexes to rebuild, not tuple count. Do not size this step from
row counts.

### Reading these numbers

**Mean 0.87 cores — the lowest of the three steps.** `CLUSTER` is single-process for the heap
rewrite; the only parallelism is the per-table index rebuild afterwards
(`max_parallel_maintenance_workers=16`), which is bursty. A spot sample taken mid-run during
one of those rebuilds read ~3.25 cores, so instantaneous readings are not representative —
only the whole-run average is.

**Memory 119.3 GiB, just under `shared_buffers`.** Consistent with the build. The step is
I/O-bound on Lustre, not memory- or CPU-bound.

**Total for the three heavy steps: 13h 51m** (00:40 + 07:17 + 05:54). Splitting `CLUSTER` out
of the build was the right call — as one job it would have been 13h 10m against a 24 h cap
with no resume, instead of two independently-retryable jobs.

### The shutdown fix worked

`run_psql_step.sh` was changed before this job to use `pg_ctl -m fast -t 600 stop` plus a
`pg_controldata`-guarded stale-lock clear. Result: the log ends
`waiting for server to shut down......... done`, no `postmaster.pid` was left behind, and
`pg_controldata` reports `shut down`. Contrast with the build step, which needed manual
cleanup. The guarded clear itself did not fire here (PGDATA was already clean on entry), so it
remains untested — it is insurance for steps 4 and 5.

---

## Job 30362384 — validation (step 4) — the correctness gate

`validate_fhir_tables.sql` via `scripts/psql_validate.sh`: `count(*)` on all 30 FHIR tables
compared against the hardcoded upstream expected counts.

```
State           COMPLETED (0:0)        Node        c205
Elapsed         00:04:17               TotalCPU    00:10:58
```

| Metric | Value |
|---|---|
| **Result** | **30 PASSED / 0 FAILED** |
| **Wall time** | **00:04:17** (`step_elapsed_seconds=232` — one statement) |
| **Peak memory** | **136.3 GiB** — the highest of any step |
| **CPU** | 10:58 → **mean 2.56 cores busy** of 8 |
| SQL errors | 0 |

Full output: `$SCRATCH/psql-baseline/out/mimic4-validation.txt`.
**Note the output does not go to the job log** — `grep PASSED` on the `.out` file returns 0 and
means nothing. Beware also that `out/` contains `mimic4-demo-validation.txt` from the smoke
test; a glob like `ls out/*valid* | head -1` picks the *demo* file. Name it explicitly.

### What this does and does not prove

Every table matches exactly, including the two giants — `observation_chartevents`
313,645,032 and `observation_labevents` 118,171,367.

This settles one open question: **the chartevents `value`/`valuenum` component fix (#125) did
not change row cardinality.** It alters the content of a component inside each Observation, not
how many Observations are emitted, and the count confirms it.

It says nothing about the birthDate anchor fix (#126), which changes `Patient.birthDate`
*values* while leaving `patient` at 299,712 rows. Row-count validation cannot see it. That fix
needs its own value-level check — see `debug-patient-birthdate-anchor/`.

More generally: 30/30 PASSED means no resource was silently dropped or duplicated. It is not
evidence that field-level content is correct.

### Parallelism finally appears

**2.56 mean cores — the highest of the four steps**, and the reason is that this is the first
*read-only* step. `max_parallel_workers_per_gather = 32` is only eligible when nothing is being
modified, so the count scans fanned out across workers; the three write-heavy steps were
disqualified from parallel plans entirely. Harmless here (the setting is oversubscribed against
8 allocated CPUs, but SLURM caps it), and it is why this step counted 555 M rows in under
4 minutes.

Peak memory 136.3 GiB, slightly above the build's 135.1 — full-table scans pulled the whole
buffer pool resident plus per-worker `work_mem`.

---

## Job 30363082 — ndjson export (step 5)

`create_fhir_jsons.sql` via `scripts/psql_fhir_export.sh`: 30 `\copy` statements, one per FHIR
resource table, to `$SCRATCH/psql-baseline/out/mimic4-export`.

| Metric | Value |
|---|---|
| **Wall time** | **00:57:36** (`step_elapsed_seconds=3433`) |
| **Peak memory** | 113.6 GiB |
| **CPU** | 1:22:30 → mean 1.43 cores of 8 |
| **Output** | **30 files, 492.2 GiB, 555,352,597 lines** |
| SQL errors | 0 |

Uncompressed — `create_fhir_jsons.sql` uses plain `\copy ... WITH CSV QUOTE AS E'\b'
DELIMITER E'\001'`, a trick to emit raw JSON without CSV escaping. No gzip anywhere in the
psql path.

### Size distribution

| File | Bytes | Share |
|---|---|---|
| `MimicObservationChartevents.ndjson` | 261,975,562,580 | 50% |
| `MimicObservationLabevents.ndjson` | 158,740,107,826 | 30% |
| `MimicMedicationAdministration.ndjson` | 25,054,007,746 | 5% |
| `MimicMedicationRequest.ndjson` | 15,955,159,998 | 3% |
| `MimicMedicationDispense.ndjson` | 12,841,505,555 | 2% |
| remaining 25 files | ~53.9 GB | 10% |

**Two files are 80% of the artifact.** Same two resources that were 75% of the build time.
Any DuckDB port is really a bet about `observation_chartevents` and `observation_labevents`.

Note `scripts/psql_fhir_export.sh` opens with `rm -rf "$MIMIC_EXPORT_DIR"` — a re-run silently
discards the previous export rather than refusing.

---

## Job 30368919 — export verification (step 6)

`hpc/psql/export_manifest.sh`. Counts lines in all 30 files (8 in parallel) and compares each
against the expected row count for its source table. One JSON object per line and jsonb never
serialises a literal newline, so `wc -l` == row count exactly.

```
Elapsed 00:01:45   TotalCPU 03:17   493 GB read in 105 s (~4.7 GB/s aggregate across 8 readers)
```

**Result: 30 PASSED, 0 problems.** `total_lines=555,352,597  total_bytes=528,481,439,367`
Manifest: `$SCRATCH/psql-baseline/out/mimic4-export/manifest.tsv`

The script derives both the file→table mapping and the expected counts *from the SQL at
runtime* (asserting 30 of each) rather than hardcoding them, so it cannot silently drift if the
mappings change.

### Reconciling against the build's INSERT total

The build step reported 555,434,043 rows inserted; the export has 555,352,597 lines — a gap of
**81,446**. This is expected and benign: 13 SQL files under `sql/fhir_etl/` insert into
`fhir_etl` lookup and terminology tables (`uuid_namespace`, `map_gender`, `map_ethnicity`,
`map_lab_interpretation`, the concept maps, …). Those are ETL support tables, not FHIR
resources, so they are correctly absent from the export. All 30 resource tables match exactly.

---

## Job 30336753 — demo smoke test (step 0, context only)

100-patient demo on database `mimic4-demo`, all four stages end to end.

```
State  COMPLETED (0:0)   Node c248   Elapsed 00:01:24   TotalCPU 01:08.288   MaxRSS 4.3 GiB
import 12s   build 61s   validate 1s   export 4s
30/30 tables PASSED, 0 FAILED   30 ndjson files, 803 MB
```

Plumbing verification only. In particular the demo **cannot** exercise the birthDate fix
(#126): all 100 demo patients satisfy `year(MIN(transfers.intime)) == anchor_year`, so the
defect measures zero there. Do not quote demo numbers as evidence of the fix.

---

## Aside: source data on NFS vs Lustre (measured 2026-08-22)

The source CSVs live on `$DATASETS` (**NFS over TCP**), not on `$SCRATCH`
(**Lustre over InfiniBand**). Raw sequential read differs by 6–9×:

| | 2 GB O_DIRECT read |
|---|---|
| `/datasets` (NFS/TCP) | 90–125 MB/s |
| `$SCRATCH` (Lustre/IB) | 783–806 MB/s |

**But it makes no difference to `psql_import.sh`**, because the inputs are `.csv.gz` and
single-threaded `gzip` is the real bottleneck — it consumes compressed input at ~23 MB/s,
roughly 4× slower than even NFS delivers, so the pipe simply backpressures:

| | 1.5 GB compressed → 19.27 GB decompressed |
|---|---|
| `/datasets` | 65.04 s |
| `$SCRATCH` | 63.33 s (2.6% — noise) |

Cross-check against step 1: 7.2 GB of source at 23 MB/s ≈ 315 s of unavoidable decompression
against 1573 s of measured COPY time; the filesystem's share at NFS speed is ~82 s and is
hidden inside the gzip time rather than added to it. **Restaging to `$SCRATCH` would have
saved low single-digit percent. Not worth doing for the psql baseline.**

**This flips for the DuckDB comparison.** DuckDB reads files in parallel, so ~8 concurrent
gzip streams demand ~190 MB/s — above NFS's ceiling, well inside Lustre's. And converting to
Parquet or uncompressed CSV removes the gzip ceiling entirely, at which point the 6–9× storage
gap becomes the dominant term. **Stage to `$SCRATCH` before the DuckDB runs.** The existing
copy at `$SCRATCH/original-mimic-iv` holds only `mimic-iv-2.2`; the ED data is
NFS-only and needs copying across.

Caveats: measured on the login node (shared, so indicative only), and while job 30353020 was
doing heavy `CLUSTER` I/O on Lustre — so the Lustre figure is a floor, not a ceiling.

## How these numbers are captured

`hpc/psql/run_psql_step.sh` emits `step_elapsed_seconds` and `peak_cgroup_memory_bytes` into
the job's `.out` log, and writes a 60-second sample series to
`$SCRATCH/psql-baseline/metrics/<job>.samples.csv`.
`hpc/psql/collect_metrics.sh <jobid>` then combines `sacct` + `seff` + the sample series into
`$SCRATCH/psql-baseline/metrics/job-<jobid>.metrics.txt`.

`\timing on` is injected for every statement via `PSQLRC=hpc/psql/psqlrc`, so per-statement
costs land in the job log without editing any SQL in the repo.

## Run summary

| Step | Job | Wall | Peak mem | Mean cores |
|---|---|---|---|---|
| import | 30337394 | 00:40:01 | 73.8 GiB | 1.68 |
| build | 30339566 | 07:16:37 | 135.1 GiB | 1.19 |
| CLUSTER | 30353020 | 05:54:05 | 119.3 GiB | 0.87 |
| validate | 30362384 | 00:04:17 | 136.3 GiB | 2.56 |
| export | 30363082 | 00:57:36 | 113.6 GiB | 1.43 |
| **total** | | **14:52:36** | **136.3 GiB** | |

Plus verification (30368919) at 00:01:45. **Wall-clock end to end 14h 54m.**

Headline facts for the DuckDB comparison:

1. **Never more than ~2.6 cores busy on an 8-CPU allocation, and under 1.7 for every
   write-heavy step.** PostgreSQL generates no parallel plan for a data-modifying statement, so
   the build is structurally serial. This is the entire motivation for the port — and it means
   the 64-core node the coworker's notes assume was never being used.
2. **Memory is the binding constraint, not CPU.** 136.3 GiB peak, driven by
   `shared_buffers = 128GB`. Any allocation must exceed that or the postmaster will not start.
3. **`observation_chartevents` + `observation_labevents` are 75% of build time and 80% of
   output bytes.** Nothing else is worth optimising first.
4. **The build is the only non-resumable step**, and it fit in 7h17m — comfortable against a
   24 h cap. Splitting `CLUSTER` out was worthwhile insurance but turned out not to be needed.

## Job 30372926 — fix verification (step 7)

`hpc/psql/verify_fixes.sh` + `verify_fixes.sql`. 16 CPU / 192 G, **00:05:07**, 38m54s TotalCPU
(≈7.6 mean cores — pure SELECTs do get parallel plans, unlike the build steps). Checks the
three fixes at the value level against `mimic4`, joining published resources back to source
through the same `uuid_generate_v5` namespaces the ETL derives ids from.

All three deployed fixes are present in the checkout that produced this build (HEAD `e7c326b`),
so the 492.2 GiB export *is* the fixed output.

### Every documented figure reproduced exactly

| fix | check | result |
|---|---|---|
| #126 | `birthDate == make_date(anchor_year,1,1) - anchor_age years` | **0 wrong / 299,712**, 0 null, all on Jan 1 |
| #126 | age from published `birthDate` vs canonical `mimic-code` age, all 431,231 admissions | **0 wrong** calendar-year, **0 wrong** anniversary |
| #126 | same, under the *pre-fix* formula, recomputed side by side | **460** and **118,225** wrong — matches the issue |
| #126 | distinct patients affected pre-fix | **87** and **47,899** — matches the issue |
| #126 | worked example subject (not in the demo; id withheld) | anchor-pair birth date ✅ |
| #124 | source admissions in a NY DST discontinuity | **44** gap, **53** ambiguous — matches the issue |
| #124 | `Encounter.period` wall clock + offset vs `admissions` | **431,231 / 431,231** exact, all `+00:00` |
| #124 | `EncounterICU.period` vs `icustays` (second resource file) | **73,181 / 73,181** exact |
| #124 | the 44 in-gap admissions individually | all `02:xx`, none shifted |
| #125 | published component count vs source predicate, per itemid | **150 / 150 itemids agree**, 0 disagree |
| #125 | component well-formedness | 27,464,811 rows, all 1-element, right system, all carry text |
| #125 | `value[x]` unaffected | all 27,464,811 still carry `valueQuantity`, 0 lost a `valueString` |
| #125 | itemid 223900 at `valuenum 1` | `No Response-ETT` (573,198) and `No Response` (71,578) now separable |

The `#126` pre-fix column matters: without it, "0 wrong" is satisfiable by a check that measures
nothing. Recomputing `MIN(transfers.intime) - anchor_age` in the same query and getting exactly
the 460 / 118,225 / 87 / 47,899 the issue reported proves the harness is looking at the right
thing, from the source distribution, with no Delta or Spark in the path.

### The one confound, and how it was handled

This cluster carries `timezone = 'UTC'` in `postgresql.conf` too — a deliberate belt-and-braces
deviation (`hpc/psql/setup_db.sh:40`). So a correct UTC output here does **not** by itself prove
`SET TimeZone='UTC'` in `create_fhir_tables.sql` did the work. Check B4 isolates it: re-rendering
the same `CAST(... AS TIMESTAMPTZ)` under `America/New_York` shifts **44 of 44** in-gap values
(`02:05` → `03:05-04:00`) while UTC shifts **0 of 44**. The mechanism the fix relies on is real
and the fix engages it.

### Deviation found: the fix emits 5,295 more components than the issue counted

Published **27,464,811 across 150 itemids**; the issue measured **27,459,516 across 147**. The
published and source sides agree *exactly* (150/150 itemids, 0 disagreeing), so this is not a
build defect — the two predicates are different tests, and the commit message says so:

```
issue  chartevents_value_drop.py:79       ^\s*-?[0-9]*\.?[0-9]+\s*$    match => "just the number", drop
fix    fhir_observation_chartevents.sql   ^\s*-?\d+(\.\d+)?\s*$        match => cast, compare to valuenum
```

The fix's pattern is strictly narrower and it additionally requires equality, so it carries a
superset. Two causes: text like `.5` matches the issue's pattern but not the fix's, and text that
is numeric but *unequal* to `valuenum` (`'98'` beside `98.5`) is dropped by the issue's test and
carried by the fix. Both err toward carrying, which is the direction the commit argues for, and
the second is arguably better than what the issue asked for. Quantified by job `30373852`
(`verify_fixes_delta.sql`). **If the issue is updated, the headline number should read 27,464,811
/ 150, not 27,459,516 / 147.**

## What is still not verified

Row counts, line counts **and the three fixes** are now clean. What remains unchecked is
field-level content *outside* the three fixed areas — the other 27 resource types are verified
only by row count and by the FHIR validator's own pass, not against source values.

Also uncaptured: on-disk size of the FHIR tables
(`tests/test_mimic_fhir_storage_size.sql`, needs a running server) and the final `pgdata`
footprint, which was 933 GB mid-CLUSTER.

## Publishing the artifact

`$DATASETS` is **not** writable (root-owned). One level down is, both setgid
to group `1366151`:

```
$DATASETS/source            drwxrws---
$DATASETS/source/mimic-iv   drwxrwsr-x   (owner <another-user>)
```

The natural home is a sibling of the authors' published output, which already sits at
`source/mimic-iv/mimic-iv-on-fhir-1.0`:

```bash
D=$DATASETS/source/mimic-iv/mimic-iv-on-fhir-2.2.0-baseline-psql
mkdir -p "$D"                       # fresh name -- nothing existing can be touched
rsync -a --ignore-existing $SCRATCH/psql-baseline/out/mimic4-export/ "$D/"
```

Two considerations before doing so: 493 GB over NFS at ~100 MB/s is 1.5–3 h, and it is a large
footprint for a shared dataset area — `gzip -1` on ndjson typically gets 8–12×, so ~50 GB is
far more neighbourly at the cost of direct greppability. And `source/mimic-iv` is owned by
another user, so a heads-up is warranted before adding half a terabyte to their tree.
