# Building MIMIC-IV-on-FHIR with PostgreSQL on the HPC cluster

A reproducible recipe for the **traditional psql build** described in `README.md`, run on
CSIRO's HPC cluster. This is the baseline: the reference output that the mapping fixes are
applied to, and that any future DuckDB port (`PORT_NOTES_DUCKDB_SPARK.md`) must reproduce.

Derived from a colleague's `mimic-iv-fhir` harness (see `PSQL_REBUILD_CSIRO.md`), with the
environment re-verified from scratch on 2026-08-21 and the hardcoded paths parameterised.

> Nothing here has been executed yet. Every fact in "Verified environment" was probed;
> every runtime figure is an estimate and is labelled as such.

---

## 1. Verified environment

Probed on the HPC cluster, 2026-08-21. These are facts, not assumptions.

| Thing | Value |
|---|---|
| PostgreSQL | 15.2 at `$DATASETS/reference/apps/postgresql/15.2` |
| `uuid-ossp` | present — `lib64/uuid-ossp.so`, `share/extension/uuid-ossp.control`, matches `pg_config --pkglibdir` |
| `defq` partition | 64 CPU, 505,463 MB (~493 GB usable), 7-day limit |
| Compute node RAM | 502 GB total, ~477 GB available |
| `/dev/shm` | 252 GB (ample for `dynamic_shared_memory_type = posix`) |
| Node-local `/tmp` | **327 GB total, 310 GB free — too small for PGDATA** |
| `$SCRATCH` | 30 TB free, **no user quota** (`bquota`/`blimit` = 0) |
| `en_AU.UTF-8` locale | present on compute nodes, so the colleague's `lc_*` config works verbatim |
| cgroup | v2; `memory.current` + `cpu.stat` readable, **`memory.peak` not exposed** |
| Slurm account | `<account>` |

**Source data** is already staged and complete, at
`$DATASETS/source/mimic-iv/`:

```
mimic-iv-2.2/{hosp,icu}                    22 + 9 csv.gz   full
mimic-iv-ed-2.2/ed                         6 csv.gz        full ED
mimic-iv-clinical-database-demo-2.2        demo
mimic-iv-ed-demo-2.2/ed                    demo ED
mimic-iv-on-fhir-1.0                       published PhysioNet FHIR ndjson
mimic-iv-clinical-database-demo-on-fhir-2.1.0
```

Those four directory names are **exactly** what `scripts/psql_import.sh` and
`scripts/demo_psql_import.sh` expect, so `MIMIC4_DATA_DIR` points straight at that parent and
both the full and demo paths resolve with no symlinking. The two `*-on-fhir-*` directories are
the published output — a useful reference to diff against, though see the key-ordering caveat
in `PORT_NOTES_DUCKDB_SPARK.md` §6.

Note `$SCRATCH/original-mimic-iv/` also holds `mimic-iv-2.2` but **no ED data**; do not
point `MIMIC4_DATA_DIR` there.

### Local constraints

- **24 h wall clock, one full node, per job.** 7-day jobs exist but queue too long. Multiple
  sequential jobs are fine — the database persists on `$SCRATCH` between them.
- **PGDATA must live on `$SCRATCH`** (Lustre). Node-local `/tmp` is only 327 GB and is
  per-job anyway, so it cannot carry a database across job boundaries. Lustre is a poor fit
  for a Postgres cluster and is the main performance unknown here.

---

## 2. Layout

The harness is the colleague's repo; **this** repo is a submodule inside it.

```
$SCRATCH/psql-baseline/
├── mimic-iv-fhir/                    $ROOT — harness
│   ├── hpc/env/postgresql.conf       tuned config
│   ├── scripts/psql_*.sh             the build steps
│   └── mimic-fhir/                   THIS repo (must be your fork, with the fixes)
│       ├── sql/                      the 37 mapping files
│       ├── mimic-code/               submodule, required by psql_import.sh
│       └── hpc/psql/                 the scripts documented here
├── pgdata/                           PGDATA
├── out/                              $MIMIC4_OUT_DIR — ndjson + validation
└── metrics/                          per-job resource metrics
```

Steps, all driven off `$HOME/.mimicrc`:

| Step | Script | What it does |
|---|---|---|
| import | `scripts/psql_import.sh` | mimic-code buildmimic → `mimiciv_hosp/icu/ed` in db `mimic4` |
| index | `sql/create_mimic_index.sql` | optional; see §6 |
| build | `mimic-fhir/hpc/psql/psql_fhir_nocluster.sh` | the mappings — the long one |
| cluster | `mimic-fhir/hpc/psql/psql_cluster.sh` | the deferred `CLUSTER` pass |
| validate | `scripts/psql_validate.sh` | `validate_fhir_tables.sql`, greps for `FAILED` |
| export | `scripts/psql_fhir_export.sh` | `create_fhir_jsons.sql` → ndjson |

---

## 3. One-time setup (login node)

Compute nodes have no outbound network, so all cloning happens here.

**The HPC cluster has no GitHub SSH key** (`git@github.com` → `Permission denied (publickey)`), and
`aehrc/mimic-iv-fhir` is private, so it cannot be cloned on the HPC at all. Copy it up from a
machine that can read it instead — it is only ~600 KB:

```bash
ssh <hpc-login> 'mkdir -p $SCRATCH/psql-baseline/{out,metrics,logs}'
rsync -a --exclude .DS_Store <local>/mimic-iv-fhir/ \
    <hpc-login>:$SCRATCH/psql-baseline/mimic-iv-fhir/
```

`fhnaumann/mimic-fhir` *is* public, so it clones over HTTPS on the login node. Its own
submodules (`mimic-code`, `mimic-profiles`, `fhir-packages`) are all public HTTPS too.

```bash
cd $SCRATCH/psql-baseline/mimic-iv-fhir
# The harness's submodule points at aehrc/mimic-fhir branch _export_fixes, which does
# NOT have the mapping fixes. Replace it with your fork or you baseline the wrong code.
rm -rf mimic-fhir
git clone --recurse-submodules --shallow-submodules \
    https://github.com/fhnaumann/mimic-fhir.git mimic-fhir
( cd mimic-fhir && git log --oneline -3 )                 # expect e7c326b / 3048c88 / ade10fb
ls mimic-fhir/mimic-code/mimic-iv/buildmimic/postgres/    # must be populated
```

If `hpc/psql/` or this file are not yet committed to the fork, rsync them on top of the clone.

`.mimicrc` — `scripts/_set_env.sh` hard-requires five variables and `exit 1`s otherwise,
three of which the psql path never touches:

```bash
cat > $HOME/.mimicrc <<'EOF'
export MIMIC4_DATA_DIR=$DATASETS/source/mimic-iv
export MIMIC4_OUT_DIR=$SCRATCH/psql-baseline/out
# unused by the psql path, but _set_env.sh demands they be set
export FHIR_VALIDATOR_SPARK_DIR=$SCRATCH/psql-baseline/unused
export MIMIC4_PUBLISH_DIR=$SCRATCH/psql-baseline/unused
export MIMIC4_PUBLISH_VERSION=2.2.0-baseline
EOF
mkdir -p $SCRATCH/psql-baseline/{out,metrics,logs}
```

Create the cluster and prove `uuid-ossp` works — a two-minute check that otherwise fails
12 hours into a job at `create_fhir_tables.sql:15`:

```bash
export ROOT=$SCRATCH/psql-baseline/mimic-iv-fhir
bash $ROOT/mimic-fhir/hpc/psql/setup_db.sh
```

It asserts `uuid_generate_v5(uuid_ns_oid(),'MIMIC-IV') = 24ba6d92-ae8e-56f9-8898-873d8cba02da`
(RFC 4122 v5, computed independently) and aborts if not. That one value validates the whole
`sql/fhir_etl/uuid_namespace.sql` chain and all 194 call sites — and it is the same vector to
check a DuckDB `uuid_v5` macro against later.

---

## 4. Smoke-test on the demo data first

~10 minutes, and it exercises config, paths, locale, extension and every mapping file. Do not
skip it before spending a day of wall clock.

```bash
cd $SCRATCH/psql-baseline/mimic-iv-fhir/mimic-fhir/hpc/psql
mkdir -p logs
sbatch run_demo_smoke.sh
```

That runs import → build → validate → export against the demo data on database
`mimic4-demo`, so it does not touch the full `mimic4` build, and prints per-step elapsed
times plus a table-size summary.

It requests `--cpus-per-task=8 --mem=160G` rather than a full exclusive node so it schedules
immediately. **`--mem` must stay comfortably above `shared_buffers` (128 GB)** or the
postmaster cannot allocate its shared memory segment — that is the reason for the otherwise
odd-looking 160 G on a 100-patient dataset.

**Verified run — job `30336753`, 2026-08-21.** Clean end to end:

```
import    12s     build    61s     validate   1s     export    4s
elapsed 00:01:24  TotalCPU 01:08   MaxRSS 4.4G  ExitCode 0:0
validate_fhir_tables_demo.sql: 30/30 tables PASSED, 0 FAILED
export: 30 ndjson files, 803 MB (MimicPatient 100 lines, ObservationChartevents 668,862)
```

Two things worth noting from it. `MaxRSS` was 4.4 GB despite `shared_buffers = 128GB`, because
Postgres only faults in the buffers it actually touches — so the large `shared_buffers` costs
address space, not resident memory. And `TotalCPU / Elapsed` ≈ 0.8 against 8 allocated CPUs,
i.e. effectively single-threaded. That is the signature §7 is looking for, but at this scale it
is dominated by process startup and is **not** evidence; the full build is what settles it.

When grepping the logs for problems, match `^psql:.*(ERROR|FATAL)` — a bare `grep ERROR` hits
the literal `ON_ERROR_STOP=1` in the `set -x` trace of every harness script and reports false
positives.

> **The demo cannot validate the birthDate fix.** As `debug-patient-birthdate-anchor/README.md`
> documents, all 100 demo patients happen to satisfy
> `year(MIN(transfers.intime)) == anchor_year`, so the `#126` defect measures 0 there. Treat
> the demo as a plumbing test only, and do not quote demo numbers as evidence.

---

## 5. The full build

Five jobs. Submit **one at a time** and check each — only one postmaster can hold a PGDATA, so
overlapping jobs collide. Use `--dependency=afterok:<jobid>` to chain them if you prefer.

```bash
cd $SCRATCH/psql-baseline/mimic-iv-fhir/hpc  # or wherever you keep logs/
mkdir -p logs
W=$SCRATCH/psql-baseline/mimic-iv-fhir/mimic-fhir/hpc/psql/run_psql_step.sh

sbatch -J psql-import   $W scripts/psql_import.sh
sbatch -J psql-index    $W mimic-fhir/hpc/psql/psql_index.sh          # optional, see §6
sbatch -J psql-fhir     $W mimic-fhir/hpc/psql/psql_fhir_nocluster.sh # the long one
sbatch -J psql-cluster  $W mimic-fhir/hpc/psql/psql_cluster.sh
sbatch -J psql-validate $W scripts/psql_validate.sh
sbatch -J psql-export   $W scripts/psql_fhir_export.sh
```

`run_psql_step.sh` requests `--cpus-per-task=8 --mem=192G --time=24:00:00`, starts PostgreSQL,
runs the step, and stops PostgreSQL via an `EXIT` trap so a failure does not leave a stale
`postmaster.pid` blocking the next job. `--signal=B:TERM@900` gives a 15-minute grace period
on a wall-time kill for the same reason.

### Why 8 CPUs and not the whole node

PostgreSQL's documented rule: *if a query contains a data-modifying operation, either at the
top level or within a CTE, no parallel plan is generated for that query.* So every
`INSERT INTO mimic_fhir.x SELECT ...` in the build is **fully serial — both the insert and the
scan feeding it**. `CLUSTER` is single-process as well; only the index rebuild after it can use
`max_parallel_maintenance_workers`. Read-only steps (`validate`) do get parallel scans, but
they take minutes either way.

| Step | Cores it can actually use |
|---|---|
| build | 1–2 |
| cluster | 1 (plus parallel index rebuild) |
| import | ~8 — `COPY` is serial per table, index builds parallelise |
| validate | many, but irrelevant at minutes |
| export | 1, serial through one client connection |

**Memory, not CPU, sets the request.** `shared_buffers = 128GB` is the floor; those buffers do
fill over a 300 M-row build, so the cgroup sees close to 128 GB resident. 192 G covers that plus
`maintenance_work_mem` and the OS. Going below ~160 G requires lowering `shared_buffers` too.

Dropping `--exclusive` costs little: the dominant I/O is Lustre, which is shared cluster-wide
regardless of what you reserve, so exclusivity mainly protected local page cache. The payoff is
much faster scheduling — a 8-CPU/192 G job backfills where a full exclusive node waits.

Note that with 8 CPUs, the config's `max_parallel_workers_per_gather = 32` is oversubscribed.
It only affects the read-only steps, so it is low stakes, but setting it to 8 would be tidier.

**Rough expectations** — the `README.md` figure is ~12 h for the build on a 24-core/64 GB box.
Here you have 64 cores and 503 GB but PGDATA is on Lustre rather than local NVMe, and
`INSERT ... SELECT` does not parallelise on the insert side, so **do not assume it gets
faster**. Import is likely 2–5 h, export a few hours. These are guesses; the point of §7 is to
replace them with measurements.

### Why the build is split

`create_fhir_tables.sql` ends with `\i create_table_clusters.sql`, a full rewrite + sort of all
24 tables including the 313 M-row `observation_chartevents`. And because the script opens with
`DROP SCHEMA ... CASCADE`, **the build is not resumable** — a wall-time kill costs the whole
job. With a hard 24 h cap that is an unacceptable risk, so `psql_fhir_nocluster.sh` comments
out that one include and `psql_cluster.sh` runs it as its own job.

The output is identical. `CLUSTER` is a physical reorganisation; it does not change a single
row. If you would rather run it exactly as the authors wrote it, use
`scripts/psql_fhir.sh` instead and accept the risk.

---

## 6. Deviations from the colleague's setup, and why

Each of these is a deliberate change from `PSQL_REBUILD_CSIRO.md` / the harness as received.

| # | Deviation | Reason |
|---|---|---|
| 1 | Paths and account parameterised | Harness hardcodes `<another-account>`, `<another-user-scratch>/psql/data`, `$HOME/apps/postgresql/15.2`, `$HOME/dev/mimic-iv-fhir` |
| 2 | `PSQL_HOME` = the `/datasets` copy | Harness used a private copy in the colleague's home, which we cannot read |
| 3 | `timezone`/`log_timezone` → `UTC` | Config sets `Australia/Sydney`, exactly the DST hazard `#124` fixes. `create_fhir_tables.sql:8` already does `SET TimeZone='UTC'` per session, so this is defence in depth |
| 4 | `CLUSTER` split into its own job | Non-resumable build vs. hard 24 h cap — see §5 |
| 5 | `ON_ERROR_STOP=1` on the build | `scripts/psql_fhir.sh` omits it, so a failing mapping is skipped and the build "succeeds" with a table missing. Set `ERROR_STOP=0` to restore |
| 6 | `--cpus-per-task=8 --mem=192G` | Harness asked for `--cpus-per-task=8 --mem 384G`. 8 CPUs is kept (the build is serial anyway) but 384 G is more than `shared_buffers = 128GB` needs — 192 G schedules faster for the same behaviour |
| 7 | `pg_ctl -w` + `EXIT` trap | Harness neither waits for startup nor cleans up on failure |
| 8 | cgroup sampler added | `memory.peak` is not exposed and `sacct` MaxRSS misreports Postgres — see §7 |

**Deliberately kept as-is:** the whole of `hpc/env/postgresql.conf` (`shared_buffers = 128GB`,
`work_mem = 512MB`, `maintenance_work_mem = 4GB`, `max_parallel_workers = 32`,
`max_parallel_workers_per_gather = 32`, `max_wal_size = 16GB`, `effective_cache_size = 128GB`).
Those values are internally consistent and fit a 64-core/503 GB node. **No `fsync=off` /
`wal_level=minimal` tuning** — this is meant to be the traditional pipeline, not a tuned one.

**`sql/create_mimic_index.sql` is dead code.** Those 33 lines of indexes on the *source* MIMIC
tables are referenced by nothing — not `create_fhir_tables.sql`, not any harness script. They
look like they would materially help the join-heavy mappings. If you want to try them, add a
one-line `psql_index.sh` calling `psql -d mimic4 -f sql/create_mimic_index.sql`, and record it
as a variant rather than folding it into the baseline.

---

## 7. Capturing metrics

The point of the baseline is a number as much as an output.

```bash
bash mimic-fhir/hpc/psql/collect_metrics.sh <jobid> [<jobid> ...]
```

That wraps `sacct` and `seff` and writes `metrics/job-<id>.metrics.txt`. It reports:

- **Elapsed / TotalCPU / AllocCPUS** from `sacct`, and the derived **mean cores busy**
  (`TotalCPU / Elapsed`). Expect a value near 1–2 against the 8 allocated CPUs, confirming the
  pipeline is effectively serial. Note this is *predicted* by PostgreSQL's no-parallel-plan
  rule for data-modifying queries, not merely observed — so it is a sanity check on the setup
  rather than a discovery. (`PORT_NOTES_DUCKDB_SPARK.md` §2 attributes the idle cores to a
  serial insert side with a parallel scan; that mechanism is wrong, the whole statement is
  disqualified from parallelism. The conclusion stands.)
- **Peak and mean whole-job memory** from cgroup v2 samples.
- `seff` output as-is.

> **Why not `sacct` MaxRSS alone.** MaxRSS is the peak RSS of a *single* process. PostgreSQL
> maps its 128 GB `shared_buffers` into every backend, so MaxRSS counts those shared pages
> once per process while also missing the sum across parallel workers. It will be wrong in
> both directions, and `seff`'s memory-efficiency percentage inherits the error. Hence the
> sampler in `run_psql_step.sh`: it polls the job-level cgroup's `memory.current` and
> `cpu.stat` every 60 s (this kernel does not expose `memory.peak`) into
> `metrics/<job>.samples.csv`. Set `SAMPLE_INTERVAL=0` to disable.

**Per-resource timings, without touching the mapping SQL.** `run_psql_step.sh` sets
`PSQLRC=hpc/psql/psqlrc`, which is just `\timing on`. Combined with the `\echo` markers
already in `create_fhir_tables.sql`, the job log becomes a per-resource cost breakdown — which
is what tells you which mappings to put in a DuckDB spike first.

### Also record, for the build to be worth anything later

```bash
# correctness gate: row counts vs the expected values in validate_fhir_tables.sql
#   (313,645,032 observation_chartevents; 118,171,367 observation_labevents; ...)
cat $MIMIC4_OUT_DIR/mimic4-validation.txt

# psql-side storage
psql -d mimic4 -f mimic-fhir/tests/test_mimic_fhir_storage_size.sql

# ndjson manifest -- the comparison target for any future backend
cd $MIMIC4_OUT_DIR/mimic4-export
for f in *.ndjson; do printf '%s\t%s\t%s\n' "$f" "$(wc -l < "$f")" "$(stat -c%s "$f")"; done \
  > manifest.tsv
```

Line counts and byte sizes, **not checksums**: Postgres `jsonb` sorts object keys by
length-then-bytes while DuckDB preserves insertion order, so identical FHIR will hash
differently (`PORT_NOTES_DUCKDB_SPARK.md` §6). Counts and sizes are the portable signal; the
semantic diff comes later.

Finally, record the allocation itself (`scontrol show job <id>`, `sinfo -p defq`) so the
elapsed figure means something when it is compared to anything.

---

## 8. Known risks

1. **Lustre PGDATA.** The largest unknown. Postgres on a parallel FS can be much slower than
   on local disk, and node-local `/tmp` (327 GB) is too small to be an alternative. If the
   build badly overruns, this is the first thing to suspect.
2. **Non-resumable build.** `DROP SCHEMA` at the top means any failure restarts from zero.
   Hence the demo smoke test and the CLUSTER split.
3. **Peak disk is worse than total.** `README.md` says ~2 TB; the `CLUSTER` pass transiently
   needs free space equal to the largest table *on top of* that. 30 TB free covers it, but do
   not size to 2 TB.
4. **`scripts/*.sh` use `source` under a `#!/bin/sh` shebang.** Fine where `/bin/sh` is bash,
   which it is here; `run_psql_step.sh` invokes them with `bash` explicitly regardless.
5. **`MIMIC4_DATA_DIR` points into a writable shared directory** owned by another user.
   Treat it as read-only; keep all output under `$SCRATCH`.
