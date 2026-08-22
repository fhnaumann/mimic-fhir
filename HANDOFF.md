# Handoff — DuckDB port of the MIMIC-IV-on-FHIR mappings

**Date:** 2026-08-22 · **Branch:** `main` @ `e7c326b` · **Nothing committed** — all new work is
untracked, see §8.

**Phase 1 is done and its gate passed.** The whole pipeline exists, runs on the HPC, and has
been compared against the psql baseline on the 100-patient demo:

> **28 of 30 resources are byte-for-byte semantically identical to the psql export.** The other
> two differ only in the ORDER of `Observation.hasMember` array elements — same members, 39
> arrays across MicroOrg and MicroTest, from a `json_agg` with no `ORDER BY` in the source SQL.
> No id is missing on either side, and every line parses.

**Phase 2 is under way and the full dataset is staged.** Job `30368991` wrote all 30 tables —
622,858,060 rows, 4.98 GB of Parquet — in 2 m 22 s with every assertion passing, including the
sub-second timestamp check that was expected to be the one to fail. Job `30369161` then loaded
all 97 SQL files and bound them against that full-scale data, and the selftest passed; its
throughput measurement was lost to a Slurm `--export` quirk (trap 25) and is being redone by
`30374038`. The psql baseline is **confirmed complete** at 30 files / 516 GB. §6 has the numbers
and what J2 should be sized from. **Do not poll running jobs.**

Read in this order:

1. This file.
2. `DUCKDB_PORT_PLAN.md` — the plan. §1 is a corrections list against the older notes; the
   rules table in §5 is your spec for the transpiler.
3. `PORT_NOTES_DUCKDB_SPARK.md` — the original feasibility notes. **Historical.** Six of its
   findings are wrong or incomplete; `DUCKDB_PORT_PLAN.md` §1 says which and why. Do not work
   from the notes directly.

---

## 0. The goal, restated by the user (2026-08-22)

**The point of this port is build TIME.** The psql pipeline takes **14 h 52 m** and underuses the
node. DuckDB should use a full node as well as it reasonably can. The user asked for *"a general
(simple) improvement"*, explicitly **not** exhaustive optimisation.

> The user and this document both said "~17 hours" for a while. The measured total in
> `PSQL_RUN_METRICS.md` is **14:52:36** across the five build steps (14 h 54 m including export
> verification). Use the measured figure. All benchmark numbers now live in **`BENCHMARK.md`** —
> that is the thesis-citable artifact; do not restate timings anywhere else.

**Parity has the opposite priority.** It runs once, to prove the DuckDB build equals the psql
build. It may take any amount of time **under 24 hours** and need not use the node. Once proven,
it never runs again. So: simplest thing that fits in 24 h single-threaded, and do not spend
effort parallelising it.

This reverses the earlier sizing instinct. "Only request what you need" still holds for probes
and staging, but the headline build is *supposed* to occupy a node — that is the result being
measured.

---

## 1. Ground rules from the user

- **Jobs may now be submitted without asking** (the user lifted the earlier rule), with two
  conditions that still hold: **no polling**, and **request only what the step needs**. Chain
  with `--dependency=afterok:` rather than watching a queue. Record the exact `sbatch` line and
  what it measures — §6.
- **Do not request a full node "for the sake of it."** Size each job to what it needs.
- **gzip stays the default for now.** zstd is a separate third run, after a passing gzip run, so
  the benchmark delta is clean. Do not make compression a per-resource choice.
- The psql baseline (`PSQL_BUILD_HPC.md`, `PSQL_RUN_COMMANDS.md`) is **running on the HPC and is
  the user's to manage.** Do not touch it, do not poll it, do not submit anything into it.

The user pushed back once already on a scope confusion worth not repeating: the three jobs in
`DUCKDB_PORT_PLAN.md` §10 (J1/J2/J3) are the **full-scale production runs**, not tests. Phases
0–2 are tests and they need **zero jobs**. A `J0` scale probe was agreed as the first real
submission — see §6.

---

## 2. Phase 0 — done, and it changed the design

### Q: does the HAPI validation path read Postgres directly, or only ndjson?

**Directly.** This was the open question from the notes and the answer means a shim is needed.

- `py_mimic_fhir/db.py` → `MFDatabaseConnection.get_resources_by_pat()` issues
  `SELECT fhir FROM mimic_fhir.{table} WHERE patient_id = '...'`; `get_n_patient_id()` issues
  `SELECT * FROM mimic_fhir.patient [LIMIT n]`; `get_pat_id_with_links()` `INNER JOIN`s several
  `mimic_fhir.*` tables on `patient_id`.
- `py_mimic_fhir/terminology.py` → `get_table('fhir_trm', 'cs_*' / 'vs_*')`, i.e.
  `SELECT * FROM fhir_trm.{table}`.

**Consequences, already reflected in the design below:**

1. `fhir_trm.*` must stay **real tables**, not files. (Already the plan.)
2. `mimic_fhir.*` must be materialisable as tables with `(id, patient_id, fhir)`.
3. But validation only ever samples patients (`LIMIT n`, `LIMIT 20`), so **materialise
   `mimic_fhir.*` only for a subject subset**, never for the full 313 M rows. This keeps the
   fast path streaming.
4. The shim itself is small: DuckDB is DBAPI-compliant and `pd.read_sql_query` works against it,
   so it is one `db_mode == 'DUCKDB'` branch in `connect_db()`. **WRITTEN — see below.**

#### The DUCKDB shim — DONE, `py_mimic_fhir/db.py`

`--db_mode DUCKDB`, with the **database file path passed as `--dbname_mimic`**; user, password,
host and port are ignored. `db_mode` had no `choices`, so this is additive.

Three things it has to get right, none of them obvious:

- **`fhir` arrives as `str`, not `dict`.** psycopg2 decodes a `jsonb` column for free and every
  caller indexes it (`fhir['id']`). DuckDB's JSON type comes back as text, exactly like
  BigQuery's — which is why that branch already had a `json.loads`. Without it, workers raise
  `TypeError: string indices must be integers` *inside* `validate.py`'s pool, where it is caught
  broadly and logged.
- **`read_only=True`**, mirroring POSTGRES's `set_session(readonly=True)`. It is also what lets
  the worker processes open the same file at once — DuckDB allows many readers, one writer.
- **Fork safety** — trap 28. This is the one that would have looked like a flaky validator.

Also made `psycopg2`, `pandas_gbq` and `google.auth` **lazy imports**. They were module-level, so
a DUCKDB run could not start without the Postgres client library and the whole GCP auth stack
installed, none of which it touches.

Tests: `duckdb/tests/test_db_shim.py`, 11 checks against a real database file, including a real
`os.fork()` and a real `multiprocessing.Pool`. Both fork checks are **timeouts, not exceptions**,
because the failure mode is a hang. Fault-injected (hook removed) → exactly those 2 fail.

### Q: is semantic parity enough, or is byte-identical parity required?

Proceeding on **semantic parity**, stated as an assumption in `DUCKDB_PORT_PLAN.md` §2. The user
has not contradicted it. Byte-identical would need a key canonicaliser because Postgres `jsonb`
sorts object keys length-then-bytes and DuckDB preserves insertion order. If anyone downstream
textually diffs against published PhysioNet ndjson, revisit.

---

## 3. What exists on disk

All under `duckdb/`. All verified locally against **DuckDB v1.5.5** and **PostgreSQL 18.4**.

```
duckdb/runtime/00_settings.sql     ${THREADS} / ${MEMORY_LIMIT} / ${TEMP_DIR} placeholders,
                                   substituted by driver.py. NOT runnable as-is.
duckdb/runtime/10_macros.sql       uuid_v5, ns_oid, strip, fhir_instant, fhir_decimal,
                                   list_denull
duckdb/runtime/20_functions.sql    the 4 ported fn_* macros
duckdb/runtime/99_selftest.sql     14 assertions — ALL PASSING
duckdb/transpile.py                the rewriter — DONE, all 10 rules + 3 new ones
duckdb/parity.py                   semantic ndjson differ — DONE
duckdb/stage.py                    csv.gz -> Parquet, explicit types + assertions — DONE
duckdb/driver.py                   load order, settings, ndjson export, metrics — DONE
duckdb/tests/test_functions.py     differential test vs real plpgsql in Postgres
duckdb/tests/golden_functions.json 36 recorded Postgres results — ALL 36 MATCHING
duckdb/tests/test_parity.py        parity.py tests — 25/25, needs no data
hpc/duckdb/run_duckdb_step.sh      SLURM wrapper; sampler copied verbatim from psql
```

### The whole pipeline, end to end

```bash
python duckdb/transpile.py                      # sql/ -> build/duckdb/ + manifest.json
python duckdb/stage.py --mimic-root DIR [--mimic-root DIR] --out PARQUET --jobs 8
python duckdb/driver.py --parquet PARQUET --out NDJSON --compression gzip
python duckdb/parity.py PSQL_NDJSON NDJSON --top 6 --json report.json
```

Run each from OUTSIDE the repo root, or `import duckdb` finds this repo's `duckdb/`
namespace package instead of the PyPI module (trap 16). All four are wired into
`hpc/duckdb/run_duckdb_step.sh`.

### Verification status

```
99_selftest.sql            14/14 assertions pass
test_functions.py          36/36 cases match Postgres  (29 non-null, 7 NULL-collapse)
transpile.py               94/94 files transpiled, guard rails clean
transpile.py --explain     everything binds except 32 un-staged MIMIC source tables
test_parity.py             25/25 checks pass
stage.py (demo)            30/30 tables, 1,226,256 rows, all assertions pass
driver.py (demo)           30 resources, 928,935 rows, 0 unparseable lines
parity.py (demo vs psql)   28/30 IDENTICAL; 2 differ only by hasMember array order
```

The demo numbers are reproduced identically on this laptop and on the HPC login node.

Run the transpiler:

```bash
python duckdb/transpile.py              # transpile + guard rails (needs sqlglot, duckdb)
python duckdb/transpile.py --explain    # ... and bind every statement
python duckdb/transpile.py --only fhir_patient.sql
```

Reproduce the selftest (needs `duckdb` on PATH):

```bash
{ echo "SET TimeZone='UTC';"; echo "CREATE SCHEMA IF NOT EXISTS fhir_etl;"
  cat duckdb/runtime/10_macros.sql
  cat sql/fhir_etl/map_ethnicity.sql; echo ";"
  cat sql/fhir_etl/map_race_omb.sql;  echo ";"
  cat duckdb/runtime/20_functions.sql
  cat duckdb/runtime/99_selftest.sql; } | duckdb -init /dev/null
```

Reproduce the differential test (needs a local Postgres only for `--record`):

```bash
python3 duckdb/tests/test_functions.py                      # asserts vs the golden file
python3 duckdb/tests/test_functions.py --record --pg "..."  # regenerates it from Postgres
python3 duckdb/tests/test_parity.py                         # parity.py, needs nothing
```

### LOAD ORDER — this bites immediately if you get it wrong

`fn_patient_extension` resolves `fhir_etl.map_ethnicity` and `fhir_etl.map_race_omb` **at CREATE
time**, not at call time. So:

```
00_settings  →  CREATE SCHEMA fhir_etl, fhir_trm  →  10_macros
             →  transpiled fhir_etl/*.sql  →  20_functions  →  99_selftest
```

Loading `20_functions.sql` before the lookup tables gives
`Catalog Error: Table with name "fhir_etl.map_ethnicity" does not exist`. Encode this as a
`LOAD_ORDER` constant in `driver.py`.

---

## 4. What to build next, in order

### 4.1 `duckdb/transpile.py` — DONE

94 files → `build/duckdb/`, guard rails clean. Measured rule counts:

| rule | count | rule | count |
|---|---|---|---|
| 1 `json_object` | 409 | 6 unqualify `fn_` | 29 |
| 1b `strip()` wrap | 409 | 7 `jsonb_agg` | 4 |
| 2 `json_array` | 166 | 8 `list_denull` | 1 |
| 3 drop `strip_nulls` | 35 | 8b `json_each` pivot | 4 |
| 4 `uuid_v5` / `ns_oid` | 173 / 31 | 8c `regexp_extract` | 2 |
| 5 `fhir_instant` | 44 | 9 views (from 34 INSERTs) | 30 |
| 5b `fhir_decimal` | 12 | 10 drops | 35 |

**Five things found while writing it that were not in the plan.** Four of them are silent
data-loss bugs — they produce valid SQL that runs and returns wrong results.

1. **`WITH` lives on the `Insert` node, not the `Select`.** `WITH x AS (…) INSERT INTO t
   SELECT … FROM x` parses with the CTEs in `insert.args["with_"]`. Rendering
   `insert.expression` alone drops every CTE and emits a view referencing tables that do not
   exist — and *silently*, because a view body is not resolved until first use. Nearly every
   mapping file has this shape. `insert_body()` reattaches them.

2. **Four tables are populated by two `INSERT`s each** — `procedure_ed`,
   `medication_request`, `observation_ed`, `observation_vital_signs`. Postgres *appends*;
   `CREATE OR REPLACE VIEW` *replaces*. One view per INSERT silently halves four resources,
   including both large ED observations. They now emit `v_X__part1` / `__part2` + a
   `UNION ALL` view. 34 INSERTs → 30 views.

3. **sqlglot's own `ARRAY_REMOVE` → DuckDB translation is wrong.** It emits
   `list_filter(a, _u -> _u <> NULL)`, and `x <> NULL` is NULL for every `x`, so the filter
   keeps **nothing**. Measured: `['x',NULL,'y']` → `[]`. The one call site is
   `Encounter.location`, so every ICU encounter would lose its location array without an
   error. Rule 8 routes around it to a new `list_denull` macro; `99_selftest.sql` now pins it.

4. **New rule 8c — `regexp_match` no-match semantics differ.** Postgres `regexp_match`
   returns NULL when the pattern does not match; DuckDB `regexp_extract` returns the **empty
   string**. Both sites feed a numeric cast, so a non-matching `fill_quantity` would hit
   `CAST('' AS DOUBLE)` and abort the build. Emits `nullif(regexp_extract(…), '')`.
   Also: **sqlglot normalises Postgres's 1-based subscript to 0-based** — source `[1]` is
   stored as `Literal(0)` — and DuckDB's capture groups are 1-based like Postgres, so the
   rule converts back. Passing the stored value through would shift every group by one.

5. **Rule 6 is structural, not a rename.** sqlglot builds
   `Dot(Identifier(fhir_etl), Anonymous(fn_x))` — it does not fold the schema into the
   function name — so the obvious in-place rename never fires and the guard rail catches it.

**Guard rails, all running by default:**

- Every file parses; the 12 whitelisted non-translated files carry their reason in `DROPPED`.
- Forbidden-token scan over generated SQL (`jsonb`, `TIMESTAMPTZ`, `uuid_generate_v5`,
  `ARRAY_REMOVE`, `<> NULL`, `mimic_fhir.`, …), comments stripped first.
- **Syntax check via DuckDB's own parser** — `json_serialize_sql()` parses without binding, so
  it needs no tables and runs on the login node. It only accepts SELECT, so view bodies are
  checked with the `CREATE VIEW` preamble stripped and DDL goes through `EXPLAIN`.
  Fault-injected to confirm it fires.
- Manifest invariants: one view per ndjson output, `include_order` ≡ files written.
- `--explain` additionally *binds* (executes DDL first, then EXPLAINs the rest). Currently
  everything binds except the 32 un-staged MIMIC source tables. **Re-run it after `stage.py`
  for full coverage** — that is when it will catch unknown columns and macro arity errors.

`build/duckdb/manifest.json` is what `driver.py` should consume: `include_order` (94 files,
psql's `\i` order), `views` (table → view), `output_files` (view → ndjson filename, parsed
from `create_fhir_jsons.sql`), and `dropped` with reasons.

### 4.1b The old transpiler notes, kept because they still explain the design

sqlglot rewriter, `sql/**.sql` → `build/duckdb/**.sql`. `build/` is already gitignored.
Spec is the rules table in `DUCKDB_PORT_PLAN.md` §5. Four things that will cost you a day if
nobody tells you:

**(a) `Expression.transform()` prunes replaced subtrees.** The obvious implementation silently
does nothing. Returning a newly constructed node from the callback stops sqlglot descending into
it, so every nested rewrite is dropped. **Rename in place** — `node.set("this", "json_object")`
on the `exp.Anonymous` — and do structural replacements (`jsonb_strip_nulls(x)` → `x`) as a
separate innermost-first pass with `node.replace(...)`. Verified working on the real
`sql/fhir_patient.sql`: 15 `jsonb_*` → 0 remaining, 10 `json_object`, `uuid_v5` in place.

**(b) Wrap EVERY `json_object` in `strip()`, not just the 116 array-element positions.** The plan
(§5 rule 1b) describes minimal placement; strip-everywhere is *provably* equivalent and far
simpler. Postgres `jsonb_strip_nulls` removes null-valued keys from every object at any depth,
including inside arrays; stripping every object individually removes exactly the same set.
Neither removes `null` *elements* of arrays. The notes measured strip-all as marginally
**faster**, so there is no performance argument for the harder rule. One safety note: only ever
wrap an actual `json_object` node — never a `CASE` or a `NULL` — or a `NULL` branch would turn
into `{}` and stop being stripped by the caller.

**(c) Revised rule 9 — emit views, not `COPY`.** The plan says
`INSERT INTO mimic_fhir.X SELECT …` → `COPY (…) TO 'X.ndjson'`. **Do this instead:**

```sql
CREATE OR REPLACE TEMP VIEW v_X AS SELECT …
```

and let `driver.py` decide the I/O — `COPY (SELECT fhir::VARCHAR FROM v_X) TO …` for ndjson, or
`CREATE TABLE mimic_fhir.X AS SELECT * FROM v_X` for the Phase-0 validation shim on a subject
subset. Baking `COPY` into generated SQL would hardcode the compression choice into the
transpiler output, which fights the gzip-now/zstd-later benchmark plan. Select-list aliases are
already `id`, `patient_id`, `fhir`, so the views come out correctly named.

**(d) Resource → filename mapping: parse it, do not hardcode it.** `create_fhir_jsons.sql` holds
the only mapping from table to output filename, and it is not mechanical —
`mimic_fhir.observation_vital_signs` → `MimicObservationVitalSignsED.ndjson`. Extract it from
the `\set outputfile` / `\copy` pairs so there is one source of truth.

**Guard rails the transpiler must enforce** (this is what catches a silently no-op rule):

1. Every file parses; abort on any failure except the two known-dropped files below.
2. Generated output contains no `jsonb`, no `fhir_etl.fn_`, no `uuid_generate_v5`, no
   `TIMESTAMPTZ`, no `CLUSTER`.
3. `EXPLAIN` every generated statement against an empty DuckDB with the schema attached, before
   running any of them.

**The two files that will not parse, and should not:** `create_fhir_jsons.sql` (pure psql
meta-commands) and `create_table_clusters.sql` (`CLUSTER`). Both are deleted by the port.
Whitelist them; do not "fix" them.

**The rule 5 check is now finished — RESOLVED, rule 5 is safe.** Done with the AST, not grep
(the grep was giving false negatives). There are **44** sites, not 45:

- 42 are `CAST(col AS TIMESTAMPTZ) AS alias` in a select list.
- 2 are direct `jsonb_build_object` arguments (`fhir_encounter.sql`, the `period` object).
- Of the aliases, the only ones reused in a comparison are `IS NOT NULL` tests — which
  `fhir_instant` preserves, since it returns NULL on NULL — plus **exactly one real
  comparison**: `ph_STARTTIME <= ph_STOPTIME` in `fhir_medication_request.sql:172`, guarding
  `dispenseRequest.validityPeriod`.

That one site is **safe**: `'%Y-%m-%dT%H:%M:%S+00:00'` is fixed-width, zero-padded, and every
value carries the same `+00:00` suffix, so lexicographic order is chronological order. It holds
for 4-digit years, which MIMIC's date-shifted 2100–2200 range satisfies. It is recorded in
`REVIEWED_INSTANT_COMPARISONS` in `transpile.py`, so **a new comparison site will be reported as
an unreviewed diagnostic** rather than passing silently. The diagnostic checks both the
immediate context of the cast and downstream reuse of the alias it produced.

### 4.2 `duckdb/parity.py` — DONE

Semantic ndjson differ, 25/25 tests passing (`duckdb/tests/test_parity.py`, no Postgres, no
DuckDB, no data — runs anywhere).

```bash
python3 duckdb/parity.py $SCRATCH/psql-baseline/out build/ndjson
python3 duckdb/parity.py A_DIR B_DIR --only MimicPatient -v --json report.json
python3 duckdb/parity.py A.ndjson B.ndjson.gz          # single file pair
```

Exit 0 identical / 1 differences / 2 could not run. Reads `.gz` on either side transparently,
which matters because the psql baseline export is uncompressed and the DuckDB run is gzip.

**Normalises exactly three things:** object key order (Postgres `jsonb` sorts length-then-bytes,
DuckDB preserves insertion order — every line differs in bytes), numeric formatting (`5.20` vs
`5.2`, `7` vs `7.0`, via `decimal.Decimal` on *both* `parse_float` and `parse_int`), and line
order (matched by `id`, not position). Nothing else.

The report is a **JSON-path histogram** with array indices collapsed to `[*]`, so a systematic
rewriter bug is one row. Diff kinds are distinguished, not lumped: `only_a`/`only_b` (strip()
placement), `value`, `type` (the `json_each` pivot leaking a number as a string), `array_length`,
`array_order` (aggregate missing an `ORDER BY` — flagged separately rather than accepted, since
FHIR array order is meaningful in `name`/`telecom`), and `array_elem_only_a/b`.

Four things worth knowing:

1. **DECIMAL(18,3) truncation is a numeric difference**, so it is precisely what a differ that
   compares numbers loosely would eat. `0.000001` vs `0.000` has a dedicated test, and
   fault-injecting a looser numeric comparison fails it. Same for `0.1+0.2 != 0.3`.
2. **A reordered array does not cascade.** Same length + equal multiset reports one
   `array_order`, not N changed elements. Also fault-injected.
3. **`valueString` numeric formatting is a real diff, not noise** — this is the §9.1 `acuity`/
   `pain` hazard, and it has a test asserting `"2"` vs `"2.0"` is reported.
4. `--ignore '$.path'` suppresses a generic path but **counts and prints what it suppressed**,
   so a known-benign diff cannot quietly become a blanket.

**Scale limit, measured:** side A is held as raw lines (not parsed objects), side B is streamed.
Peak RSS ≈ **3× the A file**, throughput ≈ **75k resources/s** (300k rows / 131 MB in 4.0 s at
380 MB RSS, worst case with every line byte-different). Fine for the 803 MB demo export, which is
the whole Phase 1/2 gate. **Not** fine for a full-scale `observation_labevents` — use `--only` on
a machine with headroom, or compare a subject subset. Deliberately not solved: an external sort is
real machinery for a problem the gate does not have.

### 4.3 `duckdb/stage.py` — DONE

30 source tables (not 31 — the plan's census counted `mimiciv_hosp.diagnosis_icd` from a
**comment**; `stage.py` re-derives the list from the AST). Types come from mimic-code's
`create.sql`, never inferred. Four Postgres load-time behaviours have no DuckDB equivalent and
all four are silent — traps 19, 20, 21 and the sub-second one below. Each is **asserted**, not
trusted.

Two reviewed-exception dicts, `PIVOT_NUMERIC` and `CHAR_REVIEW`, are checked for **exact**
coverage against the DDL and the mapping SQL, so a new pivot key or a new `CHAR` column fails
the build until someone decides what it should do.

**The sub-second assertion is the one that licenses `fhir_instant`.** Postgres `TIMESTAMP(0)`
*rounds* .7 up; `strftime('%S')` truncates it down. Bare `TIMESTAMP` keeps the fraction and
renders it into the JSON, where `fhir_instant` drops it. They agree only if the source has no
sub-second component at all. It is scoped to columns `sql/` actually reads — MIMIC has
sub-second values in columns nobody reads (`procedureevents.storetime`, 242 rows in the demo
alone), and a gate that fails on those gets switched off. Fault-injected to confirm it fires
when such a column *is* read.

`--jobs N` stages tables concurrently. That is where all the parallelism is: a `.csv.gz` is not
seekable, so DuckDB reads each file on one thread no matter how many are available, and the run
cannot finish before its largest table (chartevents, 2.5 GB gzip) does. Biggest file first, or
labevents ends up running alone at the end.

### 4.4 `duckdb/driver.py` — DONE

Owns the load order, the `${...}` substitution, I/O policy, and per-resource metrics.

- **Load order** is psql's `\i` order from the manifest, unreordered, with `20_functions.sql`
  inserted after the `fhir_etl` lookups (see the LOAD ORDER note in §3). It is not reordered
  because DuckDB binds a view body at `CREATE` time, so the codesystem tables must exist first.
- **Export** uses a TAB quote, not psql's `\b` — see trap 18, and read `export()` before
  changing it. Every file is read back with `read_ndjson_objects`; a framing bug is a build
  failure, not something parity.py discovers later.
- **`--only patient,specimen`** exports a subset but still LOADS all 94 files, which is the
  full-data binding check `transpile.py --explain` cannot do without staged tables. That is what
  the scale probe uses.
- **`--materialize N --db path`** creates `mimic_fhir.*` tables for N patients, for the HAPI
  validation path (§2). Capped deliberately; validation only ever samples.
- Per-resource rows / seconds / bytes go to `--metrics` as JSON. Log them from the first run.

Driver owns `LOAD_ORDER`, the `${...}` substitution in `00_settings.sql`, the include order
parsed from the `\i` directives, per-resource I/O policy, and **per-resource write time and byte
counts** — log those from the start, they are what make the zstd run a 30-row table instead of
two wall-clock numbers and they cannot be reconstructed later.

`stage.py` converts MIMIC csv.gz → Parquet with **explicit types derived from
`mimic-code/mimic-iv/buildmimic/postgres/create.sql`** — the same file `psql_import.sh` uses, so
both backends load identical types by construction. Never infer types. 31 source tables are
referenced; the list is in `DUCKDB_PORT_PLAN.md` §8. Assert row counts against the CSVs, and
assert **no timestamp column has a non-zero sub-second component** — that is what licenses
`fhir_instant` dropping fractional seconds.

---

### 4.6 Parallel compression — the build optimisation — DONE

**Problem.** One `COPY … TO 'x.ndjson.gz'` runs one compressor thread. Measured at 1.0–2.0 cores
of 16. No allocation fixes it.

**Fix, and it is one COPY option.** `PER_THREAD_OUTPUT true` gives every thread its own file and
so its own compressor. That would normally change the output layout to a directory of parts,
rippling into parity, HAPI, and anything else that consumes the export — **but gzip is a
multi-member format**, so concatenating the parts byte-wise yields one valid `.gz` that
decompresses to the concatenation. Verified both ways: Python's `gzip` and DuckDB's
`read_ndjson_objects` each read a concatenated file completely, with all ids distinct.

So the output layout is **unchanged** — one `.ndjson.gz` per resource, same as psql — and the
compression spreads across all threads. The join is a byte copy at disk speed.

```python
con.execute(f"COPY (SELECT fhir FROM {view}) TO '{part_dir}' "
            f"({opts}, PER_THREAD_OUTPUT true)")
with open(dest, "wb") as out:
    for part in sorted(part_dir.iterdir()):
        shutil.copyfileobj(open(part, "rb"), out, 8 << 20)
```

Verified semantically neutral: a full demo build through the parallel path gives **the same
28/30 identical** against the psql demo export, with the same two `hasMember` differences.

`--single-stream` restores the old behaviour for a like-for-like benchmark. The parallel path is
**restricted to gzip** — zstd frames concatenate too, but that is a separate benchmark run and is
not being claimed here.

**Not done, and deliberately:** exporting several resources concurrently. It would help the 28
small resources but not the two that dominate, and the user asked for a simple improvement.

### 4.7 `parity.py` at full scale — DONE

The in-memory comparison holds every raw line of **both** sides, ~2.5× the uncompressed pair.
Fine for 28 resources; impossible for `MimicObservationChartevents` (262 GB, 313 M lines → ~600 GB
against a 505 GB node).

Above `--digest-threshold-gb` (default 8) it switches to **two passes**:

1. Keep a **16-byte digest of each canonicalised record** instead of the record (~75 GB for
   chartevents), stream side B against it, collect which ids differ.
2. Re-read both files keeping **only the differing ids**, and run the same `diff()` to build the
   histogram. Cheap when the answer is "identical", bounded by `--max-detail` when it is not.

Digests are over `canon()`, not the raw line — **zero raw lines match byte-for-byte** between the
two exports (psql writes `{"id": …` with spaces and sorted keys; DuckDB writes compact
insertion-order JSON), so a raw-line digest would report every record as differing.

**16 bytes, not 8:** at 313 M records an 8-byte digest has a ~0.3 % chance of one collision in the
run, and a collision here is a **false pass**.

Single-threaded and slow on purpose — ~7 h for the whole export at the measured 43 k rows/s for
`canon()`, which is inside the 24 h budget (§0). A DuckDB Python UDF was considered and rejected:
UDFs hold the GIL, so it would be no faster and a much larger rewrite of tested code.

**How the two strategies are kept from drifting:** every case in `test_parity.py` now runs through
**both** and asserts they agree on 10 result fields. Fault-injected (digest over the raw line
instead of `canon()`) → 5 failures, exactly the normalisation cases. And forcing
`--digest-threshold-gb 0` over the real demo export reproduces the same 28/30.

### 4.5 Phase 1 gate — PASSED

`parity.py $SCRATCH/psql-baseline/out/mimic4-demo-export
$SCRATCH/duckdb-port/fhir-demo`

```
28/30 resources IDENTICAL
differing: MimicObservationMicroOrg   2 of   338   array_order $.hasMember
           MimicObservationMicroTest 37 of 1,893   array_order $.hasMember
```

Nothing else. No ids only-A or only-B on any resource, no unparseable lines, no missing files,
no value/type/length differences anywhere. The three resources the gate actually required —
`patient`, `specimen`, `observation_labevents` — are all identical, as are the two large ED
observation resources that the multi-part `UNION ALL` view fix exists for (§4.1 note 2).

**Three build-stopping bugs surfaced during this run and none of them was visible before real
data existed** — traps 22 (`fn_patient_extension`'s correlated subquery vs `LEFT JOIN`), 23
(`json_agg` rendering as `JSON_ARRAYAGG`), and 18 (the `#` quoting, which produced a file that
*ran* fine and was 12% corrupt). That is the argument for the scale probe in §6: the same class
of bug can still be hiding behind a column or a row count the demo does not have.

---

## 5. Environment facts

### Local dev machine (this repo)

- `duckdb` v1.5.5 on PATH.
- `psql` 18.4 (Homebrew). A scratch cluster for `--record` lives at
  `<scratchpad>/pg`; start it with a **short socket dir** or it fails —
  the scratchpad path exceeds the 103-byte Unix socket limit:
  ```bash
  mkdir -p /tmp/pgs54329
  pg_ctl -D <scratchpad>/pg -o "-p 54329 -k /tmp/pgs54329" -l <scratchpad>/pg.log -w start
  ```
- `sqlglot` 30.17.0 in a scratchpad venv, **not** in the repo environment. Add it to
  `environment.yml` when you wire up the build.
- `grep` here is **ugrep**; multi-file globs behave differently from GNU grep. Write file lists
  to a temp file and pass them, or you will get confusing "File name too long" errors.
- No `timeout` command.

### the HPC cluster login node — SET UP, at `$SCRATCH/duckdb-port`

```
venv/                DuckDB 1.5.5 + sqlglot 30.17.0 on python/3.12.3 (module load required)
repo/                rsync of duckdb/ sql/ hpc/, with mimic-code symlinked in
parquet-demo/        30 staged demo tables
fhir-demo/           the demo export that passed the parity gate
parquet/ fhir-gzip/  job 30368991 / J2 output
metrics/ logs/
```

Recreate the venv with: `module load python/3.12.3 && python3 -m venv venv &&
./venv/bin/pip install duckdb==1.5.5 sqlglot==30.17.0`. **Delete any existing venv first** — a
venv built by the default python3 (3.6.15) silently resolves `duckdb==1.5.5` to "no such
version, max is 0.8.1". Do not pipe the `module load` line into anything; a pipeline runs it in
a subshell and the environment change is lost.

Source data is at `$SCRATCH/original-mimic-iv`: `mimic-iv-2.2` (7.2 G) +
`mimic-iv-ed-2.2` (117 M) full, `mimic-iv-clinical-database-demo-2.2` (16 M) +
`mimic-iv-ed-demo-2.2` (140 K) demo. Largest files: `chartevents.csv.gz` 2.47 G,
`labevents.csv.gz` 1.94 G.

### The original login-node survey, kept for the module names

```
hpc-login   x86_64   outbound network: YES
python3           3.6.15   ← DEFAULT IS TOO OLD for sqlglot
duckdb            NOT INSTALLED
modules           python/3.11.0  python/3.12.3(default)  python/3.14.2
                  miniconda3/{4.9.2, 4.12.0, 23.3.1}
$SCRATCH/psql-baseline/{logs,metrics,mimic-iv-fhir,out,pgdata}
```

So the login-node setup is: `module load python/3.12.3`, make a venv, `pip install sqlglot
duckdb`, and `curl` the static DuckDB CLI (x86_64) — the login node has outbound network,
compute nodes do not, so fetch everything there.

### Parity fixtures

- **The psql demo export already exists** at `$SCRATCH/psql-baseline/out` from job
  `30336753` — 30 ndjson files, 803 MB, 30/30 tables PASSED. This is a ready-made Phase-2 parity
  target requiring no new Postgres run.
- **Known blind spot:** all 100 demo patients satisfy `year(MIN(transfers.intime)) ==
  anchor_year`, so the `#126` birthDate fix measures **zero** on demo
  (`debug-patient-birthdate-anchor/README.md`). Demo parity is a plumbing gate. Never quote demo
  numbers as evidence for that fix.
- `sql/fhir_etl/subjects.sql` (100 ICU patients) exists but is **referenced by nothing**. Wire it
  in as an optional filter on the test path only.

---

## 6. Jobs

### Done

| id | Job | Ask | Result |
|---|---|---|---|
| `30368991` | `duckdb-stage` | 12 CPU / 32 GB / 3 h | **PASSED in 2 m 22 s.** 30/30 tables, 622,858,060 rows → 4.98 GB Parquet. *All assertions pass*, including the sub-second one that was expected to fire. Peak cgroup 5.7 GiB, sacct MaxRSS 9.7 GB. |
| `30369161` | `duckdb-probe` | 16 CPU / 96 GB / 4 h | **Binding check PASSED, throughput measurement LOST.** 97 files loaded and bound against full-scale data in 6.4 s, selftest passed. But it exported only `patient`, not `patient` + `observation_labevents` — see the `--export` trap below. |

Staging is comfortably over-provisioned: it finished in 2 m 22 s using 5.7 GiB of 32 GB. The
largest file (`chartevents`, 313 M rows) took 90 s, and because a `.csv.gz` is not seekable that
90 s is a single-threaded floor no amount of CPU removes. **If staging is ever re-run, 8 CPU /
16 GB is plenty.**

**The probe's real purpose held.** Loading all 97 files against staged data binds every statement
against real columns and real macro arities — the pass `transpile.py --explain` cannot do while
the source tables do not exist. Two of this session's three build-stopping bugs (the correlated
macro, `JSON_ARRAYAGG`) were exactly that class and only surfaced when data existed. That check
passed against the full extract.

**Its throughput number is worthless and must not be used.** `patient` is the smallest resource;
3.9 s for 299,712 rows is almost entirely fixed startup. **Do not size J2 from it, and do not
size J2 by extrapolating from the demo** — that is what `30374038` is for.

| `30374038` | `duckdb-probe2` | 16 CPU / 32 GB / 1 h | **PASSED, 11 m 23 s.** 4 resources, 36.2 M rows. The measurement that sized everything below. |

### What probe2 measured, and why it changed the build

| resource | rows | gz MB | sec | rows/s |
|---|---|---|---|---|
| `medication_request` | 15,416,901 | 1850.5 | 380.2 | 40,549 |
| `specimen_lab` | 13,376,689 | 696.4 | 110.6 | 120,946 |
| `observation_datetimeevents` | 7,112,999 | 250.7 | 60.2 | 118,156 |
| `patient` | 299,712 | 13.5 | 3.7 | 81,003 |

`mean_cores_busy = 1.83` against **16 allocated**. The per-sample trace is the real finding:
sustained **1.0–2.0 cores** for the whole export, with brief 4–9 core bursts only at resource
boundaries; 5 windows of 135 above 4 cores, 1 above 8. Memory "peaked" at 32.0 GiB — exactly the
cgroup ceiling — then fell back to ~10 GiB, i.e. page cache, not working set.

**The scan parallelises; the gzip stream does not.** One `COPY … TO 'one.gz'` is one compressor
thread regardless of allocation. That is the single reason a build underuses the node, and it is
what §4.6 fixes.

### `30374970` — the headline build — **COMPLETE, 18 min 03 s**

```bash
sbatch -J duckdb-build-par --exclusive --cpus-per-task=64 --mem=480G --time=06:00:00 \
    --export=ALL,SAMPLE_INTERVAL=15,OUT_DIR=$SCRATCH/duckdb-port/fhir-gzip-par \
    repo/hpc/duckdb/run_duckdb_step.sh build gzip
```

**30 resources, 555,352,597 rows, 31.40 GB gzip, 7.5 min of write time, 18:03 wall.**

End to end the DuckDB pipeline is **20 min 25 s**: staging (`30368991`, csv.gz → Parquet,
12 CPU) `02:22` + build (`30374970`) `18:03`. Against psql's measured `14:52:36` that is
**43.7×**, or **26.2×** if psql is charged only for the steps that produce the ndjson
(import + build + export = `8:54:14`, dropping CLUSTER which DuckDB has no counterpart for).

**`BENCHMARK.md` is the single source for all of this** — the three-way comparison, the
per-resource tables, the confounds, and the derivation of each figure. Do not restate
timings here or anywhere else; update `BENCHMARK.md` and link to it. Two things it says
that are easy to get wrong: quote **26.2× not 43.7×** as the headline (the conservative
number cannot be attacked over CLUSTER), and do not quote the 18:03 on its own — staging
is part of the DuckDB path.

Peak cgroup memory 247.2 GiB (n=72 samples), `sacct`
MaxRSS 245 GB — both well inside the 480 GB ask, so the next run of this could be sized
down considerably. Mean cores busy **21.5 of 64 (34 %)**, against psql's ~1–2 of 8.

Output verified: all 30 `.ndjson.gz` present, no leftover `.parts` directories, and
**every one of the 30 row counts matches `manifest.tsv` exactly**. That is not parity —
it says nothing about field content — but it is the cheapest possible pre-parity check
and it passed.

Throughput, parallel vs sequential (`30374316`), same code, same Parquet:

| resource | seq MB/s | par MB/s | speedup |
|---|---|---|---|
| `condition` | 1.3 | 8.2 | 6.1× |
| `medication_administration` | 3.2 | 67.4 | 21× |
| `medication_dispense` | 5.7 | 116.6 | 20× |
| `encounter_ed` | 4.6 | 54.2 | 12× |
| `observation_chartevents` | — | 100.5 | 313 M rows in 156.6 s |
| `observation_labevents` | — | 88.1 | 118 M rows in 89.3 s |

`medication_mix` is the one resource that did **not** speed up (19.8 s parallel vs 35.6 s
sequential for 6,338 rows — 0.0 MB/s either way). It is correlated-subquery bound, not
compressor bound, so parallel gzip has nothing to give it. It is also 0.6 MB, so it does
not matter; noted only so the next person does not go looking for a regression.

Both builds write to different directories on purpose, so neither overwrites the other's
parity target. `OUT_DIR` and `SINGLE_STREAM=1` are the two env knobs on
`run_duckdb_step.sh` (neither contains a comma, so both are `--export`-safe — trap 25).
A running job is unaffected by `rsync`ing the repo: Python has already read the module.

### In flight

| id | Job | Ask | Purpose |
|---|---|---|---|
| `30374316` | `duckdb-build-gzip` | 8 CPU / 48 GB / 8 h | The **sequential** build → `fhir-gzip`. Left running only to finish the "before" column above for the two big resources; nothing depends on it. Safe to `scancel`. |
| `30376630` | `parity-rest` | 1 CPU / 64 GB / 12 h | 28 resources. |
| `30376631` | `parity-labevents` | 1 CPU / 48 GB / 8 h | 118 M records. |
| `30376632` | `parity-chartevents` | 1 CPU / 128 GB / 20 h | 313 M records, the long pole. |

```bash
sbatch -J parity-rest        --mem=64G  --time=12:00:00 repo/hpc/duckdb/run_parity.sh rest
sbatch -J parity-labevents   --mem=48G  --time=08:00:00 repo/hpc/duckdb/run_parity.sh labevents
sbatch -J parity-chartevents --mem=128G --time=20:00:00 repo/hpc/duckdb/run_parity.sh chartevents
```

Three jobs rather than one because two resources are 82 % of the volume: one process means
the whole comparison is hostage to chartevents, and every small resource pays chartevents'
memory bill. The shards **partition** the 30 — `rest` is `--exclude Chartevents --exclude
Labevents`, not a hand-written list of 28 names that is one typo away from a quietly partial
run that still exits 0. Verify the union covers all 30 before believing any of it:

```bash
jq -r '.resources|keys[]' $SCRATCH/duckdb-port/metrics/parity-*.json | sort -u | wc -l   # want 30
```

Each shard exits 0 identical / 1 differences / 2 could not run, and writes a full JSON report
next to its log. `array_order:$.hasMember` is ignored — see §7, accepted difference.

#### Measured throughput, and why parity is single-threaded

At 60 min in, `parity-chartevents` sampled **`1.00` cores busy** — pinned, CPU-bound, no I/O
stall — with the digest dict at 28.5 GiB growing 400 MB/min against a predicted 57 GB full
pass. That is **~43,500 records/s**, which independently reproduces the 43,370 rows/s measured
on the demo export. Projection: chartevents ~2 h for pass 1 plus ~2.5 h for pass 2 (which adds
gunzip) ≈ **4.5 h**; all three shards done in ~5 h. If a resource differs *widely*, the digest
path re-reads both files for the field histogram and that shard roughly doubles.

Read the sampler, not the log, to check progress:

```bash
awk -F, 'NR>2 && $3!="NA" { if(p==""){p=$3;t=$1}
         printf "  +%5ds  mem=%6.2f GiB  cores_busy=%.2f\n", $1-t, $2/1073741824, ($3-p)/1e6/($1-t) }' \
    $SCRATCH/duckdb-port/metrics/parity-chartevents-*.samples.csv | tail -3
```

`memory.current` includes page cache, so it is not a clean progress signal in general — but here
the observed 400 MB/min is far below the ~2.2 GB/min of file being read, so the kernel is
reclaiming cache and the growth really is the dict. Cross-check it against ~183 B/record ×
43,500 rec/s = 477 MB/min before trusting it on a different resource.

**Threads would not help and this is not a tuning failure.** The work is `json.loads` → `canon`
→ `blake2b`, all pure Python holding the GIL; the `1.00` figure above is what a GIL-bound
process looks like. Threading it changes nothing.

**Processes would help, and the design is known** — recorded here so nobody has to re-derive it,
NOT because it should be built:

- The comparison has **no cross-record state**. Records match by `id` and are compared
  independently, so workers can shard on `hash(id) % N` and the merge is summing counters.
- The naive version is pointless: a worker must read every line of both files to learn whose
  it is, and calling `json.loads` to get the `id` has already done ~all the expensive work.
- The fix is a cheap id extraction — `line.find('"id"')` and slice, no parse, ~50–100× cheaper
  — so only the 1/N of lines a worker owns get parsed, canonicalised and digested. Then N
  workers cost N× the raw scanning but 1/N of the real CPU, and 1/N of the memory each.
- It needs a correctness fallback (if the string scan does not find a plain `"id":"..."`, parse
  properly) tested against **both** emitters, since Postgres and DuckDB order keys differently
  and the `id` is not at a fixed offset on either side.

That is a couple of hours of work plus tests, to save a few hours on a job that **runs exactly
once** and already fits the budget. The three-shard split already captured the cheap 3× by
partitioning across resources; going further only shortens the chartevents critical path.
Do this only if full-scale parity has to become a routine, repeated step.

### Reading the results

```bash
cat $SCRATCH/duckdb-port/logs/duckdb-probe2-30374038.out
sacct -j 30374038 --format=JobID,MaxRSS,Elapsed        # cross-check the cgroup peak
bash hpc/psql/collect_metrics.sh 30368991 30374038
python duckdb/parity.py $SCRATCH/psql-baseline/out/mimic4-export \
                        $SCRATCH/duckdb-port/fhir-probe
```

That parity call is worth running on the probe output: it is the **first full-scale semantic
check**, on four resources, and it costs nothing beyond reading files that already exist. Expect
`array_order $.hasMember` to be absent here — none of these four resources has a `hasMember`.

`stage.py` exits non-zero and prints a `PROBLEMS` block if any assertion fails; the Parquet is
still written, so a failure is informative rather than fatal.

### Still to come

| # | Job | Ask | Purpose |
|---|---|---|---|
| ~~J2~~ | ~~Full build, gzip~~ | — | **DONE**, `30374970`, 18 min. |
| J3 | Full build, zstd | same as J2 | The benchmark delta. **Explicitly deferred by the user**: "For now I will stick with gzip, later I will also use the other compression which is more efficient, but for now gzip only." Do not submit it unprompted. |

**Two sizing rules that are not negotiable:**

- `memory_limit` must be **well below** `--mem`, not equal to it (~60%). DuckDB's limit governs
  the buffer manager only; JSON string building and CSV writer buffers sit outside the tracked
  pool, so RSS exceeds the limit. Measured: 2.4 GB RSS under a 1 GB limit.
  `run_duckdb_step.sh` computes this from `SLURM_MEM_PER_NODE` automatically.
- DuckDB reads `/proc/meminfo`, **not the cgroup**. On a partial allocation it sees all 503 GB,
  defaults `memory_limit` to ~400 GB, and gets **OOM-killed by Slurm** rather than raising a
  clean error. Always set `threads` and `memory_limit` explicitly. Set `temp_directory` too, or
  spills land silently on Lustre.

`hpc/duckdb/run_duckdb_step.sh` copies the cgroup sampler **verbatim** from
`hpc/psql/run_psql_step.sh`, so `mean_cores_busy` and peak cgroup memory are measured identically
on both backends. The psql baseline's ~1–2 busy cores against 8 allocated is the number this port
exists to move.

**Benchmark confound to handle inside J2:** the psql baseline exports **uncompressed** ndjson, so
"DuckDB run 1 vs psql" also carries gzip's cost (~3.5×, and it does not parallelise away). Fix it
cheaply — after the main build, re-export one mid-size resource (`observation_outputevents`,
4.2 M rows) at `none` / `gzip` / `zstd` and time all three. Minutes, and it gives a same-hardware
control instead of a fourth job.

### The full-scale psql export is COMPLETE — resolved

`$SCRATCH/psql-baseline/out/mimic4-export` now holds **all 30** ndjson files plus
`manifest.tsv`, **516 GB** total, last written 15:00. The earlier "24 of 30" observation was
simply a mid-run snapshot. It is a valid full-scale parity target.

Size ranking matters for sizing J2, because two resources are 82 % of the output:

| resource | psql ndjson (uncompressed) |
|---|---|
| `MimicObservationChartevents` | 262 GB |
| `MimicObservationLabevents` | 159 GB |
| `MimicMedicationAdministration` | 25 GB |
| `MimicMedicationRequest` | 16 GB |
| everything else (26 files) | ~54 GB combined |

Note these are **uncompressed**; DuckDB writes gzip, so its bytes are ~25× smaller and the two
`MB` columns are not comparable. `MimicPatient` is 357 MB in psql and 13.5 MB from DuckDB.

---

## 7. Traps already paid for — do not rediscover these

| # | Trap | Fact |
|---|---|---|
| 1 | `CAST(x AS DECIMAL)` | DuckDB's bare `DECIMAL` is `DECIMAL(18,3)`. `'0.000001'` → `0.000`, **no error**. All 12 cast sites are text→numeric on lab/med values. Use `fhir_decimal` (= `CAST … AS DOUBLE`). |
| 2 | Timestamps | DuckDB renders `TIMESTAMPTZ` in the **session timezone** with a space separator — the same `#124` DST hazard, and wrong format regardless. `fhir_instant` casts to naive `TIMESTAMP` first; that is what makes it timezone-proof. |
| 3 | `sqlglot.transform()` | Prunes replaced subtrees. See §4.1(a). |
| 4 | `jsonb_agg(x ORDER BY …)` | `json_group_array` is a **macro, not an aggregate** — `ORDER BY` inside it is a hard error. Use `to_json(list(x ORDER BY …))`. Not `list(…)::JSON`, which emits spaces. |
| 5 | Row→key-value pivot | Solved: `FROM t, json_each(to_json(t)) AS x`, value as `json_extract_string(x.value,'$')` — **not** `x.value` or `x.atom`, both stay JSON-typed. Do **not** use native `UNPIVOT`: it silently drops NULL columns where `jsonb_each_text` emits them. |
| 6 | File boundaries | **22 of 109 files do not end in a semicolon** and rely on psql flushing its buffer at EOF. Any driver that concatenates files or splits on `;` produces syntax errors. Treat each file as an implicit statement terminator. |
| 7 | Scope | The old notes' census covered 55 files / ~4,250 lines. The real tree is **109 files / 5,881 lines** — `sql/codesystem/` (52) and `sql/medication/` (2) were missed and are **not optional**. |
| 8 | Inter-resource deps | There are **none**. `grep -E '(FROM\|JOIN)\s+mimic_fhir\.'` over `sql/` hits only the dev helper `create_single_fhir_resource.sql`. The 30 resources are a clean independent fan-out. |
| 9 | Reserved word | `check` is reserved in DuckDB; the selftest macro is `assert_eq`. |
| 10 | Lambdas | `->` lambda syntax is deprecated in DuckDB 1.5 and warns. Use `lambda x: …`. |
| 11 | `ARRAY_REMOVE` | sqlglot's own duckdb translation emits `list_filter(a, _u -> _u <> NULL)`, which keeps **nothing** (`x <> NULL` is never true). Measured `['x',NULL,'y']` → `[]`. Would empty `Encounter.location` on every ICU stay. Use the `list_denull` macro. |
| 12 | `regexp_match` | Postgres returns NULL on no-match; DuckDB `regexp_extract` returns **`''`**. Feeding a numeric cast, that is `CAST('' AS DOUBLE)` → build aborts. Wrap in `nullif(…, '')`. |
| 13 | Array subscripts | sqlglot stores Postgres's 1-based `[n]` as `Literal(n-1)`. DuckDB capture groups are 1-based like Postgres, so convert back or every group shifts by one. |
| 14 | `WITH` on `INSERT` | `WITH x AS (…) INSERT INTO t SELECT …` puts the CTEs on the **Insert** node (`args['with_']`), not the Select. Rendering the Select alone drops them silently — a view body is not resolved until first use. |
| 15 | Repeated `INSERT`s | 4 tables are populated by 2 INSERTs each. Postgres appends; `CREATE OR REPLACE VIEW` replaces. Union the parts, or 4 resources come out half-size with no error. |
| 16 | `import duckdb` | The repo's `duckdb/` dir is a namespace package. When the PyPI package **is** installed it loses (fine); when it is **not**, `import duckdb` succeeds and yields a module with no `connect()`. Check `hasattr(duckdb, 'connect')` before blaming the code. |
| 17 | `json_serialize_sql` | Parses without binding — ideal for a no-tables syntax gate — but **only accepts SELECT**. It returns an error *payload* rather than raising, so `IS NOT NULL` is not a pass check; read `payload['error']`. |
| 18 | ndjson export | psql's `\copy … WITH CSV QUOTE AS E'\b'` trick **does not transfer**. DuckDB's CSV writer also quotes any value containing `#`, the reader's comment character — hardcoded, and `COMMENT ''` on the COPY is accepted and ignored. That quoted 1,173 of 9,362 outputevents lines (`Chest Tube #1`) as `\b{""resourceType"":…}\b`: valid CSV, invalid JSON, 12% of lines. **Use a TAB as quote and escape** — DuckDB always escapes a literal tab inside JSON so nothing is ever doubled, and a leading/trailing tab is legal JSON whitespace, so both the quoted and unquoted forms parse. `driver.py` reads every file back with `read_ndjson_objects` to make this a build failure, not a parity finding. |
| 19 | CSV `""` | Postgres `NULL ''` makes an **unquoted** empty field NULL but keeps a **quoted** `""` as the empty string. DuckDB's `allow_quoted_nulls` defaults to **true** and nulls both — and a nulled field is dropped by `strip()`, so the FHIR key silently vanishes where psql emits `""`. Also: Postgres `ESCAPE` defaults to the `QUOTE` value, DuckDB's defaults to `"` independently. |
| 20 | `NUMERIC` scale | Postgres unconstrained `NUMERIC` **preserves the input's textual scale** through `to_jsonb`/`jsonb_each_text`: `'3.0000'` stays `3.0000`, `'2'` stays `2`. No DuckDB numeric type does — DOUBLE gives `3.0`, and `DECIMAL(18,4)` *also* renders `3.0` through `to_json`. Only VARCHAR reproduces it. `mimiciv_ed.triage.acuity` is the one column where this reaches output verbatim (a `valueString`), and the **full** ED extract writes `3.0000` where the demo writes `3`. `stage.py`'s `PIVOT_NUMERIC` enumerates every pivot-exposed NUMERIC column and fails the build on a new one. |
| 21 | `CHAR(n)` | Blank-padded, and the padding is **visible in `to_jsonb` but not in concatenation**: `to_jsonb('4019'::char(7))` is `"4019   "` while `'X-' \|\| '4019'::char(7)` is `X-4019` (the bpchar→text cast strips). So one column can be safe at one use site and wrong at another. `stage.py`'s `CHAR_REVIEW` records a decision per column and asserts its premise against the data. |
| 22 | Correlated macro | A macro whose body is `SELECT … FROM lookup WHERE key = $param` is a **correlated scalar subquery**, and DuckDB cannot plan one inside a query that also has an outer join: `Not implemented Error: Non-inner join on correlated columns not supported`. `fhir_patient.sql` has seven `LEFT JOIN`s, so `fn_patient_extension` failed at its only call site. Fix: build the lookup as a constant `MAP` in an **uncorrelated** subquery and index it — `map[k]` is NULL for a miss and a struct-with-NULL-fields for a row whose value is NULL, which is exactly the three-way distinction the join made. |
| 23 | `json_agg` ≠ `jsonb_agg` | sqlglot parses `jsonb_agg` as a generic `Anonymous` but `json_agg` as a **typed `exp.JSONArrayAgg`**, so a rule matching only function *names* misses it and sqlglot renders `JSON_ARRAYAGG` — which DuckDB does not have. One site, and it failed at `CREATE VIEW` time on real data. `FORBIDDEN` now scans for `JSON_ARRAYAGG` and `json_group_array`. |
| 24 | `date_part('microsecond')` | Follows the Postgres convention of returning **seconds × 10⁶ + microseconds**, so it is non-zero for any timestamp with a seconds value — it flagged most of MIMIC as "sub-second". Use `date_trunc('second', c) <> c`. Related: `sqlglot.optimizer.scope.build_scope` returns **None** for these files (they parse to a `Block`, or an `Insert`), so a scope-based column analysis resolves almost nothing and passes vacuously. |

| 25 | `sbatch --export` splits on commas | `--export=ALL,ONLY=a,b` does **not** set `ONLY="a,b"`. Slurm reads a comma-separated *list*: `ALL`, `ONLY=a`, and a bare `b` meaning "inherit `b` from the submitting environment" — which does not exist, so it is dropped. The job then runs a **valid, smaller request and exits 0**. This cost probe `30369161`, which measured one resource instead of two and reported success. `run_duckdb_step.sh` now takes the resource list as its **third positional argument**, which cannot be split, and echoes `only:` at startup so the log shows what was actually requested. |
| 26 | Sampler resolution | The cgroup sampler's default `SAMPLE_INTERVAL=60` is fine for a multi-hour build and **actively misleading** on a short one: on a 15 s job it sampled once, at startup, and printed a confident `peak_cgroup_memory_bytes=0.0 GiB` while `sacct` recorded MaxRSS **4.0 GB**. Sizing an allocation from that number would be badly wrong in the safe-looking direction. The peak line now prints `n=` and warns when `n < 5`. Pass `SAMPLE_INTERVAL=5` for probes (no comma, so it is `--export`-safe) and cross-check `sacct MaxRSS`. |
| 27 | `--only` is not the risk | `driver.py --only` is strict: it raises `SystemExit` listing every unknown resource name, so a typo cannot silently narrow a run. Both silent-narrowing incidents so far happened **upstream of Python**, in Slurm argument handling. When an export comes out short, check the submission line before the code. |

| 28 | DuckDB + `fork()` | An open DuckDB connection owns a background thread pool. `fork()` copies its mutexes but **not** its threads, so a child of a process holding an open connection **deadlocks on its first query** — even if the child opens a brand-new connection of its own, and even for plain `fetchall()` (it is not the pandas conversion). A pid guard cannot help: the child hangs before it can reconnect. This is not hypothetical — `validate.py` builds a connection, calls `get_n_patient_id()`, then forks a `multiprocessing` pool, so **every worker would hang, deterministically**. Fix: `os.register_at_fork(before=...)` closes every live handle and lets it reopen lazily. Measured: parent keeps a handle → child hangs; parent closes first → child fine. |
| 29 | `PER_THREAD_OUTPUT` | Writes a *directory* of `data_N.csv.gz`, not a file. Concatenating the parts is valid **because gzip is multi-member** — do not assume this transfers to every codec before checking. It does hold for zstd frames, but that has not been exercised here. |

### Accepted difference: `hasMember` array order

`MimicObservationMicroOrg` (2 of 338) and `MimicObservationMicroTest` (37 of 1,893) differ from
psql only in the **order** of `$.hasMember`, with identical members. The source is
`jsonb_agg(...)` under a `GROUP BY` with **no `ORDER BY`** in
`sql/fhir_observation_micro_org.sql:21` and `sql/fhir_observation_micro_test.sql`. Postgres does
not specify aggregate input order either, so this is upstream mapping nondeterminism that the
port faithfully reproduces — not a port defect.

**Deliberately not fixed in this session.** Adding `ORDER BY mi.microevent_id` to `sql/*.sql`
would be the correct fix and would give both backends determinism — but it changes the psql
output too, invalidating the completed 516 GB baseline, which is the user's run. Raise it as a
separate upstream mapping issue.

Until then, J2's parity run should be:

```bash
python duckdb/parity.py …/mimic4-export …/fhir-gzip --ignore 'array_order:$.hasMember'
```

`--ignore` **counts and reports** what it suppresses rather than hiding it, so the exception
stays visible in the output.

### Inherited behaviour that must NOT be "fixed"

Pinned by `test_functions.py`. All of these look like bugs. They are the current published
behaviour, and changing them here would be a mapping change smuggled into a port:

- `fn_med_statement` with `med_GSN = '0'` (or NULL) returns **NULL for the entire call** — the
  leading `CASE … THEN '['` has no `ELSE` and `||` propagates the NULL.
- `fn_patient_extension` returns **NULL** when `ethnicity` matches no `map_ethnicity` row,
  including when `ethnicity IS NULL`. A patient with no race therefore loses the
  `us-core-birthsex` extension too, even though birthsex is present.
- `fn_patient_extension`'s opening `'['` guard tests `fhir_ethnicity_code` while the closing
  `']'` guard tests `ethnicity`. Both are encoded in the port.

If any of these should actually be fixed, that is a **separate issue against the mapping SQL**,
applied to `sql/*.sql` so both backends get it. That is the whole point of keeping `sql/*.sql` as
the Postgres-dialect source of truth and transpiling at build time — the in-flight fixes
(`#124`, `#125`, `#126`, `debug-patient-birthdate-anchor/`) must be applied exactly once.

---

## 8. Repo state

Nothing is committed. Untracked, all new:

```
DUCKDB_PORT_PLAN.md          the plan
BENCHMARK.md                 THE benchmark artifact — 3-way psql/gzip/zstd, thesis-citable
metrics/                     machine-readable metrics pulled off the HPC, for plotting
HANDOFF.md                   this file
duckdb/                      runtime + transpile/stage/driver/parity + tests (§3)
hpc/duckdb/                  SLURM wrappers: run_duckdb_step.sh, run_parity.sh
build/duckdb/                generated SQL + manifest.json — gitignored, do not commit
PORT_NOTES_DUCKDB_SPARK.md   historical feasibility notes — superseded by §1 of the plan
PSQL_BUILD_HPC.md            psql baseline recipe        ) the user's, in flight,
PSQL_RUN_COMMANDS.md         psql baseline commands      ) do not modify
PSQL_RUN_METRICS.md          psql baseline metrics       )
PSQL_REBUILD_CSIRO.md        colleague's original notes  )
hpc/                         psql SLURM wrappers         )
hpc/psql/gzip_export.sh      NEW, ours — compresses the psql export into the IG shape
                             (one <Resource>.ndjson.gz per resource). Writes to a NEW
                             directory and never modifies the 528 GB source; refuses to
                             start if the destination is non-empty. BENCHMARK.md §7.
```

Modified (pre-existing work, unrelated to the port):
`debug-patient-birthdate-anchor/{BUG_patient_birthdate_anchor,ISSUE_DRAFT,README}.md`

Ask before committing — the user has not said how they want this branched.

Nothing on the HPC is under version control. `$SCRATCH/duckdb-port/repo` is an `rsync`
of `duckdb/`, `sql/` and `hpc/` from this checkout; re-sync it after any change, or the job runs
old code:

```bash
rsync -a duckdb/ <hpc-login>:$SCRATCH/duckdb-port/repo/duckdb/
rsync -a sql/    <hpc-login>:$SCRATCH/duckdb-port/repo/sql/
rsync -a hpc/    <hpc-login>:$SCRATCH/duckdb-port/repo/hpc/
```

Slurm snapshots a batch script at submit time, so editing `run_duckdb_step.sh` does not affect
an already-queued job.

---

## 9. Task list state

```
#1  Build DuckDB runtime macro library + selftest        COMPLETED
#2  Write transpile.py with all rewrite rules            COMPLETED
#3  Port the 5 plpgsql functions with unit tests         COMPLETED
#4  Finish the rule 5 TIMESTAMPTZ audit                  COMPLETED  (§4.1, safe)
#5  Write parity.py semantic ndjson differ               COMPLETED  (§4.2, 25/25)
#6  Write stage.py                                       COMPLETED  (§4.3)
#7  Write driver.py                                      COMPLETED  (§4.4)
#8  Set up login-node DuckDB environment                 COMPLETED  (§5)
#9  Phase 1 gate: demo parity vs psql                    COMPLETED  (28/30, §4.5)
#10 Stage the full dataset                               COMPLETED  job 30368991
#11 Scale probe                                          COMPLETED  job 30374038
#12 J2 full build (gzip)                                 COMPLETED  job 30374970, 18 min
#13 J3 full build (zstd)                                 DEFERRED   by the user, gzip only
#14 py_mimic_fhir DUCKDB shim for HAPI validation        COMPLETED  (§2, 11/11)
#15 Parallel compression (the build optimisation)        COMPLETED  (§4.6)
#16 parity.py digest path for full scale                 COMPLETED  (§4.7, 25/25 both paths)
#17 Full-scale parity vs psql, 3 shards                  RUNNING    30376630/1/2  <- the gate
```

### The one open question the demo cannot answer

`Observation.hasMember` array ORDER differs on 39 arrays. The source `json_agg`/`jsonb_agg` has
no `ORDER BY`, so the order is unspecified on the Postgres side too — the members are the same
set, which parity.py proves by reporting `array_order` rather than element diffs. **Decide
whether that is acceptable before J2**, because the answer is either "yes, document it" or "add
an `ORDER BY` to `sql/*.sql`", and the second is a mapping change that belongs in its own issue
against `sql/` so both backends get it.

### Two things the demo provably does NOT test

1. **The `#126` birthDate fix measures zero on demo** — all 100 demo patients satisfy
   `year(MIN(transfers.intime)) == anchor_year` (`debug-patient-birthdate-anchor/README.md`).
   Demo parity is a plumbing gate. Never quote demo numbers as evidence for that fix.
2. **`triage.acuity` is `3.0000` in the full extract and `3` in the demo.** The VARCHAR staging
   decision (trap 20) is only really exercised at full scale. It is the first thing to check in
   `MimicObservationED` against the full psql export.
