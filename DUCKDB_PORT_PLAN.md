# DuckDB port — implementation plan

Turns `PORT_NOTES_DUCKDB_SPARK.md` (feasibility notes) into an executable plan.

The notes' verdict holds: **DuckDB, transpile at build time, do not hand-port.** This document
fixes the scope, corrects the findings from the notes that did not survive re-verification,
and lays out the files, rules, gates and jobs.

Everything marked ✓ below was executed on 2026-08-22 against **DuckDB v1.5.5** and
**PostgreSQL 18.4** locally. Postgres reference values are measured, not recalled.

> Baseline dependency: this plan compares against the psql run in `PSQL_BUILD_HPC.md`, which is
> in flight. Phases 0–2 do not depend on it. Only the full-scale parity gate (Phase 4) does.

---

## 1. Corrections to `PORT_NOTES_DUCKDB_SPARK.md`

Re-verification changed five of the notes' findings and added one the notes did not reach.
Four of the five are cheap to fix; one (numerics) is a silent data-loss bug that would have
shipped. The sixth (§1.7) goes the other way — a shape I expected to be a problem is not.

### 1.1 The census missed half the codebase

The notes say "37 files, ~4,250 lines". The actual tree is **109 files, 5,881 lines**.
`sql/codesystem/` (52 files, building the `fhir_trm.*` CodeSystem/ValueSet tables) and
`sql/medication/` (2 files, building `mimic_fhir.medication` and `medication_mix`) were not
counted. They are not optional: `fhir_condition`, `fhir_condition_ed` and
`fhir_medication_statement_ed` join `fhir_trm.*`, and `py_mimic_fhir/terminology.py` consumes
those tables to emit the CodeSystem/ValueSet resources.

They are also the *easy* half — 9 `jsonb_build_object` and 4 `uuid_generate_v5` across all 54,
mostly `CREATE TABLE` + `INSERT ... SELECT DISTINCT`. They cost rewriter coverage, not new rules.

### 1.2 `CAST(x AS DECIMAL)` silently destroys data in DuckDB ✓

DuckDB's bare `DECIMAL` is **`DECIMAL(18,3)`**. Postgres `NUMERIC` is arbitrary precision.

| expression | Postgres jsonb | DuckDB `json_object` |
|---|---|---|
| `CAST('5.20' AS NUMERIC)` | `5.20` | `5.200` |
| `CAST('7' AS NUMERIC)` | `7` | `7.000` |
| `CAST('0.000001' AS NUMERIC)` | `0.000001` | **`0.000`** ← data loss |

All 12 numeric cast sites are **text → numeric** (`vs.value` from a `jsonb_each_text` unnest,
`split_part(lab.value,…)`, `emd.dose_given`, `pr.dose_val_rx`, `ph.one_hr_max`), so the source
scale is whatever the CSV holds and Postgres preserves it exactly. Truncating lab values to
3 decimal places would corrupt `MimicObservationLabevents` and `MimicObservationVitalSignsED`
without erroring.

**Fix:** rewrite rule 5b, `CAST(x AS NUMERIC|DECIMAL)` → `fhir_decimal(x)`, defined as
`CAST(x AS DOUBLE)`. Verified ✓ `5.20→5.2`, `7→7.0`, `0.000001→0.000001`, `1e-9→1e-9`. Residual
differences from Postgres are trailing-zero/exponent formatting of the *same* decimal value and
are legal FHIR `decimal`; the parity harness must compare numerics as decimals, not as strings
(§7). Use `CAST`, not `TRY_CAST`, so a value the guards fail to catch still errors loudly
instead of silently becoming `null`.

### 1.3 DuckDB does **not** sidestep the timezone trap ✓

The notes (§6) claim DuckDB "sidesteps the DST trap … because the macro formats UTC explicitly".
The macro does, but only if written correctly — DuckDB has the same hazard as Postgres:

```
DuckDB  json_object('t', TIMESTAMPTZ '2180-07-23 14:00:00+00')  -> {"t":"2180-07-24 00:00:00+10"}
```

That `+10` is the session timezone leaking in, exactly the `#124` defect. And even under
`SET TimeZone='UTC'` DuckDB renders `"2180-07-23 14:00:00+00"` — a **space separator and a
`+00` offset**, where Postgres jsonb renders `"2180-07-23T14:00:00+00:00"`. Every one of the
45 timestamp sites would differ.

**Fix (verified ✓ byte-identical to Postgres):**

```sql
CREATE MACRO fhir_instant(t) AS strftime(CAST(t AS TIMESTAMP), '%Y-%m-%dT%H:%M:%S+00:00');
```

Casting to naive `TIMESTAMP` first is what makes it timezone-proof — the session setting cannot
reach it. Returns `NULL` on `NULL` ✓ (so `strip()` still removes the key). It drops fractional
seconds; MIMIC-IV is second-resolution, but Phase 1 asserts that on real data rather than
assuming it.

`DATE` needs no macro — both engines render `"2180-07-23"` ✓.

### 1.4 `sqlglot`'s `transform()` prunes replaced subtrees ✓

The obvious rewriter implementation silently does nothing. `Expression.transform` stops
descending into any node the callback replaces, so returning a freshly constructed node drops
every rewrite beneath it:

```
transform: jsonb_strip_nulls(jsonb_build_object('a',x,'b',jsonb_build_array(uuid_generate_v5(n,'q'))))
        -> SELECT JSONB_BUILD_OBJECT('a', x, 'b', JSONB_BUILD_ARRAY(UUID_GENERATE_V5(n, 'q')))   ✗
```

The outer `strip_nulls` was unwrapped; nothing under it was touched. **Rename in place**
(`node.set("this", "json_object")` on the `exp.Anonymous`) and run structural replacements as a
separate innermost-first pass:

```
        -> SELECT JSON_OBJECT('a', x, 'b', JSON_ARRAY(UUID_V5(n, 'q')))                          ✓
```

Applied to the real `sql/fhir_patient.sql`: 15 `jsonb_*` calls → 0 remaining, 10 `json_object`,
`uuid_v5` in place ✓.

### 1.5 `jsonb_agg(x ORDER BY …)` has no direct DuckDB equivalent ✓

DuckDB's `json_group_array` is a **macro, not an aggregate**, so `ORDER BY` inside it is a hard
error — and ordering matters here (`medication_mix.sql` orders ingredients MAIN-BASE-ADDITIVE).

```
Invalid Input Error: Function "json_group_array" is a Macro Function.
"DISTINCT", "FILTER", and "ORDER BY" are only applicable to aggregate functions.
```

**Fix:** `jsonb_agg(x ORDER BY …)` → `to_json(list(x ORDER BY …))` ✓. Use `to_json(list(…))`,
not `list(…)::JSON` — the cast emits `[{"v":2}, {"v":1}]` with spaces, `to_json` emits compact.
`STRING_AGG(x, sep ORDER BY …)` works unchanged ✓.

### 1.6 Also worth knowing

- **22 of the 109 files do not end in a semicolon** and rely on psql flushing its query buffer
  at EOF (`map_gender.sql`, `uuid_namespace.sql`, `fhir_patient.sql`, …). Any driver that
  concatenates files or splits on `;` will produce syntax errors. Treat each file as an
  implicit statement terminator.
- **No mapping reads another mapping's output.** `grep -E '(FROM|JOIN)\s+mimic_fhir\.'` over
  `sql/` returns exactly one hit, in the dev helper `create_single_fhir_resource.sql`. Every
  resource is independently derivable from source + `fhir_etl.*` + `fhir_trm.*`. This is what
  makes rewrite rule 7 (`INSERT INTO` → `COPY TO ndjson`) safe, and it makes the 30 resources a
  clean fan-out with no ordering constraints beyond the lookup tables.
- `ARRAY[jsonb_build_object(...)]` needs no rule — a DuckDB `LIST` of `JSON` renders as a JSON
  array in a `json_object` position ✓. `ARRAY_REMOVE(a, NULL)` →
  `list_filter(a, lambda v: v IS NOT NULL)` ✓ (use the new `lambda` syntax; `->` is deprecated
  in 1.5 and warns).
- Max `jsonb_build_object` arity in the repo is 38; DuckDB `json_object` handles 120 ✓.

### 1.7 The row-to-key-value pivot is a direct translation, not a rewrite ✓

I expected the two `jsonb_each_text(to_jsonb(vs))` unnests in `fhir_observation_vitalsigns.sql`
and `fhir_observation_ed.sql` to be the one genuinely non-portable *shape*. They are not —
DuckDB has `json_each` as a table function and `to_json()` works on a table alias:

```sql
FROM vs, json_each(to_json(vs)) AS x        -- x.key, x.value
```

Take the value as `json_extract_string(x.value, '$')`, **not** `x.value` or `x.atom` — both of
those stay JSON-typed and keep string quoting. With `json_extract_string` the semantics match
`jsonb_each_text` exactly ✓: unquoted text, SQL `NULL` for a null column, embedded quotes and
commas preserved.

Do **not** use DuckDB's native `UNPIVOT` for this. Verified ✓ that it silently drops columns
whose value is `NULL`, where `jsonb_each_text` emits them with a `NULL` value — a row-count
difference in the two largest ED resources.

### 1.8 Confirmed from the notes

- `uuid_v5` macro ✓ — matches the RFC 4122 vector *and* the MIMIC-IV root
  `24ba6d92-ae8e-56f9-8898-873d8cba02da` that `hpc/psql/setup_db.sh` asserts. Chained through
  `uuid_v5(uuid_v5(ns_oid(),'MIMIC-IV'),'Patient')` = `569c1027-384a-58ff-84f2-0338bab99bc1`.
  **Consequence: `uuid_namespace.sql` needs no precomputed CSV** — rule 4 plus a constant
  `ns_oid()` macro transpiles it as-is, keeping one source of truth.
- `strip(j) = json_merge_patch('{}'::JSON, j)` ✓, recurses objects, not arrays. Postgres
  `jsonb_strip_nulls` does recurse arrays ✓ (`{"x": [{"b": 1}]}`), so the array-element wrapping
  in rule 1 is genuinely required.
- Both engines keep explicit JSON `null`s in build-object ✓, so `strip` is load-bearing.
- ndjson export via `FORMAT CSV, QUOTE '', DELIMITER E'\x01'` ✓, with `COMPRESSION 'gzip'` ✓;
  round-trips through `json.loads` including embedded quotes, commas and backslashes.
- Key ordering differs (Postgres sorts length-then-bytes: `{"a","zz","bbb"}`; DuckDB preserves
  insertion order) ✓ — semantic comparison only, as the notes say.
- **107 of 109 files parse under `sqlglot.parse(read="postgres")`** after stripping backslash
  meta-commands ✓. The two failures are `create_fhir_jsons.sql` (pure psql meta-commands) and
  `create_table_clusters.sql` (`CLUSTER`) — precisely the two files the port deletes.

---

## 2. Scope and non-goals

**In scope:** all 109 `sql/**.sql` files → a DuckDB pipeline producing the same 30 ndjson files
as `create_fhir_jsons.sql`, plus the `fhir_trm.*` tables `py_mimic_fhir/terminology.py` reads.

**Out of scope for now, stated so it is a decision and not an oversight:**

- **Spark.** Notes §9 settles it. Keep the dialect seam honest; build nothing.
- **Byte-identical parity with published PhysioNet ndjson.** Requires a key canonicalizer.
  Semantic parity only (§7) unless someone downstream says otherwise — this is open question 1
  in the notes and is now a stated assumption.
- **Retiring the psql pipeline.** The port runs alongside it. `sql/*.sql` stays the source of
  truth in Postgres dialect and is transpiled at build time, so the in-flight mapping fixes
  (`debug-patient-birthdate-anchor/`, `#125`, `#126`) need applying exactly once.
- **HAPI/`py_mimic_fhir` changes.** Open question 3 in the notes — resolve in Phase 0 by reading
  `py_mimic_fhir/db.py`; if validation reads Postgres directly it needs a shim, and that is a
  separate piece of work.

---

## 3. Layout

New code lives under `duckdb/`; generated SQL under `build/duckdb/` (already gitignored).

```
duckdb/
├── runtime/
│   ├── 00_settings.sql       threads, memory_limit, temp_directory, TimeZone='UTC'
│   ├── 10_macros.sql         uuid_v5, ns_oid, strip, fhir_instant, fhir_decimal
│   ├── 20_functions.sql      the 5 hand-ported fn_* macros
│   └── 99_selftest.sql       macro assertions; must pass before any build
├── transpile.py              sqlglot rewriter: sql/**.sql -> build/duckdb/**.sql
├── driver.py                 orchestration replacing the psql meta-commands
├── stage.py                  MIMIC csv.gz -> Parquet, explicit types from mimic-code DDL
├── parity.py                 semantic ndjson diff, psql export vs duckdb export
├── tests/
│   └── test_functions.py     unit tests for the 5 ported functions
└── hpc/
    ├── run_stage.sh          SLURM: CSV -> Parquet on node-local scratch
    └── run_duckdb_build.sh   SLURM: the build
```

`transpile.py` writes a `-- generated from sql/<path> — do not edit` banner into every output
file so a stray hand-edit is obvious in review.

---

## 4. The runtime macro library

`duckdb/runtime/10_macros.sql` — every definition below is verified ✓.

```sql
CREATE OR REPLACE MACRO ns_oid() AS '6ba7b812-9dad-11d1-80b4-00c04fd430c8';

CREATE OR REPLACE MACRO uuid_v5(ns, name) AS (
  WITH h AS (SELECT sha1(from_hex(replace(ns::VARCHAR,'-','')) || encode(name)) AS s)
  SELECT substr(s,1,8)||'-'||substr(s,9,4)||'-5'||substr(s,14,3)||'-'
      || format('{:x}', ((('0x'||substr(s,17,1))::INT & 3) + 8))
      || substr(s,18,3)||'-'||substr(s,21,12)
  FROM h
);

CREATE OR REPLACE MACRO strip(j)         AS json_merge_patch('{}'::JSON, j);
CREATE OR REPLACE MACRO fhir_instant(t)  AS strftime(CAST(t AS TIMESTAMP), '%Y-%m-%dT%H:%M:%S+00:00');
CREATE OR REPLACE MACRO fhir_decimal(x)  AS CAST(x AS DOUBLE);
```

`duckdb/runtime/00_settings.sql`, sized from the SLURM allocation rather than the node — the
notes' §7.2 gotcha (DuckDB reads `/proc/meminfo`, not the cgroup, and will get OOM-killed by
Slurm on a partial allocation):

```sql
SET threads = ?;                       -- from SLURM_CPUS_PER_TASK
SET memory_limit = '?';                -- ~60% of --mem, NOT equal to it (RSS exceeds the limit)
SET preserve_insertion_order = false;
SET temp_directory = '?';              -- node-local /tmp, 327 GB
SET TimeZone = 'UTC';                  -- defence in depth; fhir_instant does not depend on it
```

`99_selftest.sql` asserts, and the driver refuses to build if any fails:

| assertion | expected |
|---|---|
| `uuid_v5('6ba7b810-…-c04fd430c8','www.example.com')` | `2ed6657d-e927-568b-95e1-2665a8aea6a2` |
| `uuid_v5(ns_oid(),'MIMIC-IV')` | `24ba6d92-ae8e-56f9-8898-873d8cba02da` |
| `uuid_v5(uuid_v5(ns_oid(),'MIMIC-IV'),'Patient')` | `569c1027-384a-58ff-84f2-0338bab99bc1` |
| `fhir_instant(TIMESTAMP '2180-07-23 14:00:00')` | `2180-07-23T14:00:00+00:00` |
| `fhir_instant(NULL)` | `NULL` |
| `strip(json_object('a',NULL,'b',1))` | `{"b":1}` |
| `fhir_decimal('0.000001')` | `0.000001` (not `0.000`) |

The first two are the same vectors `hpc/psql/setup_db.sh` checks against `uuid-ossp`, which is
what ties the two backends to one identity scheme.

---

## 5. The rewriter

`sqlglot.parse(read="postgres")` → in-place rules → `.sql(dialect="duckdb")`. Rule counts are
re-measured over all 109 files (the notes' counts covered 55).

| # | Rule | Sites | Notes |
|---|---|---|---|
| 1 | `jsonb_build_object` / `json_build_object` → `json_object` | 427 | in-place rename (§1.4) |
| 1b | wrap in `strip()` at top level + direct array-element positions | ~116 | §3.2 of the notes; verified equivalent to wrapping all |
| 2 | `jsonb_build_array` → `json_array` | 168 | |
| 3 | `jsonb_strip_nulls(x)` → `x` | 35 | redundant after 1b; innermost-first replacement pass |
| 4 | `uuid_generate_v5` → `uuid_v5`, `uuid_ns_oid()` → `ns_oid()` | 198 + 31 | makes `uuid_namespace.sql` transpile as-is |
| 5 | `CAST(x AS TIMESTAMPTZ)` → `fhir_instant(x)` | 45 | §1.3 |
| 5b | `CAST(x AS NUMERIC\|DECIMAL)` → `fhir_decimal(x)` | 12 | §1.2 — **new rule, data-loss fix** |
| 6 | `fhir_etl.fn_*` → unqualified `fn_*` macro | 34 | |
| 7 | `jsonb_agg(x ORDER BY …)` → `to_json(list(x ORDER BY …))` | 4 | §1.5 — **new rule** |
| 8 | `ARRAY_REMOVE(a, NULL)` → `list_filter(a, lambda v: v IS NOT NULL)` | 1 | §1.6 |
| 8b | `jsonb_each_text(to_jsonb(t)) AS x(key, value)` → `json_each(to_json(t)) AS x`, reading the value as `json_extract_string(x.value,'$')` | 4 | §1.7 |
| 9 | `INSERT INTO mimic_fhir.X SELECT …` → `COPY (SELECT fhir::VARCHAR …) TO 'X.ndjson…'` | 30 | safe: no inter-resource reads (§1.6) |
| 10 | Drop `CREATE TABLE mimic_fhir.*`, `CLUSTER`, `fn_create_table_patient_dependent(…)` | ~55 | |

`fhir_etl.*` and `fhir_trm.*` tables are **kept as real tables** — they are small, they are
joined against, and `fhir_trm.*` has an external consumer. Only `mimic_fhir.*` becomes files.

**Guard rails in `transpile.py`:**

1. Parse every file; abort on any failure except the two known-dropped files.
2. After rewriting, assert the generated SQL contains no `jsonb`, no `fhir_etl.fn_`, no
   `uuid_generate_v5`, no `TIMESTAMPTZ`, no `CLUSTER`. A silent no-op rule (§1.4) is the failure
   mode this catches.
3. `EXPLAIN` every generated statement against an empty DuckDB with the schema attached, before
   running any of them. Catches typos in seconds instead of an hour into a build.

Non-goal: `transpile.py` does not emit Spark. Keep the dialect a parameter, add no second
backend.

---

## 6. The 5 plpgsql functions

All five parse as `Create` nodes with an opaque `$$` body — sqlglot will not port them, as
expected. Hand-write them as DuckDB macros in `duckdb/runtime/20_functions.sql`.

| function | call sites | port |
|---|---|---|
| `fn_create_table_patient_dependent` | 25 | **delete** — no tables, no indexes |
| `fn_patient_extension` | 1 | scalar subquery over `map_ethnicity`/`map_race_omb` |
| `fn_med_statement` | 1 | `to_json(list_filter([...], lambda x: x IS NOT NULL))` |
| `fn_build_medication_identifier` | 1 | as above |
| `fn_prescriptions_medication_code` | 1 | plain `CASE`, trivial |

Two things the notes did not flag:

- **`fn_patient_extension` is not a pure function** — it reads `fhir_etl.map_ethnicity` and
  `map_race_omb`. As a DuckDB scalar macro this becomes a correlated subquery, which DuckDB
  decorrelates into a join, so it should not be a per-row cost. Verify on the demo, and if it
  does show up, hoist the two lookups into `fhir_patient.sql` as `LEFT JOIN`s instead.
- **It returns a JSON *array*, and `strip()` does not recurse into arrays.** Postgres's outer
  `jsonb_strip_nulls` on the Patient object *does* strip nulls inside that array. So the ported
  macro must strip each element itself. Rule 1b cannot see inside a hand-written macro; this is
  the one place the automated placement rule has a blind spot.

**Behaviour to pin with tests before touching anything** (`duckdb/tests/test_functions.py`):
the `med_GSN = '0'` path in `fn_med_statement` — the leading `CASE … THEN '['` has no `ELSE`,
so the whole concatenation collapses to `NULL`. Same for `fn_patient_extension` when no
`map_ethnicity` row matches (`SELECT … INTO` with no rows → `NULL`). Both look intentional.
Preserve exactly; do not "fix" them in this port.

---

## 7. Parity harness

`duckdb/parity.py` — parse both sides to Python objects and diff. Never `cmp`.

Normalisations, each justified by a measured engine difference:

| normalise | why |
|---|---|
| object key order | Postgres sorts length-then-bytes, DuckDB preserves insertion order ✓ |
| numbers → `decimal.Decimal` | `5.20` vs `5.2`, `7` vs `7.0`, `1e-9` vs `0.000000001` (§1.2) |
| **nothing else** | any other difference is a real defect |

Sort each file by `resource.id` before diffing — `preserve_insertion_order = false` means line
order is not stable, and it carries no meaning.

Report per resource: line count, resources only-in-A / only-in-B (by id), and for common ids the
top differing JSON paths with counts. A field-path histogram is what makes a systematic
rewriter bug visible; a list of 40,000 individual diffs is not.

**Fixtures, in order of cost:**

1. **The 100-patient set.** `sql/fhir_etl/subjects.sql` exists but — as the notes flag — is
   referenced by nothing. Wire it into the driver as an optional `subject_id IN (SELECT …)`
   filter on the test path only.
2. **The demo dataset.** `mimic-iv-clinical-database-demo-2.2` + `-ed-demo-2.2`. The psql demo
   export **already exists** on `$SCRATCH` from job `30336753` (30 files, 803 MB) — a
   ready-made parity target requiring no new Postgres run. This is the Phase-2 gate.
3. **Full scale.** Phase 4, against the in-flight baseline.

Fixture 2 has a documented blind spot: all 100 demo patients satisfy
`year(MIN(transfers.intime)) == anchor_year`, so the `#126` birthDate fix measures zero there
(`debug-patient-birthdate-anchor/README.md`). Demo parity is a plumbing gate, not evidence.

---

## 8. Staging the source data

`stage.py`: MIMIC csv.gz → Parquet with **explicit column types**, once, then never read CSV on
the hot path. 31 distinct source tables are referenced across `sql/`:

```
mimiciv_hosp   patients admissions transfers prescriptions pharmacy poe emar emar_detail
               labevents d_labitems microbiologyevents diagnoses_icd d_icd_diagnoses
               procedures_icd d_icd_procedures hcpcsevents services diagnosis_icd
mimiciv_icu    icustays chartevents d_items inputevents outputevents procedureevents
               datetimeevents
mimiciv_ed     edstays triage vitalsign diagnosis medrecon pyxis
```

Types must not be inferred. Two failure modes if they are: a `VARCHAR` value column sampled as
`DOUBLE` and then rejected 200 M rows in, and a timestamp column that parses on one table and
not another. Derive the DDL from
`mimic-code/mimic-iv/buildmimic/postgres/create.sql` — the same file `psql_import.sh` uses, so
both backends load identical types by construction. (The `mimic-code` submodule is not checked
out locally; it is populated on the HPC clone per `PSQL_BUILD_HPC.md` §3.)

Assertions in `stage.py`: row count per table matches the CSV, and **no timestamp column has a
non-zero sub-second component** — that is what licenses `fhir_instant` dropping fractional
seconds (§1.3).

Target: node-local `/tmp` (327 GB, per notes §7.3 — MIMIC-IV is ~20–30 GB as Parquet, so it
fits with room to spare). Because `/tmp` is per-job, staging and building must be **one job**,
or staging writes to `$SCRATCH` and the build copies it in. Start with one job.

---

## 9. Phases and gates

Each gate is pass/fail. A failed gate stops the phase; it does not get waived.

### Phase 0 — decisions (½ day, no code)

- Read `py_mimic_fhir/db.py` and `validate.py`: does HAPI validation read Postgres directly, or
  only ndjson? Determines whether a shim is in scope (notes, open question 3).
- Confirm semantic-parity-only is acceptable (notes, open question 1).

**Gate:** both answered in writing.

### Phase 1 — runtime + rewriter, three resources (2–3 days)

`duckdb/runtime/*`, `transpile.py` with all 10 rules, and the three shape-covering resources
from the notes: `fhir_patient` (flat, exercises `fn_patient_extension`), `fhir_specimen`
(grouped), `fhir_observation_labevents` (high volume, exercises `fhir_decimal` and the
`split_part` guards).

**Gate:** `99_selftest.sql` green; all 109 files parse; the three resources produce
semantically-identical output to psql on the 100-patient fixture.

**Runs on the login node.** No job.

### Phase 2 — full port on demo (1–2 weeks)

Remaining 27 resources, 52 codesystem files, `fn_*` ports with tests, `driver.py`, `stage.py`.

**Gate:** 30/30 ndjson files semantically identical to the psql demo export from job
`30336753`; per-resource line counts match exactly.

**Runs on the login node**, threads and memory capped (demo is ~100 patients). No job.

### Phase 3 — full-scale staging + build (1 job each)

Stage CSV → Parquet, then build. Both need submission — see §10.

**Gate:** row counts match `sql/validate_fhir_tables.sql` exactly — `313,645,032`
`observation_chartevents`, `118,171,367` `observation_labevents`, and the other 28. This is the
same gate the psql baseline is held to, so it is directly comparable.

### Phase 4 — parity + benchmark

Semantic diff against the psql baseline export; publish the runtime comparison.

**Gate:** zero unexplained semantic diffs; a measured speedup number replacing the notes'
"12h → well under 1h" estimate.

### Phase 5 — zstd run

See §11.

---

## 10. HPC jobs — what I would submit, and why

Nothing gets submitted without your go-ahead. There are **three** jobs in the whole plan.

| # | Job | Ask | Purpose |
|---|---|---|---|
| J1 | Stage Parquet | 16 CPU, 64 GB, 4 h | csv.gz → Parquet + row-count and sub-second assertions. I/O-bound, needs no RAM. |
| J2 | Full build (gzip) | 32 CPU, 256 GB, 8 h | The measurement. `threads=32` per notes §7.4; `memory_limit='150GB'` — deliberately ~60 % of `--mem`, because DuckDB's limit does not bound RSS (notes §7.2, measured 2.4 GB RSS at a 1 GB limit). |
| J3 | Full build (zstd) | same | §11. Only after J2 passes its gate. |

J1 and J2 can be one job if node-local `/tmp` is the staging target — that avoids writing
20–30 GB of Parquet to Lustre and reading it back. I would rather run them **separately first**
(Parquet to `$SCRATCH`), so a build failure does not cost the staging time again, and fuse them
only if Lustre read bandwidth turns out to dominate.

Why the ask is smaller than the psql jobs: no `shared_buffers` floor. The psql wrapper needs
192 GB because Postgres reserves 128 GB up front; DuckDB's working set is ~0.3 GB/thread
(notes §7.1), so 256 GB is already ~10× headroom and 32 CPUs backfill fast on `defq`.

Reuse `hpc/psql/collect_metrics.sh` unchanged so `mean_cores_busy` and peak cgroup memory are
measured the same way on both backends. The psql baseline's ~1–2 busy cores against 8 allocated
is the number the port exists to move; measuring it differently would waste it.

Everything before Phase 3 runs on the login node. I will come to you with the exact `sbatch`
line and expected runtime before J1.

---

## 11. Compression: gzip now, zstd as run 3 — yes, that works

Your plan is right and it is what I would do. Compression is a single `COPY` option; nothing in
the rewriter, driver, macros or parity harness varies with it, so run 3 is a one-line config
change and re-submission. Sequencing it after a passing gzip run also means the zstd run has
nothing else changing underneath it, which is exactly what you want from a benchmark delta.

Two things to set up now so run 3 is clean, and one confound to be aware of.

**Set up now.** The driver must log **per-resource write time** and output bytes, not just a
total. With that, gzip-vs-zstd is a 30-row table rather than two wall-clock numbers, and you can
see whether the difference concentrates in the two 100 M-row resources or spreads evenly. Cheap
now, impossible to reconstruct later.

**The confound.** The psql baseline exports **uncompressed** ndjson (`\copy … WITH CSV`). So
"DuckDB run 1 vs psql" is not purely a backend comparison — it also carries gzip's cost, which
the notes measured at ~3.5× the uncompressed write time and which does not parallelise away.
Fix it cheaply inside J2 rather than with a fourth job: after the main build, re-export **one
mid-size resource** — `observation_outputevents`, 4.2 M rows — at `none` / `gzip` / `zstd` and
time all three. Adds a few minutes, and it turns run 3 from "is zstd faster" into a number you
already have a same-hardware control for.

**Where it could bite.** If the full-scale run turns out to be write-stage-dominated, gzip
plateauing at ~4 threads (measured, notes §5) becomes the ceiling on the whole pipeline, and run
1 would understate what DuckDB can do. The `outputevents` control tells you that immediately
instead of at the end of run 3. If it shows the write stage dominating, worth pulling run 3
forward rather than treating it as an epilogue.

Plain uncompressed output stays selectable throughout, per the §5 decision.

---

## 12. Risks

1. **`fhir_observation_vitalsigns.sql` (417 lines) and `fhir_observation_ed.sql` (201).** The
   `jsonb_each_text` pivot itself is solved (§1.7), but these remain the two largest and least
   regular mapping files, and the pivot makes their row counts depend on how many source columns
   are non-null. Port them early in Phase 2 and check row counts first, not last.
2. **`fhir_decimal` formatting.** §1.2 fixes the data loss but leaves `7` vs `7.0`. If anyone
   downstream diffs against published PhysioNet ndjson textually, this shows up everywhere.
   Phase 0's parity decision covers it — make it consciously.
3. **Parquet staging assumes node-local `/tmp` survives the job.** It does not survive *across*
   jobs, hence the J1/J2 split question in §10.
4. **DuckDB gets OOM-killed by Slurm, not by DuckDB.** Notes §7.2. Mitigated by sizing
   `memory_limit` from the allocation in `00_settings.sql`, but it is still the most likely
   cause of a confusing first J2 failure. Set `temp_directory` too — spilling to a default
   location on Lustre would be slow and silent.
5. **The baseline is still running.** Phase 4 blocks on it. Phases 0–3 do not.
