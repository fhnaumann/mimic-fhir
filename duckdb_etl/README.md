# DuckDB backend

Builds the MIMIC-IV-on-FHIR ndjson export with [DuckDB](https://duckdb.org)
instead of Postgres, from the same `sql/` mapping files.

The point is that `sql/` stays the single source of truth. Nothing here is a
hand-rewritten copy of a mapping: `transpile.py` rewrites the Postgres SQL into
DuckDB SQL at build time, so a change to `sql/fhir_patient.sql` reaches both
backends. The DuckDB path needs no server, no `initdb`, no bulk `\COPY`, and no
superuser — it runs against a directory of Parquet files.

## Why

The Postgres build needs a running server and a full load of MIMIC-IV before any
FHIR SQL executes. That is a hard prerequisite on a shared cluster or a laptop,
and the load dominates the wall clock. DuckDB reads Parquet in place, so the
build is a single process over a directory that can live on scratch or in a
bucket.

## Layout

| Path | What it does |
|---|---|
| `transpile.py` | Rewrites `sql/**.sql` → `build/duckdb/`. Emits `manifest.json`. |
| `stage.py` | MIMIC-IV `csv.gz` → typed Parquet. |
| `driver.py` | Runs the generated SQL and writes ndjson. |
| `parity.py` | Diffs two ndjson trees (e.g. Postgres output vs DuckDB output). |
| `runtime/*.sql` | Hand-written session settings, macros, and UDFs the rewriter targets. |

Tests live in `tests/duckdb/`.

## Requirements

```
pip install duckdb "sqlglot==30.17.0"
```

`sqlglot` is **pinned**. The rewriter depends on AST shapes that are unversioned
internals (notably that a schema-qualified call parses as
`Dot(Identifier, Anonymous)`, and that array subscripts are stored 0-based).
After any bump, re-run `transpile.py` and check the printed rewrite counts, then
re-run `tests/duckdb/`.

`stage.py` also needs a [mimic-code](https://github.com/MIT-LCP/mimic-code)
checkout, as a sibling directory or a submodule. It reads the column types from
`buildmimic/postgres/create.sql` — the same DDL `psql` is fed — so both backends
load identical types by construction rather than by inference.

## Build

Three steps. Staging is once per dataset; the other two are per build.

```bash
# 1. csv.gz -> Parquet, with types derived from mimic-code's DDL
python duckdb_etl/stage.py \
    --mimic-root /path/to/mimic-iv-2.2 \
    --mimic-root /path/to/mimic-iv-ed-2.2 \
    --out /scratch/mimic-parquet

# 2. sql/ -> build/duckdb/, checked against DuckDB's parser
python duckdb_etl/transpile.py

# 3. run it and write ndjson
python duckdb_etl/driver.py \
    --parquet /scratch/mimic-parquet \
    --out /scratch/fhir-ndjson
```

Useful flags:

- `transpile.py --explain` — additionally `EXPLAIN` every generated statement
  against an empty database, so a binder error surfaces without touching data.
- `transpile.py --only fhir_patient.sql` — one file.
- `driver.py --only patient,specimen` — subset of resources.
- `driver.py --compression zstd|gzip|none` — default `gzip`.
- `stage.py --tables patients,admissions` — subset, for a quick iteration.

## Validation

`py_mimic_fhir` accepts `DUCKDB` as a `--db_mode`, so the existing validation
path works unchanged. The database file path goes in `--dbname_mimic`; user,
password, host and port are ignored.

```bash
# materialise mimic_fhir.* tables for a patient subset, which is what
# validation samples
python duckdb_etl/driver.py --parquet /scratch/mimic-parquet \
    --db /scratch/mimic.duckdb --materialize 20

python -m py_mimic_fhir validate --db_mode DUCKDB \
    --dbname_mimic /scratch/mimic.duckdb ...
```

To compare against a Postgres build:

```bash
python duckdb_etl/parity.py /path/to/psql-ndjson /path/to/duckdb-ndjson \
    --json report.json
```

`parity.py` reports a **JSON-path histogram** rather than a list of diffs, so a
systematic difference aggregates into one row instead of a million. It compares
numerics by value (`98.6000 == 98.6`) and normalises nothing else — a missing
key, a reformatted timestamp, or a truncated decimal all show as diffs, because
those are the failure modes a port actually has.

## Design notes

**The transpiler is a rewriter, not a translator.** Every difference between the
two dialects is handled in exactly one of two places: a rewrite rule in
`transpile.py`, or a macro in `runtime/10_macros.sql`. Nowhere else. Run
`transpile.py` and it prints how many times each rule fired — an unexpected count
after a change to `sql/` is the signal to look.

**Guard rails fail the build, not the review.** The transpiler refuses to emit
SQL containing constructs it knows it cannot faithfully reproduce, and
`runtime/99_selftest.sql` asserts the invariants that would otherwise fail
silently — including that a session timezone cannot reach timestamp rendering.

**Types are derived and then asserted.** Four Postgres load-time behaviours have
no DuckDB default equivalent and all four are silent: quoted-empty-string
handling, unconstrained `NUMERIC` preserving input scale, blank-padded `CHAR(n)`,
and `TIMESTAMP(0)` rounding rather than truncating. `stage.py` documents each one
and checks it against the data instead of trusting it, because each produces a
build that completes and emits wrong JSON.

**ndjson uses a tab as the CSV quote character.** `psql` uses `E'\b'`, chosen so
its writer never quotes anything. Copying that choice into DuckDB produces a
broken file: DuckDB's CSV writer also quotes values containing `#`, which is
hardcoded. A tab sidesteps it — DuckDB always escapes a literal tab inside JSON,
and leading/trailing tabs are legal JSON whitespace, so both the quoted and
unquoted forms parse identically. `driver.py` reads every export back as ndjson
and fails the build if it does not.

**Parallel compression.** A single `COPY ... TO 'one.gz'` runs one compressor
thread regardless of allocation, which is the main reason a build underuses a
node. `driver.py` uses `PER_THREAD_OUTPUT` and byte-concatenates the parts, which
is valid for gzip (multi-member) and zstd (multi-frame) only —
`tests/duckdb/test_concat_codecs.py` verifies that against both DuckDB's reader
and each codec's own CLI. Use `--single-stream` for a like-for-like comparison
with `psql`.

## Tests

```bash
python tests/duckdb/test_functions.py      # UDFs vs recorded Postgres output
python tests/duckdb/test_parity.py         # the differ itself
python tests/duckdb/test_db_shim.py        # py_mimic_fhir DUCKDB branch
python tests/duckdb/test_concat_codecs.py  # concatenated-stream validity (slow)
```

`test_functions.py` checks the DuckDB UDFs against `golden_functions.json`,
recorded from a real Postgres instance. To re-record after changing a function:

```bash
python tests/duckdb/test_functions.py --record --pg "host=... port=5432 user=postgres"
```
