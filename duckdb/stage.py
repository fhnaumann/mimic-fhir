#!/usr/bin/env python3
"""Stage MIMIC-IV csv.gz -> Parquet with explicit types, for the DuckDB port.

    python duckdb/stage.py --mimic-root $SCRATCH/original-mimic-iv/mimic-iv-2.2 \
                           --mimic-root $SCRATCH/original-mimic-iv/mimic-iv-ed-2.2 \
                           --out /tmp/mimic-parquet

Types are DERIVED, never inferred. The DDL comes from mimic-code's
`buildmimic/postgres/create.sql` -- the same file `psql_import.sh` feeds to psql --
so both backends load identical types by construction. Inferring them would give a
VARCHAR value column sampled as DOUBLE and rejected 200 M rows in, or a timestamp
that parses on one table and not another.

Getting the types right is necessary but NOT sufficient. Four Postgres load-time
behaviours have no DuckDB default equivalent, and each one is silent:

  1. QUOTED EMPTY STRINGS.  psql loads with `NULL ''`, where an unquoted empty
     field is NULL but a quoted `""` is the empty string. DuckDB's default
     `allow_quoted_nulls=true` makes BOTH null. Measured, and it is not academic:
     a nulled field is dropped by strip() and the FHIR key vanishes, while psql
     emits `""`. We set allow_quoted_nulls=false.

  2. UNCONSTRAINED `NUMERIC` PRESERVES THE INPUT'S SCALE.  Postgres stores
     '3.0000' as 3.0000 and '2' as 2, and `jsonb_each_text` hands back exactly
     that text. No DuckDB numeric type does this -- DOUBLE gives 3.0, and
     DECIMAL(18,4) also renders 3.0 through to_json. Where such a column reaches
     output as TEXT it must be staged VARCHAR. See PIVOT_NUMERIC.

  3. `CHAR(n)` IS BLANK-PADDED, and the padding is visible in JSON but NOT in
     concatenation.  Measured in Postgres 18.4: `to_jsonb('4019'::char(7))` is
     `"4019   "`, while `'X-' || '4019'::char(7)` is `X-4019` (the bpchar->text
     cast strips). So a CHAR column can be safe at one use site and wrong at
     another. See CHAR_REVIEW.

  4. `TIMESTAMP(0)` ROUNDS, it does not truncate.  Postgres rounds .7 up to the
     next second; `fhir_instant` renders with %S and truncates. They agree only
     if the source has no sub-second component at all -- which is what
     assert_no_subseconds() checks, and what licenses fhir_instant.

Everything above is asserted at stage time rather than trusted, because all four
produce a build that runs to completion and emits wrong JSON.

Usage:
    python duckdb/stage.py --mimic-root DIR [--mimic-root DIR ...] --out DIR
    python duckdb/stage.py ... --tables patients,admissions,transfers
    python duckdb/stage.py ... --no-assert        # skip the scans, for a quick retry
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import json
import pathlib
import re
import sys
import threading
import time

REPO = pathlib.Path(__file__).resolve().parent.parent
SQL = REPO / "sql"

# mimic-code checkout. Sibling of this repo locally; a submodule path on the HPC
# (PSQL_BUILD_HPC.md 3). Both are tried before giving up.
MIMIC_CODE_CANDIDATES = [
    REPO / "mimic-code",
    REPO.parent / "mimic-code",
]
CREATE_SQL = [
    "mimic-iv/buildmimic/postgres/create.sql",
    "mimic-iv-ed/buildmimic/postgres/create.sql",
]
LOAD_SQL = [
    "mimic-iv/buildmimic/postgres/load_gz.sql",
    "mimic-iv-ed/buildmimic/postgres/load_gz.sql",
]


# --------------------------------------------------------------------------
# Type mapping.
#
# Deliberately total and deliberately small: an unrecognised Postgres type is an
# error, not a guess. Adding a type here should mean someone checked how it
# renders in JSON on both sides.
# --------------------------------------------------------------------------

TYPE_MAP = {
    "SMALLINT": "SMALLINT",
    "INTEGER": "INTEGER",
    "INT": "INTEGER",
    "BIGINT": "BIGINT",
    "REAL": "DOUBLE",
    "FLOAT": "DOUBLE",
    "DOUBLE PRECISION": "DOUBLE",
    "TEXT": "VARCHAR",
    "DATE": "DATE",
    "TIMESTAMP": "TIMESTAMP",
}


# The exact dialect psql's `\COPY ... DELIMITER ',' CSV HEADER NULL ''` implies.
# Every option is stated because every default here has bitten:
#
#   auto_detect=false     the sniffer overrides declared types (it widens INTEGER
#                         to BIGINT) and mis-detects the dialect; we have the DDL,
#                         so there is nothing to sniff.
#   escape='"'            MIMIC contains `"1"" Packing"` (chartevents itemid 228529).
#                         Postgres CSV treats "" as a literal quote; DuckDB's
#                         sniffer detected escape as EMPTY and rejected the row.
#   allow_quoted_nulls=false   Postgres `NULL ''` makes an UNQUOTED empty field NULL
#                         but keeps a quoted "" as the empty string. DuckDB's
#                         default nulls both.
CSV_DIALECT = ("header=true, delim=',', quote='\"', escape='\"', "
               "nullstr='', allow_quoted_nulls=false")


def map_type(pg: str, table: str, col: str) -> str:
    """Postgres declared type -> DuckDB type. Raises on anything unreviewed."""
    t = pg.strip().upper()
    t = re.sub(r"\s+NOT\s+NULL$", "", t).strip()

    key = f"{table}.{col}"
    if key in PIVOT_NUMERIC:
        return PIVOT_NUMERIC[key][0]

    # VARCHAR(n) and CHAR(n) both become VARCHAR. For CHAR that is a deliberate
    # decision per column, recorded in CHAR_REVIEW and asserted against the data.
    m = re.fullmatch(r"(VARCHAR|CHARACTER VARYING)\s*\(\d+\)", t)
    if m or t == "VARCHAR":
        return "VARCHAR"
    if re.fullmatch(r"(CHAR|CHARACTER|BPCHAR)\s*\(\d+\)", t):
        return "VARCHAR"

    # TIMESTAMP(0) and bare TIMESTAMP differ in Postgres (the first rounds to the
    # second, the second keeps microseconds). Both map to DuckDB TIMESTAMP; what
    # makes that safe is assert_no_subseconds(), not the mapping.
    if re.fullmatch(r"TIMESTAMP\s*\(\d+\)", t):
        return "TIMESTAMP"

    m = re.fullmatch(r"(NUMERIC|DECIMAL)\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)", t)
    if m:
        return f"DECIMAL({m.group(2)},{m.group(3)})"
    if t in ("NUMERIC", "DECIMAL"):
        # Unconstrained. DOUBLE is right wherever the value reaches JSON as a
        # NUMBER (parity.py compares numerics by value, so 98.6000 == 98.6).
        # It is wrong wherever the value reaches JSON as TEXT -- those columns
        # are enumerated in PIVOT_NUMERIC and handled above.
        return "DOUBLE"

    if t in TYPE_MAP:
        return TYPE_MAP[t]
    raise SystemExit(f"unreviewed Postgres type {pg!r} on {table}.{col} -- "
                     f"add it to TYPE_MAP only after checking how it renders in JSON")


# --------------------------------------------------------------------------
# Reviewed exceptions.
#
# Both dicts are checked for EXACT coverage against what is actually in the DDL
# and the mapping SQL. A new pivot key or a new CHAR column fails the build until
# someone decides what it should do -- the same discipline as
# REVIEWED_INSTANT_COMPARISONS in transpile.py.
# --------------------------------------------------------------------------

# Unconstrained-NUMERIC columns exposed through a `jsonb_each_text(to_jsonb(t))`
# pivot. value -> (duckdb type, reason).
PIVOT_NUMERIC = {
    # The one column whose pivoted TEXT reaches output verbatim, as a valueString
    # (fhir_observation_ed.sql:193). The full ED extract writes acuity as
    # '3.0000' and the demo writes '3', and Postgres NUMERIC preserves both
    # exactly; only VARCHAR reproduces that. DOUBLE would emit '3.0' on the full
    # data AND '2.0' on the demo, so this is caught by the demo gate.
    "mimiciv_ed.triage.acuity":
        ("VARCHAR", "reaches valueString verbatim; NUMERIC keeps the CSV scale"),

    # Everything below is pivoted, then explicitly CAST(... AS NUMERIC) into a
    # valueQuantity.value -- a JSON number, which parity.py compares by value.
    # DOUBLE is fine; the scale difference is not observable.
    "mimiciv_ed.triage.temperature": ("DOUBLE", "-> valueQuantity number"),
    "mimiciv_ed.triage.heartrate":   ("DOUBLE", "-> valueQuantity number"),
    "mimiciv_ed.triage.resprate":    ("DOUBLE", "-> valueQuantity number"),
    "mimiciv_ed.triage.o2sat":       ("DOUBLE", "-> valueQuantity number"),
    "mimiciv_ed.triage.dbp":         ("DOUBLE", "-> valueQuantity number"),
    "mimiciv_ed.vitalsign.temperature": ("DOUBLE", "-> valueQuantity number"),
    "mimiciv_ed.vitalsign.heartrate":   ("DOUBLE", "-> valueQuantity number"),
    "mimiciv_ed.vitalsign.o2sat":       ("DOUBLE", "-> valueQuantity number"),
}

# CHAR(n) columns. value -> (check, reason). `check` is one of:
#   "exact"   assert every non-null value is exactly n chars, so blank-padding is
#             a no-op and staging as VARCHAR is byte-identical to Postgres.
#   "trimmed" the mapping TRIMs (or text-casts, which strips) at every use site,
#             so the padding is never observable. Only the whitespace assertion
#             applies.
CHAR_REVIEW = {
    "mimiciv_hosp.hcpcsevents.hcpcs_cd":
        ("exact", "emitted UNTRIMMED into jsonb_build_object (fhir_encounter.sql:34); "
                  "measured 150,771/150,771 values are exactly 5 chars"),
    "mimiciv_hosp.patients.gender":
        ("exact", "emitted into the Patient resource; CHAR(1) cannot pad"),
    "mimiciv_hosp.diagnoses_icd.icd_code":
        ("trimmed", "TRIM() at every output/join site; the uuid_generate_v5 "
                    "concatenation goes through a bpchar->text cast, which strips"),
    "mimiciv_hosp.d_icd_diagnoses.icd_code":
        ("trimmed", "join key only, TRIM()ed"),
    "mimiciv_hosp.d_icd_procedures.icd_code":
        ("trimmed", "join key only, TRIM()ed"),
}


# --------------------------------------------------------------------------
# Parsing mimic-code
# --------------------------------------------------------------------------

def find_mimic_code() -> pathlib.Path:
    for c in MIMIC_CODE_CANDIDATES:
        if (c / CREATE_SQL[0]).exists():
            return c
    raise SystemExit(
        "cannot find a mimic-code checkout with "
        f"{CREATE_SQL[0]}; looked in " + ", ".join(str(c) for c in MIMIC_CODE_CANDIDATES))


CREATE_RE = re.compile(r"CREATE\s+TABLE\s+([a-z_0-9]+\.[a-z_0-9]+)\s*\((.*?)\)\s*;",
                       re.S | re.I)
COL_RE = re.compile(r"^\s*([a-z_0-9]+)\s+(.+?)\s*$", re.I)


def parse_create(root: pathlib.Path) -> dict[str, list[tuple[str, str]]]:
    """schema.table -> [(column, postgres type)], in DDL order.

    Column ORDER matters and is not cosmetic: `to_jsonb(t)` emits keys in column
    order, and the pivot in fhir_observation_ed reads that. Staging must preserve
    it, which is why read_csv gets an ordered `columns` struct rather than a
    projection.
    """
    out: dict[str, list[tuple[str, str]]] = {}
    for rel in CREATE_SQL:
        text = (root / rel).read_text()
        text = re.sub(r"--[^\n]*", "", text)          # strip line comments
        for table, body in CREATE_RE.findall(text):
            cols = []
            for line in body.split(","):
                # NUMERIC(10, 4) splits on its own comma; rejoin by requiring a
                # leading identifier and re-attaching orphan fragments.
                m = COL_RE.match(line)
                if not m:
                    if cols:                            # e.g. " 4)" from NUMERIC(10, 4)
                        name, typ = cols[-1]
                        cols[-1] = (name, f"{typ},{line.strip()}")
                    continue
                cols.append((m.group(1).lower(), m.group(2).strip()))
            out[table.lower()] = cols
    return out


LOAD_RE = re.compile(r"\\copy\s+([a-z_0-9.]+)\s+from\s+program\s+"
                     r"'gzip\s+-dc\s+([^']+)'", re.I)
SEARCH_PATH_RE = re.compile(r"SET\s+search_path\s+TO\s+([a-z_0-9]+)\s*;", re.I)


def parse_load(root: pathlib.Path) -> dict[str, str]:
    """schema.table -> csv.gz basename, from the same load script psql uses.

    The two load scripts do not agree with each other. mimic-iv qualifies the
    target (`\\COPY mimiciv_hosp.labevents`); mimic-iv-ed sets `search_path` once
    and then writes bare table names in lower case. Both forms are handled, which
    is the whole reason for parsing this file instead of assuming a convention.
    """
    out = {}
    for rel in LOAD_SQL:
        text = (root / rel).read_text()
        schema = None
        for line in text.splitlines():
            m = SEARCH_PATH_RE.search(line)
            if m and not line.lstrip().startswith("--"):
                schema = m.group(1).lower()
            m = LOAD_RE.search(line)
            if not m:
                continue
            table = m.group(1).lower()
            if "." not in table:
                if schema is None:
                    continue
                table = f"{schema}.{table}"
            out[table] = m.group(2)
    return out


# --------------------------------------------------------------------------
# What the mapping SQL actually needs
# --------------------------------------------------------------------------

def source_tables() -> set[str]:
    """The mimiciv_* tables referenced by sql/, from the AST.

    Parsed rather than hardcoded so the list cannot drift, and via the AST rather
    than grep because a grep counts `mimiciv_hosp.diagnosis_icd` in a COMMENT --
    which is how the plan's census came to say 31 tables instead of 30.
    """
    try:
        import sqlglot
        from sqlglot import exp
    except ImportError:
        raise SystemExit("sqlglot is required: pip install sqlglot==30.17.0")

    found = set()
    for p in sorted(SQL.rglob("*.sql")):
        try:
            trees = sqlglot.parse(p.read_text(), read="postgres")
        except Exception:
            continue
        for t in trees:
            if t is None:
                continue
            for tbl in t.find_all(exp.Table):
                db = (tbl.text("db") or "").lower()
                if db.startswith("mimiciv"):
                    found.add(f"{db}.{tbl.name.lower()}")
    return found


def referenced_columns(schemas: dict) -> set[str]:
    """{schema.table.column} for every source column sql/ actually reads.

    Attribution is by FILE-LOCAL alias, not by sqlglot's scope graph. The scope
    graph is the obvious tool and it does not work here: these files parse to a
    `Block` (or an `Insert`), `build_scope` returns None for both, and the result
    is a set that resolves almost nothing and a gate that passes vacuously.

    So: collect every `mimiciv_*` table in the file with its alias, then attribute
    `alias.column`. A column with no qualifier, or one whose alias belongs to a CTE
    that shadows a table alias, is attributed to EVERY mimiciv table in the file.
    That over-includes, which is the right direction for a gate -- the cost of a
    false positive is one reviewed exception, the cost of a false negative is a
    silent build.
    """
    try:
        import sqlglot
        from sqlglot import exp
    except ImportError:
        raise SystemExit("sqlglot is required: pip install sqlglot==30.17.0")

    refs: set[str] = set()
    whole_row: set[str] = set()
    for p in sorted(SQL.rglob("*.sql")):
        try:
            trees = sqlglot.parse(p.read_text(), read="postgres")
        except Exception:
            continue

        aliases: dict[str, str] = {}
        tables: set[str] = set()
        columns: list[tuple[str, str]] = []
        for tree in trees:
            if tree is None:
                continue
            for tbl in tree.find_all(exp.Table):
                db = (tbl.text("db") or "").lower()
                if not db.startswith("mimiciv"):
                    continue
                qname = f"{db}.{tbl.name.lower()}"
                tables.add(qname)
                aliases[(tbl.alias or tbl.name).lower()] = qname
            for col in tree.find_all(exp.Column):
                columns.append(((col.text("table") or "").lower(), col.name.lower()))

        for qual, name in columns:
            if qual and qual in aliases:
                refs.add(f"{aliases[qual]}.{name}")
            else:
                for t in tables:
                    refs.add(f"{t}.{name}")

        # `to_jsonb(t)` reads the WHOLE row, so every column of a pivoted table is
        # read even though none of them appears as a Column node. Without this the
        # gate would miss a timestamp that only ever reaches output through a pivot.
        for m in re.finditer(r"to_jsonb\s*\(\s*(\w+)\s*\)", " ".join(
                t.sql(dialect="postgres") for t in trees if t is not None), re.I):
            tbl = aliases.get(m.group(1).lower())
            if tbl:
                whole_row.add(tbl)
    for tbl in whole_row:
        for c, _ in schemas.get(tbl, []):
            refs.add(f"{tbl}.{c}")
    return refs


PIVOT_RE = re.compile(
    r"jsonb_each_text\s*\(\s*to_jsonb\s*\(\s*(\w+)\s*\)\s*\).*?KEY\s+IN\s*\(([^)]*)\)",
    re.S | re.I)
ALIAS_RE = re.compile(r"(mimiciv_\w+\.\w+)\s+(\w+)\s*,\s*jsonb_each_text", re.I)


def pivot_exposed() -> set[str]:
    """{schema.table.column} for every column a jsonb_each_text pivot reads.

    These are the columns whose Postgres TEXT rendering becomes observable, which
    is what makes the NUMERIC scale question (note 2) matter at all.
    """
    exposed = set()
    for p in sorted(SQL.rglob("*.sql")):
        text = p.read_text()
        aliases = {a.lower(): t.lower() for t, a in ALIAS_RE.findall(text)}
        for alias, keys in PIVOT_RE.findall(text):
            table = aliases.get(alias.lower())
            if not table:
                continue
            for k in re.findall(r"'([^']+)'", keys):
                exposed.add(f"{table}.{k.lower()}")
    return exposed


def check_reviews(schemas: dict, wanted: set[str]) -> None:
    """Fail the build if a reviewed exception list has drifted from reality."""
    types = {f"{t}.{c}": ty for t, cols in schemas.items() for c, ty in cols}

    # 1. Every pivot-exposed unconstrained-NUMERIC column must be reviewed.
    unconstrained = set()
    for key in pivot_exposed():
        table = key.rsplit(".", 1)[0]
        if table not in wanted:
            continue
        ty = (types.get(key) or "").strip().upper()
        ty = re.sub(r"\s+NOT\s+NULL$", "", ty)
        if ty in ("NUMERIC", "DECIMAL"):
            unconstrained.add(key)
    missing = unconstrained - set(PIVOT_NUMERIC)
    if missing:
        raise SystemExit(
            "unreviewed pivot-exposed NUMERIC column(s): " + ", ".join(sorted(missing))
            + "\nPostgres NUMERIC preserves the CSV's scale through jsonb_each_text. "
              "Decide per column whether the pivoted text reaches output verbatim "
              "(-> VARCHAR) or is cast to a JSON number (-> DOUBLE), then add it to "
              "PIVOT_NUMERIC with the reason.")
    stale = set(PIVOT_NUMERIC) - unconstrained
    if stale:
        print(f"  note: PIVOT_NUMERIC has {len(stale)} entry(s) no longer pivot-exposed: "
              + ", ".join(sorted(stale)))

    # 2. Every CHAR(n) column on a staged table must be reviewed.
    chars = {f"{t}.{c}" for t, cols in schemas.items() if t in wanted
             for c, ty in cols
             if re.match(r"^(CHAR|CHARACTER|BPCHAR)\s*\(", ty.strip(), re.I)}
    missing = chars - set(CHAR_REVIEW)
    if missing:
        raise SystemExit(
            "unreviewed CHAR(n) column(s): " + ", ".join(sorted(missing))
            + "\nPostgres blank-pads CHAR and the padding IS visible in to_jsonb but "
              "NOT in concatenation. Check every use site, then add to CHAR_REVIEW "
              "as 'exact' (padding unreachable) or 'trimmed' (never observable).")


# --------------------------------------------------------------------------
# Staging
# --------------------------------------------------------------------------

def csv_path(roots: list[pathlib.Path], schema: str, fname: str) -> pathlib.Path | None:
    sub = {"mimiciv_hosp": "hosp", "mimiciv_icu": "icu", "mimiciv_ed": "ed"}[schema]
    for root in roots:
        for cand in (root / sub / fname, root / fname):
            if cand.exists():
                return cand
        hits = sorted(root.rglob(f"{sub}/{fname}"))
        if hits:
            return hits[0]
    return None


def stage_one(con, table: str, cols: list[tuple[str, str]], src: pathlib.Path,
              out_dir: pathlib.Path, compression: str) -> dict:
    schema, name = table.split(".")
    dest = out_dir / schema / f"{name}.parquet"
    dest.parent.mkdir(parents=True, exist_ok=True)

    struct = ", ".join(f"'{c}': '{map_type(ty, table, c)}'" for c, ty in cols)
    # Column ORDER is preserved (to_jsonb key order is column order), names come
    # from the DDL, and header=true means the CSV's own header is skipped rather
    # than trusted -- a renamed column in a future release becomes a type error
    # here instead of a silently shifted column.
    reader = (f"read_csv('{src}', columns={{{struct}}}, {CSV_DIALECT}, "
              f"auto_detect=false)")

    t0 = time.time()
    con.execute(f"COPY (SELECT * FROM {reader}) TO '{dest}' "
                f"(FORMAT parquet, COMPRESSION {compression})")
    elapsed = time.time() - t0
    rows = con.execute(f"SELECT count(*) FROM read_parquet('{dest}')").fetchone()[0]
    return {"table": table, "source": str(src), "parquet": str(dest),
            "rows": rows, "seconds": round(elapsed, 2),
            "bytes": dest.stat().st_size}


def assert_rowcount(con, table: str, src: pathlib.Path, rows: int) -> list[str]:
    """Parquet row count must match the CSV's own line count.

    Counts records, not newlines: MIMIC free-text columns (comments, chiefcomplaint)
    contain embedded newlines inside quotes, so `zcat | wc -l` disagrees with both
    backends and is not the check to write.
    """
    csv_rows = con.execute(
        f"SELECT count(*) FROM read_csv('{src}', all_varchar=true, {CSV_DIALECT})"
    ).fetchone()[0]
    if csv_rows != rows:
        return [f"{table}: staged {rows:,} rows but the CSV has {csv_rows:,}"]
    return []


def assert_no_subseconds(con, table: str, cols: list[tuple[str, str]],
                         dest: str, used: set[str]) -> list[str]:
    """No timestamp may carry a non-zero fractional second.

    This is the assertion that LICENSES fhir_instant's '%Y-%m-%dT%H:%M:%S+00:00'.
    Two independent things break if it fails, in opposite directions: Postgres
    TIMESTAMP(0) ROUNDS .7 up to the next second while strftime truncates it down,
    and Postgres bare TIMESTAMP keeps the fraction and renders it into the JSON
    where fhir_instant drops it.
    """
    ts = [c for c, ty in cols
          if map_type(ty, table, c) == "TIMESTAMP" and f"{table}.{c}" in used]
    if not ts:
        return []
    # NOT date_part('microsecond', ...): that follows the Postgres convention of
    # returning seconds*1e6 + microseconds, so it is non-zero for any timestamp
    # whose SECONDS field is set, and flags most of MIMIC. date_trunc says what
    # is actually meant.
    checks = " OR ".join(f"(\"{c}\" IS NOT NULL AND date_trunc('second', \"{c}\") <> \"{c}\")"
                         for c in ts)
    bad = con.execute(f"SELECT count(*) FROM read_parquet('{dest}') "
                      f"WHERE {checks}").fetchone()[0]
    if bad:
        return [f"{table}: {bad:,} row(s) have a non-zero sub-second timestamp -- "
                f"fhir_instant truncates and Postgres TIMESTAMP(0) rounds, so these "
                f"WILL diverge (columns checked: {', '.join(ts)})"]
    return []


def assert_char(con, table: str, cols: list[tuple[str, str]], dest: str) -> list[str]:
    """CHAR(n) staged as VARCHAR: verify the review's premise against the data."""
    problems = []
    for c, ty in cols:
        m = re.match(r"^(?:CHAR|CHARACTER|BPCHAR)\s*\((\d+)\)", ty.strip(), re.I)
        if not m:
            continue
        n = int(m.group(1))
        decision, _ = CHAR_REVIEW[f"{table}.{c}"]

        # Trailing whitespace in the SOURCE breaks the 'trimmed' argument too:
        # Postgres strips it on the bpchar->text cast used by uuid_generate_v5,
        # DuckDB VARCHAR would keep it, and every generated id would differ.
        ws = con.execute(f"SELECT count(*) FROM read_parquet('{dest}') "
                         f"WHERE \"{c}\" <> trim(\"{c}\")").fetchone()[0]
        if ws:
            problems.append(f"{table}.{c}: {ws:,} value(s) carry surrounding "
                            f"whitespace; Postgres strips it on the bpchar->text "
                            f"cast, DuckDB VARCHAR would not")
        if decision == "exact":
            bad = con.execute(f"SELECT count(*) FROM read_parquet('{dest}') "
                              f"WHERE \"{c}\" IS NOT NULL AND length(\"{c}\") <> {n}"
                              ).fetchone()[0]
            if bad:
                problems.append(
                    f"{table}.{c}: {bad:,} value(s) shorter than CHAR({n}); Postgres "
                    f"blank-pads them and emits the padding in JSON, DuckDB VARCHAR "
                    f"will not. Re-review this entry in CHAR_REVIEW")
    return problems


# --------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mimic-root", action="append", required=True,
                    help="directory containing hosp/ icu/ or ed/; repeatable")
    ap.add_argument("--out", required=True, help="Parquet output root")
    ap.add_argument("--tables", help="comma-separated bare table names to stage")
    ap.add_argument("--compression", default="zstd",
                    help="Parquet compression (default zstd -- this is the STAGING "
                         "format, not the ndjson export, so it is not the gzip/zstd "
                         "benchmark variable)")
    ap.add_argument("--threads", type=int)
    ap.add_argument("--memory-limit")
    ap.add_argument("--jobs", type=int, default=4,
                    help="tables staged concurrently (default 4). gzip is not "
                         "seekable so each table is single-threaded; this is where "
                         "the parallelism comes from")
    ap.add_argument("--no-assert", action="store_true")
    args = ap.parse_args()

    try:
        import duckdb
    except ImportError:
        raise SystemExit("duckdb is required: pip install duckdb")
    if not hasattr(duckdb, "connect"):
        raise SystemExit("`import duckdb` found this repo's duckdb/ namespace package "
                         "instead of the PyPI module (HANDOFF.md trap 16) -- run from "
                         "outside the repo root or install duckdb")

    roots = [pathlib.Path(r).resolve() for r in args.mimic_root]
    for r in roots:
        if not r.exists():
            raise SystemExit(f"no such --mimic-root: {r}")
    out_dir = pathlib.Path(args.out).resolve()

    mc = find_mimic_code()
    schemas = parse_create(mc)
    loads = parse_load(mc)
    wanted = source_tables()
    print(f"mimic-code:  {mc}")
    print(f"needed:      {len(wanted)} source tables referenced by sql/")

    check_reviews(schemas, wanted)
    used = referenced_columns(schemas)
    print(f"columns read: {len(used)} source columns referenced by sql/")

    if args.tables:
        keep = {t.strip().lower() for t in args.tables.split(",")}
        wanted = {t for t in wanted if t.split(".")[1] in keep}
        print(f"--tables:    staging {len(wanted)} of them")

    con = duckdb.connect()
    if args.threads:
        con.execute(f"SET threads={args.threads}")
    if args.memory_limit:
        con.execute(f"SET memory_limit='{args.memory_limit}'")
    con.execute("SET TimeZone='UTC'")

    staged, problems, missing = [], [], []
    work = []
    for table in sorted(wanted):
        schema = table.split(".")[0]
        cols = schemas.get(table)
        if cols is None:
            problems.append(f"{table}: no CREATE TABLE in mimic-code")
            continue
        fname = loads.get(table)
        if fname is None:
            problems.append(f"{table}: no \\COPY line in load_gz.sql")
            continue
        src = csv_path(roots, schema, fname)
        if src is None:
            missing.append(f"{table} ({fname})")
            continue
        work.append((table, cols, src))

    # Biggest file first. A .csv.gz is not seekable, so DuckDB reads each one on a
    # SINGLE thread no matter how many are available -- all the parallelism here is
    # across tables, and the run cannot finish before its largest table does.
    # Starting labevents last would leave it running alone at the end.
    work.sort(key=lambda w: -w[2].stat().st_size)

    lock = threading.Lock()

    def run(item):
        table, cols, src = item
        # A cursor per thread: one duckdb connection is not safe to use
        # concurrently, but cursor() hands out an independent one on the same db.
        cur = con.cursor()
        try:
            rec = stage_one(cur, table, cols, src, out_dir, args.compression)
        except Exception as e:
            return table, None, [f"{table}: {type(e).__name__}: {e}"]
        probs = []
        if not args.no_assert:
            probs += assert_rowcount(cur, table, src, rec["rows"])
            probs += assert_no_subseconds(cur, table, cols, rec["parquet"], used)
            probs += assert_char(cur, table, cols, rec["parquet"])
        return table, rec, probs

    with cf.ThreadPoolExecutor(max_workers=max(1, args.jobs)) as pool:
        for table, rec, probs in pool.map(run, work):
            with lock:
                problems += probs
                if rec is None:
                    continue
                staged.append(rec)
                print(f"  {table:<38} {rec['rows']:>12,} rows  "
                      f"{rec['bytes'] / 1e6:>9.1f} MB  {rec['seconds']:>6.1f}s",
                      flush=True)

    if missing:
        print(f"\n  ! not found under {', '.join(str(r) for r in roots)}:")
        for m in missing:
            print(f"      {m}")

    manifest = out_dir / "stage_manifest.json"
    manifest.write_text(json.dumps({
        "mimic_code": str(mc),
        "roots": [str(r) for r in roots],
        "compression": args.compression,
        "tables": {r["table"]: r for r in staged},
        "total_rows": sum(r["rows"] for r in staged),
        "total_bytes": sum(r["bytes"] for r in staged),
    }, indent=2))

    print(f"\n{len(staged)}/{len(wanted)} tables staged, "
          f"{sum(r['rows'] for r in staged):,} rows, "
          f"{sum(r['bytes'] for r in staged) / 1e9:.2f} GB")
    print(f"manifest: {manifest}")

    if problems:
        print(f"\nPROBLEMS ({len(problems)}):")
        for p in problems:
            print(f"  ! {p}")
        return 1
    if missing:
        return 1
    print("all assertions pass")
    return 0


if __name__ == "__main__":
    sys.exit(main())
