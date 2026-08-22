#!/usr/bin/env python3
"""Differential test for the five ported plpgsql functions.

The functions in sql/fn/ build JSON by string concatenation with manual comma
placement, and several collapse to NULL through `||` when a guard CASE has no ELSE.
That behaviour is easy to break and hard to notice, so this test pins it by running
the ORIGINAL plpgsql in Postgres and the ported macros in DuckDB over the same input
matrix, and comparing parsed JSON.

Two modes:

  --record   needs a Postgres with sql/fn/ loaded; writes golden_functions.json
  (default)  no Postgres needed; asserts DuckDB against the committed golden file

Record mode is how the golden file is produced or refreshed. Default mode is what
runs on the HPC and in CI, where there is no Postgres.

  python3 duckdb/tests/test_functions.py --record --pg "host=/tmp/s port=5432 user=postgres"
  python3 duckdb/tests/test_functions.py
"""
import argparse
import json
import pathlib
import subprocess
import sys

REPO = pathlib.Path(__file__).resolve().parents[2]
GOLDEN = pathlib.Path(__file__).parent / "golden_functions.json"

# ---------------------------------------------------------------------------
# Input matrix. Each entry is (function, [args...]). NULL is represented by None.
#
# Cases are chosen to hit the guard boundaries, not to be realistic: the '0' and ''
# sentinels, absent lookup rows, and every combination that can null out a segment.
# ---------------------------------------------------------------------------
CASES = [
    # fn_patient_extension(race, ethnicity, birthsex)
    #   'WHITE' is in both map_ethnicity and map_race_omb
    #   'UNKNOWN' is in map_ethnicity with a NULL fhir_ethnicity_code
    #   'PORTUGUESE' is in map_ethnicity; checks the map_race_omb LEFT JOIN miss
    ("fn_patient_extension", ["WHITE", "WHITE", "M"]),
    ("fn_patient_extension", ["WHITE", "WHITE", None]),
    ("fn_patient_extension", ["WHITE", "WHITE", "F"]),
    ("fn_patient_extension", [None, "WHITE", "M"]),
    ("fn_patient_extension", ["WHITE", None, "M"]),
    ("fn_patient_extension", [None, None, "M"]),
    ("fn_patient_extension", [None, None, None]),
    ("fn_patient_extension", ["UNKNOWN", "UNKNOWN", "F"]),
    ("fn_patient_extension", ["UNKNOWN", "UNKNOWN", None]),
    ("fn_patient_extension", ["PORTUGUESE", "PORTUGUESE", "M"]),
    ("fn_patient_extension", ["NOT A REAL ETHNICITY", "NOT A REAL ETHNICITY", "M"]),
    ("fn_patient_extension", ["HISPANIC OR LATINO", "HISPANIC OR LATINO", "F"]),

    # fn_med_statement(med_GSN, med_NDC, med_ETC_CODES)
    #   med_GSN = '0' must collapse the WHOLE call to NULL
    ("fn_med_statement", ["0", "123", "[null]"]),
    ("fn_med_statement", ["0", "0", "[null]"]),
    ("fn_med_statement", ["123", "0", "[null]"]),
    ("fn_med_statement", ["123", "456", "[null]"]),
    ("fn_med_statement", ["123", "456", '{"code":"E1","system":"s"}']),
    ("fn_med_statement", ["123", "0", '{"code":"E1","system":"s"},{"code":"E2","system":"s"}']),
    ("fn_med_statement", [None, "456", "[null]"]),
    ("fn_med_statement", ["123", None, "[null]"]),
    ("fn_med_statement", ["123", "456", None]),

    # fn_build_medication_identifier(ndc, formulary_drug_cd, drug)
    ("fn_build_medication_identifier", ["123", "ABC", "Aspirin"]),
    ("fn_build_medication_identifier", [None, "ABC", "Aspirin"]),
    ("fn_build_medication_identifier", ["0", "ABC", "Aspirin"]),
    ("fn_build_medication_identifier", ["", "ABC", "Aspirin"]),
    ("fn_build_medication_identifier", ["123", None, "Aspirin"]),
    ("fn_build_medication_identifier", ["123", "", "Aspirin"]),
    ("fn_build_medication_identifier", ["123", "ABC", ""]),
    ("fn_build_medication_identifier", ["0", "", ""]),
    ("fn_build_medication_identifier", [None, None, None]),
    ("fn_build_medication_identifier", ['quo"te', "a,b", "back\\slash"]),

    # fn_prescriptions_medication_code(ndc, formulary_drug_cd, drug)
    ("fn_prescriptions_medication_code", ["123", "ABC", "Aspirin"]),
    ("fn_prescriptions_medication_code", ["0", "A  B", "Aspirin"]),
    ("fn_prescriptions_medication_code", ["", None, "Multi   space   drug"]),
    ("fn_prescriptions_medication_code", [None, None, "  trim me  "]),
    ("fn_prescriptions_medication_code", [None, "  pad  ", "x"]),
]


def lit(v):
    """SQL literal. Both engines use the same single-quote escaping."""
    if v is None:
        return "NULL"
    return "'" + v.replace("'", "''") + "'"


def build_query(qualify):
    """One SELECT emitting a JSON array of every case's result, in CASES order."""
    parts = []
    for fn, args in CASES:
        call = f"{qualify}{fn}({', '.join(lit(a) for a in args)})"
        parts.append(f"({call})::VARCHAR")
    return "SELECT json_array(" + ", ".join(parts) + ")::VARCHAR AS r;"


def run_postgres(dsn):
    sql = []
    sql.append("SET TimeZone='UTC';")
    sql.append("CREATE SCHEMA IF NOT EXISTS fhir_etl;")
    for f in ["fhir_etl/map_ethnicity.sql", "fhir_etl/map_race_omb.sql"]:
        sql.append((REPO / "sql" / f).read_text())
        sql.append(";")
    for f in sorted((REPO / "sql" / "fn").glob("*.sql")):
        if "create_table_patient_dependent" in f.name:
            continue
        sql.append(f.read_text())
    # Postgres has no json_array(); build the array with jsonb_build_array
    q = build_query("fhir_etl.").replace("json_array(", "jsonb_build_array(", 1)
    sql.append(q)
    out = subprocess.run(
        ["psql", dsn, "-X", "-q", "-A", "-t", "-v", "ON_ERROR_STOP=1", "-f", "-"],
        input="\n".join(sql), capture_output=True, text=True,
    )
    if out.returncode != 0:
        sys.exit(f"postgres failed:\n{out.stderr}")
    return json.loads(out.stdout.strip().splitlines()[-1])


def run_duckdb():
    rt = REPO / "duckdb" / "runtime"
    sql = ["SET TimeZone='UTC';", "CREATE SCHEMA IF NOT EXISTS fhir_etl;"]
    sql.append((rt / "10_macros.sql").read_text())
    for f in ["fhir_etl/map_ethnicity.sql", "fhir_etl/map_race_omb.sql"]:
        sql.append((REPO / "sql" / f).read_text())
        sql.append(";")
    sql.append((rt / "20_functions.sql").read_text())
    sql.append(build_query(""))
    out = subprocess.run(
        ["duckdb", "-init", "/dev/null", "-noheader", "-list"],
        input="\n".join(sql), capture_output=True, text=True,
    )
    if out.returncode != 0:
        sys.exit(f"duckdb failed:\n{out.stdout}\n{out.stderr}")
    return json.loads(out.stdout.strip().splitlines()[-1])


def norm(v):
    """Compare JSON semantically: key order and numeric formatting differ by engine."""
    if v is None:
        return None
    try:
        return json.loads(v)
    except (json.JSONDecodeError, TypeError):
        return v


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--record", action="store_true",
                    help="regenerate golden_functions.json from Postgres")
    ap.add_argument("--pg", default="", help="psql connection string")
    args = ap.parse_args()

    if args.record:
        pg = run_postgres(args.pg)
        GOLDEN.write_text(json.dumps(
            {"cases": [[f, a] for f, a in CASES], "expected": pg}, indent=1) + "\n")
        print(f"recorded {len(pg)} results from Postgres -> {GOLDEN.name}")

    if not GOLDEN.exists():
        sys.exit("no golden file; run with --record against a Postgres first")

    golden = json.loads(GOLDEN.read_text())
    if [list(c) for c in golden["cases"]] != [[f, a] for f, a in CASES]:
        sys.exit("CASES has changed since the golden file was recorded; re-record it")

    got = run_duckdb()
    fails = []
    for (fn, argv), want, have in zip(CASES, golden["expected"], got):
        if norm(want) != norm(have):
            fails.append((fn, argv, want, have))

    print(f"{len(CASES) - len(fails)}/{len(CASES)} cases match Postgres")
    for fn, argv, want, have in fails:
        print(f"\nFAIL {fn}({', '.join(repr(a) for a in argv)})")
        print(f"  postgres: {want}")
        print(f"  duckdb  : {have}")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
