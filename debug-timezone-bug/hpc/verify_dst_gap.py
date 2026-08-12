#!/usr/bin/env python3
"""Reproduce the DST spring-forward gap defect in MIMIC-on-FHIR.

Compares mimiciv_hosp.admissions.admittime in the original MIMIC-IV
distribution against Encounter.period.start in the published MIMIC-on-FHIR
ndjson release.

The comparison is wall-clock only: it takes the first 19 characters of the
FHIR dateTime string and never parses the offset as an instant. Parsing the
offset would introduce a separate, larger local-timezone artefact and mask
the one-hour effect being measured.

Requires only duckdb:

    pip install duckdb
    python verify_dst_gap.py \
        --admissions /path/to/mimic-iv-2.2/hosp/admissions.csv.gz \
        --encounter-ndjson /path/to/fhir/MimicEncounter.ndjson.gz
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

HOSP_SYSTEM = 'http://mimic.mit.edu/fhir/mimic/identifier/encounter-hosp'


def lit(value) -> str:
    """Quote a value as a SQL string literal (CREATE VIEW cannot bind params)."""
    return "'" + str(value).replace("'", "''") + "'"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--admissions', type=Path, required=True,
                        help='mimic-iv-2.2/hosp/admissions.csv.gz')
    parser.add_argument('--encounter-ndjson', type=Path, required=True,
                        help='MimicEncounter.ndjson.gz from the FHIR release')
    parser.add_argument('--threads', type=int, default=4)
    args = parser.parse_args()

    for path in (args.admissions, args.encounter_ndjson):
        if not path.is_file():
            print(f'ERROR: not found: {path}', file=sys.stderr)
            return 2

    con = duckdb.connect()
    con.execute(f'SET threads = {args.threads}')
    # Nothing here uses TIMESTAMPTZ. Set explicitly so that the result cannot
    # depend on the machine running this script.
    con.execute("SET TimeZone = 'UTC'")

    con.execute(f"""
        CREATE VIEW admissions AS
        SELECT subject_id, hadm_id, admittime
        FROM read_csv({lit(args.admissions)}, header = true, types = {{
            'subject_id': 'BIGINT', 'hadm_id': 'BIGINT',
            'admittime': 'TIMESTAMP'
        }})
    """)

    # Only two fields are needed, so the schema is given explicitly rather
    # than inferred over the whole file.
    con.execute(f"""
        CREATE VIEW fhir_encounters AS
        WITH raw AS (
            SELECT unnest(identifier) AS ident, period.start AS period_start
            FROM read_json({lit(args.encounter_ndjson)},
                           format = 'newline_delimited',
                           compression = 'gzip', columns = {{
                'identifier': 'STRUCT(system VARCHAR, value VARCHAR)[]',
                'period': 'STRUCT(start VARCHAR, "end" VARCHAR)'
            }})
        )
        SELECT DISTINCT
            TRY_CAST(ident.value AS BIGINT) AS hadm_id,
            period_start,
            -- Keep the wall-clock digits; do not interpret the offset.
            TRY_CAST(substr(period_start, 1, 19) AS TIMESTAMP) AS period_walltime
        FROM raw
        WHERE ident.system = {lit(HOSP_SYSTEM)}
          AND TRY_CAST(ident.value AS BIGINT) IS NOT NULL
    """)

    con.execute("""
        CREATE VIEW comparison AS
        SELECT a.hadm_id, a.subject_id, a.admittime, f.period_start,
               epoch(f.period_walltime - a.admittime) / 3600.0 AS delta_hours
        FROM admissions a
        JOIN fhir_encounters f USING (hadm_id)
    """)

    compared, identical, plus_one, other = con.execute("""
        SELECT count(*),
               count(*) FILTER (WHERE delta_hours = 0),
               count(*) FILTER (WHERE delta_hours = 1),
               count(*) FILTER (WHERE delta_hours NOT IN (0, 1))
        FROM comparison
    """).fetchone()

    # Every MIMIC admission whose wall-clock time falls in the spring-forward
    # gap: 02:00-02:59 on the second Sunday of March (US rule since 2007).
    at_risk = con.execute("""
        SELECT count(*) FROM admissions
        WHERE month(admittime) = 3 AND dayofweek(admittime) = 0
          AND day(admittime) BETWEEN 8 AND 14 AND hour(admittime) = 2
    """).fetchone()[0]

    print(f'encounters compared      : {compared:,}')
    print(f'identical wall clock     : {identical:,}')
    print(f'FHIR exactly +1 hour     : {plus_one:,}')
    print(f'other differences        : {other:,}')
    print()
    print(f'admissions in the DST gap: {at_risk:,}')
    print(f'  ...of which shifted    : {plus_one:,}'
          f' ({"100%" if at_risk and plus_one == at_risk else "see below"})')

    profile = con.execute("""
        SELECT count(*),
               count(*) FILTER (WHERE hour(admittime) = 2),
               count(*) FILTER (WHERE dayname(admittime) = 'Sunday'),
               count(*) FILTER (WHERE month(admittime) = 3
                                  AND day(admittime) BETWEEN 8 AND 14),
               count(*) FILTER (WHERE right(period_start, 6) = '-04:00')
        FROM comparison WHERE delta_hours != 0
    """).fetchone()
    total, at_two, sundays, second_sunday, edt = profile
    print()
    print('Profile of the differing rows (all should equal the total):')
    print(f'  total differing        : {total:,}')
    print(f'  source hour is 02      : {at_two:,}')
    print(f'  falls on a Sunday      : {sundays:,}')
    print(f'  2nd Sunday of March    : {second_sunday:,}')
    print(f'  FHIR offset is -04:00  : {edt:,}')

    rows = con.execute("""
        SELECT hadm_id, subject_id, admittime, period_start, delta_hours
        FROM comparison WHERE delta_hours != 0
        ORDER BY hadm_id LIMIT 10
    """).fetchall()
    if rows:
        print()
        print('First 10 differing rows:')
        print(f'  {"hadm_id":>10}  {"subject_id":>10}  {"MIMIC admittime":>19}'
              f'  {"FHIR period.start":>26}  {"delta":>5}')
        for hadm_id, subject_id, admittime, start, delta in rows:
            print(f'  {hadm_id:>10}  {subject_id:>10}  {str(admittime):>19}'
                  f'  {start:>26}  {delta:>5}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
