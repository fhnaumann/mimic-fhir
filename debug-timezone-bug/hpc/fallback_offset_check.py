#!/usr/bin/env python3
"""Follow-up: what offset did the ETL pick for the fall-back ambiguous hour?

The main comparison in ``timezone_gap_debug.py`` is wall-clock only, so it is
blind to one case. On a US fall-back transition the local hour 01:00-01:59
occurs twice, once at -04:00 (EDT) and once at -05:00 (EST). Whichever the
ETL picks, the wall-clock digits are unchanged and ``delta_hours`` is 0, yet
one of the two choices names the wrong instant by an hour.

This script reports the offset distribution the ETL assigned to those rows,
alongside the offsets it assigned in the unambiguous parts of the same day.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import duckdb

import timezone_gap_debug as tz

WAREHOUSE = Path(os.path.expandvars('$SCRATCH/mimic-on-fhir-delta/spark_warehouse'))
ADMISSIONS = Path(
    os.path.expandvars('$SCRATCH/original-mimic-iv/mimic-iv-2.2/hosp/admissions.csv.gz'))


def main() -> int:
    con = duckdb.connect()
    con.execute('SET threads = 8')
    con.execute("SET TimeZone = 'UTC'")

    snapshot = tz.resolve_snapshot(con, WAREHOUSE / 'Encounter.parquet')
    tz.assert_readable_protocol(snapshot)
    print(f'snapshot v{snapshot.version}, {len(snapshot.files)} active files\n')

    tz.build_views(con, ADMISSIONS, snapshot)

    # The offset is the trailing 6 characters of the FHIR dateTime string.
    con.execute("""
        CREATE OR REPLACE VIEW dst_days AS
        SELECT
            hadm_id,
            oracle_admittime,
            fhir_period_start,
            right(fhir_period_start, 6) AS fhir_offset,
            hour(oracle_admittime) AS oracle_hour,
            delta_hours
        FROM timezone_comparison
        WHERE oracle_admittime IS NOT NULL
          AND fhir_period_start IS NOT NULL
          AND month(oracle_admittime) = 11
          AND dayofweek(oracle_admittime) = 0
          AND day(oracle_admittime) BETWEEN 1 AND 7
    """)

    print('== Offsets assigned on fall-back Sundays, by source hour ==')
    print('   hour 01 is the ambiguous hour that occurs twice locally.\n')
    rows = con.execute("""
        SELECT oracle_hour, fhir_offset, count(*) AS row_count,
               count(*) FILTER (WHERE delta_hours != 0) AS wall_clock_shifted
        FROM dst_days
        GROUP BY oracle_hour, fhir_offset
        ORDER BY oracle_hour, fhir_offset
    """).fetchall()
    print(f'  {"hour":>5}  {"offset":>7}  {"rows":>7}  {"wall-clock shifted":>19}')
    for hour, offset, count, shifted in rows:
        marker = '  <- ambiguous hour' if hour == 1 else ''
        print(f'  {hour:>5}  {offset:>7}  {count:>7,}  {shifted:>19,}{marker}')

    print()
    print('== Ambiguous-hour rows only ==')
    total, distinct_offsets = con.execute("""
        SELECT count(*), count(DISTINCT fhir_offset)
        FROM dst_days WHERE oracle_hour = 1
    """).fetchone()
    print(f'  rows in the ambiguous hour: {total:,}')
    print(f'  distinct offsets assigned:  {distinct_offsets}')
    if distinct_offsets == 1:
        print('  The ETL resolves the ambiguity deterministically. Wall-clock')
        print('  digits survive, so this comparison rates them identical, but')
        print('  roughly half name an instant one hour from the truth.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
