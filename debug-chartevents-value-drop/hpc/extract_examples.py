#!/usr/bin/env python3
"""Pull real source rows to illustrate the defect in the issue.

Two cases:
  1. an Orientation (229381) charttime where several rows collapse onto
     identical served content
  2. the GCS verbal (223900) pair that share valuenum = 1
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--oracle-db', type=Path, required=True)
    parser.add_argument('--out-dir', type=Path, required=True)
    parser.add_argument('--threads', type=int, default=8)
    parser.add_argument('--memory-limit', default='32GB')
    args = parser.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f'SET threads = {args.threads}')
    con.execute(f"SET memory_limit = '{args.memory_limit}'")
    con.execute(f"ATTACH '{args.oracle_db}' AS oracle (READ_ONLY)")

    out: dict = {}

    # The single charttime with the most colliding Orientation rows, then
    # every row charted for that patient at that instant.
    target = con.execute("""
        SELECT subject_id, stay_id, charttime, count(*) AS n
        FROM oracle.mimiciv_icu.chartevents
        WHERE itemid = 229381 AND value IS NOT NULL AND valuenum IS NOT NULL
        GROUP BY subject_id, stay_id, charttime
        HAVING count(DISTINCT value) >= 5
        ORDER BY n DESC, stay_id, charttime
        LIMIT 1
    """).fetchone()
    out['orientation_group_key'] = list(map(str, target))

    subject_id, stay_id, charttime, _ = target
    out['orientation_rows'] = [
        dict(zip(('subject_id', 'hadm_id', 'stay_id', 'charttime', 'storetime',
                  'itemid', 'value', 'valuenum', 'valueuom', 'warning'),
                 map(str, row)))
        for row in con.execute(f"""
            SELECT subject_id, hadm_id, stay_id, charttime, storetime,
                   itemid, value, valuenum, valueuom, warning
            FROM oracle.mimiciv_icu.chartevents
            WHERE stay_id = {stay_id} AND itemid = 229381
              AND charttime = TIMESTAMP '{charttime}'
            ORDER BY value
        """).fetchall()
    ]

    # One GCS verbal row of each label, from the same stay if possible.
    out['gcs_rows'] = [
        dict(zip(('subject_id', 'hadm_id', 'stay_id', 'charttime', 'storetime',
                  'itemid', 'value', 'valuenum', 'valueuom', 'warning'),
                 map(str, row)))
        for row in con.execute("""
            WITH stays_with_both AS (
                SELECT stay_id
                FROM oracle.mimiciv_icu.chartevents
                WHERE itemid = 223900 AND value IN ('No Response-ETT', 'No Response')
                GROUP BY stay_id
                HAVING count(DISTINCT value) = 2
                ORDER BY stay_id
                LIMIT 1
            )
            SELECT ce.subject_id, ce.hadm_id, ce.stay_id, ce.charttime,
                   ce.storetime, ce.itemid, ce.value, ce.valuenum,
                   ce.valueuom, ce.warning
            FROM oracle.mimiciv_icu.chartevents ce
            JOIN stays_with_both USING (stay_id)
            WHERE ce.itemid = 223900
              AND ce.value IN ('No Response-ETT', 'No Response')
            ORDER BY ce.charttime
        """).fetchall()[:6]
    ]

    # d_items context for both.
    out['d_items'] = [
        dict(zip(('itemid', 'label', 'category', 'param_type', 'unitname',
                  'lownormalvalue', 'highnormalvalue'), map(str, row)))
        for row in con.execute("""
            SELECT itemid, label, category, param_type, unitname,
                   lownormalvalue, highnormalvalue
            FROM oracle.mimiciv_icu.d_items
            WHERE itemid IN (229381, 223900)
        """).fetchall()
    ]

    path = args.out_dir / 'examples.json'
    path.write_text(json.dumps(out, indent=2) + '\n')
    print(json.dumps(out, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
