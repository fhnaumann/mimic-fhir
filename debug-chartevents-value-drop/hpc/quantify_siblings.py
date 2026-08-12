#!/usr/bin/env python3
"""Quantify the two sibling questions the ETL sweep left open.

1. Does `labevents` actually lose anything? Its ETL is the counter-example --
   it forces text-bearing values down the valueString path and writes
   `comments` to `note` unconditionally. If it loses nothing in practice, the
   chartevents defect is an outlier. If it loses a lot, it is a second bug.

   The ETL does not use `labevents.valuenum` directly; it recomputes it
   (fhir_observation_labevents.sql:27-37), forcing NULL when the text contains
   letters or non-ASCII, and parses comparators out separately. So the
   measurement has to replicate that logic rather than test the raw column,
   otherwise it will count rows the ETL already protects.

2. How many `inputevents` rows lose `starttime`? fhir_medication_administration
   _icu.sql:61-69 writes effectivePeriod only when `rate IS NOT NULL`, and
   effectiveDateTime (endtime) otherwise -- so a bolus loses its start. Rows
   where starttime = endtime lose nothing.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb

# Numbers, optionally wrapped in the comparator syntax the ETL parses out into
# valueQuantity.comparator. Text matching this carries nothing the published
# Quantity does not already carry.
COMPARATOR_NUMERIC_RE = r'^\s*(<=|>=|<|>|=)?\s*-?[0-9]*\.?[0-9]+\s*$'
PLAIN_NUMERIC_RE = r'^\s*-?[0-9]*\.?[0-9]+\s*$'


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--oracle-db', type=Path, required=True)
    parser.add_argument('--out-dir', type=Path, required=True)
    parser.add_argument('--threads', type=int, default=16)
    parser.add_argument('--memory-limit', default='96GB')
    parser.add_argument('--temp-dir', type=Path, default=None)
    args = parser.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f'SET threads = {args.threads}')
    con.execute(f"SET memory_limit = '{args.memory_limit}'")
    if args.temp_dir:
        args.temp_dir.mkdir(parents=True, exist_ok=True)
        con.execute(f"SET temp_directory = '{args.temp_dir}'")
    con.execute(f"ATTACH '{args.oracle_db}' AS oracle (READ_ONLY)")

    out: dict = {}

    # -- 1. labevents ------------------------------------------------------
    # Replicate the ETL's own valuenum: NULL when the text has letters or
    # non-ASCII, so those rows take the valueString branch and keep their text.
    con.execute(f"""
        CREATE OR REPLACE VIEW lab AS
        SELECT itemid, value, valuenum,
               CASE
                   WHEN valuenum IS NULL THEN NULL
                   WHEN value IS NULL THEN valuenum
                   WHEN regexp_matches(value, '[a-zA-Z]') THEN NULL
                   WHEN regexp_matches(value, '[^\\x00-\\x7F]') THEN NULL
                   ELSE valuenum
               END AS etl_valuenum
        FROM oracle.mimiciv_hosp.labevents
    """)
    row = con.execute(f"""
        SELECT count(*),
               count(*) FILTER (WHERE valuenum IS NOT NULL),
               count(*) FILTER (WHERE etl_valuenum IS NOT NULL),
               count(*) FILTER (WHERE etl_valuenum IS NOT NULL
                                  AND value IS NOT NULL
                                  AND NOT regexp_matches(value, '{PLAIN_NUMERIC_RE}')),
               count(*) FILTER (WHERE etl_valuenum IS NOT NULL
                                  AND value IS NOT NULL
                                  AND NOT regexp_matches(value, '{COMPARATOR_NUMERIC_RE}'))
        FROM lab
    """).fetchone()
    out['labevents'] = {
        'rows': row[0],
        'rows_with_source_valuenum': row[1],
        'rows_the_etl_publishes_as_quantity': row[2],
        'quantity_rows_whose_text_is_not_plain_number': row[3],
        'quantity_rows_whose_text_is_not_number_or_comparator': row[4],
    }
    out['labevents']['text_saved_by_comparator_parsing'] = row[3] - row[4]

    # The residue, if any: what does it actually look like?
    out['labevents']['residue_examples'] = [
        {'itemid': i, 'label': lab, 'value': v, 'valuenum': str(vn), 'n_rows': n}
        for i, lab, v, vn, n in con.execute(f"""
            SELECT l.itemid, max(d.label), l.value, max(l.etl_valuenum), count(*) AS n
            FROM lab l
            LEFT JOIN oracle.mimiciv_hosp.d_labitems d ON d.itemid = l.itemid
            WHERE l.etl_valuenum IS NOT NULL AND l.value IS NOT NULL
              AND NOT regexp_matches(l.value, '{COMPARATOR_NUMERIC_RE}')
            GROUP BY l.itemid, l.value
            ORDER BY n DESC
            LIMIT 25
        """).fetchall()
    ]

    # The chartevents equivalent of the same measurement, for a like-for-like
    # comparison in the report.
    out['labevents']['ambiguous_pairs'] = con.execute(f"""
        SELECT count(*) FROM (
            SELECT itemid, etl_valuenum
            FROM lab
            WHERE etl_valuenum IS NOT NULL AND value IS NOT NULL
            GROUP BY itemid, etl_valuenum
            HAVING count(DISTINCT value) > 1
               AND count(DISTINCT value) FILTER (
                     WHERE NOT regexp_matches(value, '{COMPARATOR_NUMERIC_RE}')) > 0
        )
    """).fetchone()[0]

    # -- 2. inputevents / medication administration ------------------------
    row = con.execute("""
        SELECT count(*),
               count(*) FILTER (WHERE rate IS NULL),
               count(*) FILTER (WHERE rate IS NULL AND starttime IS DISTINCT FROM endtime),
               count(*) FILTER (WHERE rate IS NULL AND starttime IS DISTINCT FROM endtime
                                  AND date_diff('minute', starttime, endtime) > 5)
        FROM oracle.mimiciv_icu.inputevents
    """).fetchone()
    out['inputevents'] = {
        'rows': row[0],
        'rows_without_rate': row[1],
        'rows_losing_a_distinct_starttime': row[2],
        'rows_where_the_lost_interval_exceeds_5_min': row[3],
    }
    out['inputevents']['duration_percentiles_minutes'] = con.execute("""
        SELECT quantile_cont(date_diff('second', starttime, endtime) / 60.0,
                             [0.5, 0.9, 0.99, 1.0])
        FROM oracle.mimiciv_icu.inputevents
        WHERE rate IS NULL AND starttime IS DISTINCT FROM endtime
    """).fetchone()[0]

    path = args.out_dir / 'siblings.json'
    path.write_text(json.dumps(out, indent=2, default=str) + '\n')
    print(json.dumps(out, indent=2, default=str))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
