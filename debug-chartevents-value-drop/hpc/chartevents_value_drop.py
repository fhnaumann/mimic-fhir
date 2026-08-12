#!/usr/bin/env python3
"""Full-cohort verification of the chartevents `value`-text-dropped defect.

The claim under test, from `sql/fhir_observation_chartevents.sql`:

    , 'valueQuantity',
        CASE WHEN ce_VALUENUM IS NOT NULL THEN ... ELSE NULL END
    , 'valueString',
        CASE WHEN ce_VALUENUM IS NULL THEN ce_VALUE ELSE NULL END

The two branches partition the rows, so `ce.value` is served only when there
is no number to serve. Any chartevents row carrying both a `valuenum` and a
semantically distinct `value` loses the text.

Everything here is measured against the ORIGINAL MIMIC-IV distribution. There
is deliberately no Delta/Pathling/warehouse dependency: the served side is
reproduced by re-implementing the ETL's own two CASE expressions in SQL, so a
reader can re-run this against the files PhysioNet distributes with nothing
but DuckDB and check the arithmetic themselves.

Two independent readings of the source are taken and asserted to agree:

  csv     chartevents.csv.gz, read directly by DuckDB's CSV reader
  oracle  a prebuilt DuckDB database loaded from those same CSVs

The csv reading is the one quoted in the issue; the oracle is the fast
cross-check and the substrate for the per-charttime collision pass, which is
too heavy to want to run twice.

Outputs, all written to --out-dir:

  report.txt                    human-readable findings
  summary.json                  the same numbers, machine-readable
  source_value_agg.parquet      (itemid, value, valuenum) -> row count
  ambiguous_pairs.csv           test 1: one number, several meanings
  nonnumeric_text_items.csv     test 2: text that is not a rendered number
  gcs_verbal_223900.csv         test 3: the GCS verbal breakdown
  concept_itemid_audit.csv      test 5: itemids used as text by mimic-code
  collisions_by_item.csv        resources that collapse onto identical content
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

# --------------------------------------------------------------------------
# The itemids that official mimic-code concepts match as text. Obtained by
# sweeping mimic-iv/concepts/**/*.sql for comparisons against chartevents
# `value`; these four are the complete set at the commit swept.
# --------------------------------------------------------------------------
CONCEPT_TEXT_ITEMIDS = {
    223900: ('measurement/gcs.sql', "value = 'No Response-ETT' -> verbal 0, gcs_unable 1"),
    223758: ('treatment/code_status.sql', "value IN ('Full code', 'DNR / DNI', ...)"),
    224146: ('treatment/crrt.sql', "value IN ('Active', 'Clots Present', ...)"),
    225965: ('treatment/rrt.sql', "value = 'In use' -> dialysis_active"),
}

# The ETL excludes one (stay_id, charttime) outright, and every row with a
# NULL value. Mirrored here so the projected served counts match what the ETL
# would actually emit -- the exclusion is two rows out of 313M and changes no
# headline number, but a reader checking the arithmetic should not have to
# wonder where the discrepancy came from.
ETL_EXCLUDED_STAY_ID = 34934165
ETL_EXCLUDED_CHARTTIME = '2151-10-03 05:14:00'
ETL_EXCLUSION_SQL = (
    f'(stay_id = {ETL_EXCLUDED_STAY_ID} '
    f"AND charttime = TIMESTAMP '{ETL_EXCLUDED_CHARTTIME}')"
)

# A `value` that is merely the number written out carries no information the
# valuenum does not already carry. Anything else plausibly does.
NUMERIC_TEXT_RE = r'^\s*-?[0-9]*\.?[0-9]+\s*$'

CHARTEVENTS_TYPES = {
    'stay_id': 'BIGINT',
    'charttime': 'TIMESTAMP',
    'itemid': 'INTEGER',
    'value': 'VARCHAR',
    'valuenum': 'DOUBLE',
    'valueuom': 'VARCHAR',
}


def sql_literal(value) -> str:
    """Quote a value as a SQL string literal (CREATE VIEW cannot bind params)."""
    return "'" + str(value).replace("'", "''") + "'"


def one(con, sql: str):
    """Run a query expected to produce exactly one row."""
    row = con.execute(sql).fetchone()
    return row if row is None or len(row) > 1 else row[0]


def write_csv(path: Path, header: list[str], rows) -> None:
    with path.open('w', newline='') as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        writer.writerows(rows)


# --------------------------------------------------------------------------
# Stage 1/2 -- the source aggregate, read two ways
# --------------------------------------------------------------------------

def aggregate_from_csv(con, chartevents_csv: Path, out_dir: Path) -> Path:
    """Aggregate (itemid, value, valuenum) straight off the distributed CSV.

    One pass. gzip decompression is single-threaded regardless of --threads,
    so this is the slowest stage by a wide margin; everything downstream runs
    off the aggregate it produces.
    """
    target = out_dir / 'source_value_agg_csv.parquet'
    types = ', '.join(f"{sql_literal(k)}: {sql_literal(v)}"
                      for k, v in CHARTEVENTS_TYPES.items())
    con.execute(f"""
        COPY (
            SELECT itemid, value, valuenum, {ETL_EXCLUSION_SQL} AS etl_excluded,
                   count(*) AS n_rows
            FROM read_csv({sql_literal(chartevents_csv)},
                          header = true, types = {{{types}}})
            GROUP BY itemid, value, valuenum, etl_excluded
        ) TO {sql_literal(target)} (FORMAT PARQUET)
    """)
    return target


def aggregate_from_oracle(con, out_dir: Path) -> Path:
    """The same aggregate from the prebuilt DuckDB load of those CSVs."""
    target = out_dir / 'source_value_agg_oracle.parquet'
    con.execute(f"""
        COPY (
            SELECT itemid, value, valuenum, {ETL_EXCLUSION_SQL} AS etl_excluded,
                   count(*) AS n_rows
            FROM oracle.mimiciv_icu.chartevents
            GROUP BY itemid, value, valuenum, etl_excluded
        ) TO {sql_literal(target)} (FORMAT PARQUET)
    """)
    return target


def compare_aggregates(con, csv_path: Path, oracle_path: Path) -> dict:
    """Full outer join the two readings and count every way they can differ.

    Cell-by-cell equality on a NULL-bearing key, so the join uses IS NOT
    DISTINCT FROM rather than =; `value` and `valuenum` are both nullable and
    a plain equijoin would silently drop those groups from the comparison.

    The two readings do not agree bit-for-bit on `valuenum` and cannot be made
    to. MIMIC-IV declares the column double precision, which is what reading
    the CSV text yields; the prebuilt database stores it single precision, so
    widening it back to DOUBLE reproduces the binary32 rounding -- 0.9 comes
    back as 0.8999999761581421. That is a property of the convenience copy,
    not a disagreement about the data.

    So the comparison is made at the coarser of the two precisions: the CSV
    side is re-aggregated to FLOAT before joining. Distinct doubles that share
    a float are merged first, and `csv_groups_merged_by_float` reports how
    many, so the concession is visible rather than buried in the join.

    The authoritative aggregate remains the DOUBLE one; only this cross-check
    is done at FLOAT.
    """
    con.execute(f"""
        CREATE OR REPLACE VIEW agg_compare AS
        WITH csv_at_float AS (
            SELECT itemid, value, CAST(valuenum AS FLOAT) AS valuenum,
                   etl_excluded, sum(n_rows) AS n_rows
            FROM read_parquet({sql_literal(csv_path)})
            GROUP BY itemid, value, CAST(valuenum AS FLOAT), etl_excluded
        )
        SELECT
            coalesce(c.itemid, o.itemid)        AS itemid,
            coalesce(c.value, o.value)          AS value,
            coalesce(c.valuenum, o.valuenum)    AS valuenum,
            c.n_rows                            AS csv_rows,
            o.n_rows                            AS oracle_rows
        FROM csv_at_float c
        FULL OUTER JOIN read_parquet({sql_literal(oracle_path)}) o
          ON  c.itemid       IS NOT DISTINCT FROM o.itemid
          AND c.value        IS NOT DISTINCT FROM o.value
          AND c.valuenum     IS NOT DISTINCT FROM o.valuenum
          AND c.etl_excluded IS NOT DISTINCT FROM o.etl_excluded
    """)
    groups, csv_only, oracle_only, count_mismatch = con.execute("""
        SELECT count(*),
               count(*) FILTER (WHERE oracle_rows IS NULL),
               count(*) FILTER (WHERE csv_rows IS NULL),
               count(*) FILTER (WHERE csv_rows IS NOT NULL
                                  AND oracle_rows IS NOT NULL
                                  AND csv_rows != oracle_rows)
        FROM agg_compare
    """).fetchone()
    csv_total = one(con, "SELECT coalesce(sum(csv_rows), 0) FROM agg_compare")
    oracle_total = one(con, "SELECT coalesce(sum(oracle_rows), 0) FROM agg_compare")
    merged = one(con, f"""
        SELECT count(*) - count(DISTINCT (itemid, value,
                                          CAST(valuenum AS FLOAT), etl_excluded))
        FROM read_parquet({sql_literal(csv_path)})
    """)
    return {
        'distinct_groups': groups,
        'csv_groups_merged_by_float': int(merged),
        'groups_only_in_csv': csv_only,
        'groups_only_in_oracle': oracle_only,
        'groups_with_differing_counts': count_mismatch,
        'total_rows_csv': int(csv_total),
        'total_rows_oracle': int(oracle_total),
        'agree': (csv_only == 0 and oracle_only == 0 and count_mismatch == 0
                  and csv_total == oracle_total),
    }


# --------------------------------------------------------------------------
# Stage 3 -- the tests, all derived from the aggregate
# --------------------------------------------------------------------------

def build_views(con, agg_path: Path, d_items_csv: Path) -> None:
    con.execute(f"""
        CREATE OR REPLACE VIEW d_items AS
        SELECT itemid, label, category, param_type
        FROM read_csv({sql_literal(d_items_csv)}, header = true, types = {{
            'itemid': 'INTEGER', 'label': 'VARCHAR',
            'category': 'VARCHAR', 'param_type': 'VARCHAR'
        }})
    """)
    # `agg` is every (itemid, value, valuenum) combination in the source.
    # `served` restricts to what the ETL would actually insert -- it drops
    # NULL values outright -- and applies the ETL's own choice of value[x].
    con.execute(f"""
        CREATE OR REPLACE VIEW agg AS
        SELECT a.itemid, a.value, a.valuenum, a.etl_excluded, a.n_rows,
               di.label, di.category, di.param_type
        FROM read_parquet({sql_literal(agg_path)}) a
        LEFT JOIN d_items di ON di.itemid = a.itemid
    """)
    con.execute("""
        CREATE OR REPLACE VIEW served AS
        SELECT itemid, label, category, param_type, value, valuenum, n_rows,
               -- Exactly the ETL's two CASE expressions.
               CASE WHEN valuenum IS NOT NULL THEN valuenum END AS value_quantity,
               CASE WHEN valuenum IS NULL     THEN value    END AS value_string
        FROM agg
        -- Exactly the ETL's WHERE clause.
        WHERE value IS NOT NULL AND NOT etl_excluded
    """)


def collect_results(con, out_dir: Path) -> dict:
    results: dict = {}

    # -- Totals -----------------------------------------------------------
    (total_rows, rows_with_num, rows_null_value,
     rows_excluded, distinct_items) = con.execute("""
        SELECT sum(n_rows),
               sum(n_rows) FILTER (WHERE valuenum IS NOT NULL),
               sum(n_rows) FILTER (WHERE value IS NULL),
               sum(n_rows) FILTER (WHERE etl_excluded),
               count(DISTINCT itemid)
        FROM agg
    """).fetchone()
    results['chartevents_rows'] = int(total_rows)
    results['rows_with_valuenum'] = int(rows_with_num)
    results['rows_with_null_value'] = int(rows_null_value or 0)
    results['rows_excluded_by_etl_filter'] = int(rows_excluded or 0)
    results['distinct_itemids'] = distinct_items

    served_rows, served_quantity, served_string = con.execute("""
        SELECT sum(n_rows),
               sum(n_rows) FILTER (WHERE value_quantity IS NOT NULL),
               sum(n_rows) FILTER (WHERE value_string IS NOT NULL)
        FROM served
    """).fetchone()
    results['served_observations'] = int(served_rows)
    results['served_with_value_quantity'] = int(served_quantity)
    results['served_with_value_string'] = int(served_string)

    # Rows where a number is served and a non-numeric text exists alongside:
    # the population whose text the resource cannot express.
    text_lost = one(con, f"""
        SELECT coalesce(sum(n_rows), 0) FROM served
        WHERE value_quantity IS NOT NULL
          AND NOT regexp_matches(value, {sql_literal(NUMERIC_TEXT_RE)})
    """)
    results['rows_with_number_and_nonnumeric_text'] = int(text_lost)

    # -- Test 1: ambiguous (itemid, valuenum) pairs ------------------------
    con.execute("""
        CREATE OR REPLACE VIEW ambiguous_pairs AS
        SELECT itemid, label, valuenum,
               count(DISTINCT value)      AS n_distinct_text,
               list_sort(list(DISTINCT value)) AS texts,
               sum(n_rows)                AS n_rows
        FROM served
        WHERE valuenum IS NOT NULL
        GROUP BY itemid, label, valuenum
        HAVING count(DISTINCT value) > 1
    """)
    n_pairs, n_items, n_ambiguous_rows = con.execute("""
        SELECT count(*), count(DISTINCT itemid), coalesce(sum(n_rows), 0)
        FROM ambiguous_pairs
    """).fetchone()
    results['ambiguous_pairs'] = n_pairs
    results['ambiguous_itemids'] = n_items
    results['ambiguous_rows'] = int(n_ambiguous_rows)

    write_csv(
        out_dir / 'ambiguous_pairs.csv',
        ['itemid', 'label', 'valuenum', 'n_distinct_text', 'texts', 'n_rows'],
        [(i, lab, vn, nd, ' | '.join('' if t is None else t for t in tx), nr)
         for i, lab, vn, nd, tx, nr in con.execute("""
            SELECT itemid, label, valuenum, n_distinct_text, texts, n_rows
            FROM ambiguous_pairs ORDER BY n_rows DESC, itemid, valuenum
         """).fetchall()],
    )

    # The same list restricted to pairs where at least one label is not just
    # the number rendered as text -- ambiguity that actually costs meaning.
    results['ambiguous_pairs_with_meaningful_text'] = one(con, f"""
        SELECT count(*) FROM (
            SELECT itemid, valuenum FROM served
            WHERE valuenum IS NOT NULL
            GROUP BY itemid, valuenum
            HAVING count(DISTINCT value) > 1
               AND count(DISTINCT value) FILTER (
                     WHERE NOT regexp_matches(value, {sql_literal(NUMERIC_TEXT_RE)})
                   ) > 0
        )
    """)

    # -- Test 2: numeric rows whose text is not a rendered number ----------
    rows = con.execute(f"""
        SELECT itemid, label, param_type,
               sum(n_rows) AS n_rows,
               count(DISTINCT value) AS n_distinct_text
        FROM served
        WHERE valuenum IS NOT NULL
          AND NOT regexp_matches(value, {sql_literal(NUMERIC_TEXT_RE)})
        GROUP BY itemid, label, param_type
        ORDER BY n_rows DESC
    """).fetchall()
    write_csv(out_dir / 'nonnumeric_text_items.csv',
              ['itemid', 'label', 'param_type', 'n_rows', 'n_distinct_text'], rows)
    results['itemids_with_nonnumeric_text'] = len(rows)
    results['top_nonnumeric_text_items'] = [
        {'itemid': i, 'label': lab, 'param_type': pt,
         'n_rows': int(n), 'n_distinct_text': nd}
        for i, lab, pt, n, nd in rows[:20]
    ]

    # -- Test 3: GCS verbal, itemid 223900 --------------------------------
    gcs_rows = con.execute("""
        SELECT value, valuenum, n_rows
        FROM agg WHERE itemid = 223900
        ORDER BY n_rows DESC
    """).fetchall()
    write_csv(out_dir / 'gcs_verbal_223900.csv',
              ['value', 'valuenum', 'n_rows'], gcs_rows)
    results['gcs_223900'] = {
        'total_rows': int(sum(r[2] for r in gcs_rows)),
        'rows_with_valuenum': int(sum(r[2] for r in gcs_rows if r[1] is not None)),
        'breakdown': [{'value': v, 'valuenum': vn, 'n_rows': int(n)}
                      for v, vn, n in gcs_rows],
    }
    ett, no_response, at_one = con.execute("""
        SELECT coalesce(sum(n_rows) FILTER (WHERE value = 'No Response-ETT'), 0),
               coalesce(sum(n_rows) FILTER (WHERE value = 'No Response'), 0),
               coalesce(sum(n_rows) FILTER (WHERE valuenum = 1), 0)
        FROM agg WHERE itemid = 223900
    """).fetchone()
    results['gcs_223900'].update({
        'no_response_ett_rows': int(ett),
        'no_response_rows': int(no_response),
        'rows_at_valuenum_1': int(at_one),
    })

    # -- Test 5: every itemid a mimic-code concept reads as text -----------
    audit = []
    for itemid, (concept, usage) in sorted(CONCEPT_TEXT_ITEMIDS.items()):
        row = con.execute(f"""
            SELECT coalesce(sum(n_rows), 0),
                   coalesce(sum(n_rows) FILTER (WHERE valuenum IS NOT NULL), 0),
                   count(DISTINCT value),
                   max(label)
            FROM agg WHERE itemid = {itemid}
        """).fetchone()
        n_rows, n_with_num, n_texts, label = row
        affected = int(n_with_num) > 0
        audit.append({
            'itemid': itemid, 'label': label, 'concept': concept, 'usage': usage,
            'n_rows': int(n_rows), 'rows_with_valuenum': int(n_with_num),
            'n_distinct_text': n_texts,
            'text_survives_etl': not affected,
            'affected': affected,
        })
    write_csv(
        out_dir / 'concept_itemid_audit.csv',
        ['itemid', 'label', 'concept', 'usage', 'n_rows', 'rows_with_valuenum',
         'n_distinct_text', 'text_survives_etl'],
        [(a['itemid'], a['label'], a['concept'], a['usage'], a['n_rows'],
          a['rows_with_valuenum'], a['n_distinct_text'], a['text_survives_etl'])
         for a in audit],
    )
    results['concept_itemid_audit'] = audit
    results['affected_concepts'] = sorted(
        {a['concept'] for a in audit if a['affected']})

    return results


def collect_collisions(con, out_dir: Path) -> dict:
    """Rows that collapse onto byte-identical served content.

    Test 1 shows a number can mean two things across the table. This is the
    stricter claim: the same patient, at the same instant, on the same item,
    with the same number and different text -- two source rows that the ETL
    turns into two Observations distinguishable only by id, because the id is
    a UUIDv5 over `value` and nothing else in the body is.

    Grouped on the ETL's own reference key rather than on the aggregate, so
    this needs a second pass over the full table.
    """
    con.execute(f"""
        CREATE OR REPLACE VIEW collisions AS
        SELECT stay_id, charttime, itemid, valuenum, valueuom,
               count(DISTINCT value) AS n_distinct_text,
               count(*)              AS n_rows
        FROM oracle.mimiciv_icu.chartevents
        WHERE value IS NOT NULL
          AND valuenum IS NOT NULL
          AND NOT {ETL_EXCLUSION_SQL}
        GROUP BY stay_id, charttime, itemid, valuenum, valueuom
        HAVING count(DISTINCT value) > 1
    """)
    groups, rows, items = con.execute("""
        SELECT count(*), coalesce(sum(n_rows), 0), count(DISTINCT itemid)
        FROM collisions
    """).fetchone()

    by_item = con.execute("""
        SELECT c.itemid, max(di.label) AS label,
               count(*) AS n_groups, sum(c.n_rows) AS n_rows
        FROM collisions c
        LEFT JOIN d_items di ON di.itemid = c.itemid
        GROUP BY c.itemid
        ORDER BY n_rows DESC
    """).fetchall()
    write_csv(out_dir / 'collisions_by_item.csv',
              ['itemid', 'label', 'n_groups', 'n_rows'], by_item)

    return {
        'collision_groups': groups,
        'collision_rows': int(rows),
        'collision_itemids': items,
        'top_collision_items': [
            {'itemid': i, 'label': lab, 'n_groups': g, 'n_rows': int(n)}
            for i, lab, g, n in by_item[:20]
        ],
    }


# --------------------------------------------------------------------------
# Reporting
# --------------------------------------------------------------------------

def rule(title: str) -> str:
    return f'== {title} '.ljust(78, '=')


def render_report(results: dict, config: dict, agreement: dict | None) -> str:
    out: list[str] = []
    add = out.append

    add('chartevents `value` text dropped -- full-cohort verification')
    add(f'Generated: {datetime.now(timezone.utc).isoformat(timespec="seconds")}')
    add('')

    add(rule('Inputs'))
    for key, value in config.items():
        add(f'  {key}: {value}')
    add('')

    if agreement is not None:
        add(rule('Source agreement (csv.gz vs prebuilt DuckDB)'))
        add('  Compared at FLOAT: the prebuilt database stores valuenum single')
        add('  precision, the CSV text reads as double. The authoritative')
        add('  aggregate is the double one; this is the cross-check only.')
        add(f'  distinct (itemid, value, valuenum) groups: {agreement["distinct_groups"]:,}')
        add(f'  csv groups merged by the FLOAT cast:       {agreement["csv_groups_merged_by_float"]:,}')
        add(f'  groups only in csv.gz:                     {agreement["groups_only_in_csv"]:,}')
        add(f'  groups only in oracle:                     {agreement["groups_only_in_oracle"]:,}')
        add(f'  groups with differing counts:              {agreement["groups_with_differing_counts"]:,}')
        add(f'  total rows, csv.gz:                        {agreement["total_rows_csv"]:,}')
        add(f'  total rows, oracle:                        {agreement["total_rows_oracle"]:,}')
        add(f'  verdict: {"IDENTICAL" if agreement["agree"] else "MISMATCH -- see above"}')
        add('')

    add(rule('Source totals'))
    add(f'  chartevents rows:            {results["chartevents_rows"]:,}')
    add(f'  distinct itemids:            {results["distinct_itemids"]:,}')
    add(f'  rows with a valuenum:        {results["rows_with_valuenum"]:,}')
    add(f'  rows with NULL value:        {results["rows_with_null_value"]:,}  (excluded by the ETL)')
    add(f'  rows on the excluded stay:   {results["rows_excluded_by_etl_filter"]:,}  (excluded by the ETL)')
    add('')

    add(rule('What the ETL would serve'))
    add('  Applying the two CASE expressions from fhir_observation_chartevents.sql')
    add('  to the source rows the ETL admits.')
    add(f'  Observations emitted:        {results["served_observations"]:,}')
    add(f'  ...with valueQuantity:       {results["served_with_value_quantity"]:,}')
    add(f'  ...with valueString:         {results["served_with_value_string"]:,}')
    add(f'  rows whose text is dropped')
    add(f'  and is not just the number:  {results["rows_with_number_and_nonnumeric_text"]:,}')
    add('')

    add(rule('Test 1 -- ambiguous (itemid, valuenum) pairs'))
    add('  One number that the source gives more than one meaning. Every row in')
    add('  such a pair is served as a value the consumer cannot disambiguate.')
    add(f'  ambiguous pairs:             {results["ambiguous_pairs"]:,}')
    add(f'  ...over distinct itemids:    {results["ambiguous_itemids"]:,}')
    add(f'  ...covering rows:            {results["ambiguous_rows"]:,}')
    add(f'  pairs where >=1 label is not')
    add(f'  merely the number as text:   {results["ambiguous_pairs_with_meaningful_text"]:,}')
    add('  full list: ambiguous_pairs.csv')
    add('')

    add(rule('Test 2 -- numeric rows carrying non-numeric text'))
    add(f'  itemids affected:            {results["itemids_with_nonnumeric_text"]:,}')
    add('  full list: nonnumeric_text_items.csv')
    if results['top_nonnumeric_text_items']:
        add('')
        add(f'  {"itemid":>8}  {"rows":>14}  {"texts":>6}  label')
        for item in results['top_nonnumeric_text_items']:
            add(f'  {item["itemid"]:>8}  {item["n_rows"]:>14,}  '
                f'{item["n_distinct_text"]:>6}  {item["label"]}')
    add('')

    gcs = results['gcs_223900']
    add(rule('Test 3 -- GCS verbal response (itemid 223900)'))
    add(f'  total rows:                  {gcs["total_rows"]:,}')
    add(f'  ...with a valuenum:          {gcs["rows_with_valuenum"]:,}')
    add(f'  value = No Response-ETT:     {gcs["no_response_ett_rows"]:,}')
    add(f'  value = No Response:         {gcs["no_response_rows"]:,}')
    add(f'  rows at valuenum = 1:        {gcs["rows_at_valuenum_1"]:,}')
    if gcs['breakdown']:
        add('')
        add(f'  {"valuenum":>9}  {"rows":>12}  value')
        for entry in gcs['breakdown']:
            vnum = '' if entry['valuenum'] is None else f'{entry["valuenum"]:g}'
            add(f'  {vnum:>9}  {entry["n_rows"]:>12,}  {entry["value"]}')
    add('')

    add(rule('Test 5 -- itemids that mimic-code concepts read as text'))
    add(f'  {"itemid":>8}  {"rows":>12}  {"with num":>12}  {"survives":>8}  concept')
    for entry in results['concept_itemid_audit']:
        add(f'  {entry["itemid"]:>8}  {entry["n_rows"]:>12,}  '
            f'{entry["rows_with_valuenum"]:>12,}  '
            f'{"yes" if entry["text_survives_etl"] else "NO":>8}  {entry["concept"]}')
    add('')
    if results['affected_concepts']:
        add('  Concepts that cannot be reproduced from the served resources:')
        for concept in results['affected_concepts']:
            add(f'    {concept}')
    else:
        add('  No concept is affected.')
    add('')

    if 'collision_groups' in results:
        add(rule('Collisions -- resources identical except for id'))
        add('  Same stay, same charttime, same itemid, same number, different')
        add('  text. The ETL keys its UUIDv5 on `value`, so these become')
        add('  distinct Observations with identical content.')
        add(f'  colliding groups:            {results["collision_groups"]:,}')
        add(f'  rows involved:               {results["collision_rows"]:,}')
        add(f'  itemids involved:            {results["collision_itemids"]:,}')
        add('  full list: collisions_by_item.csv')
        if results['top_collision_items']:
            add('')
            add(f'  {"itemid":>8}  {"groups":>10}  {"rows":>12}  label')
            for item in results['top_collision_items']:
                add(f'  {item["itemid"]:>8}  {item["n_groups"]:>10,}  '
                    f'{item["n_rows"]:>12,}  {item["label"]}')
        add('')

    add(rule('Verdict'))
    confirmed = results['served_with_value_string'] == 0 or results['ambiguous_rows'] > 0
    if confirmed:
        add(f'  {results["ambiguous_rows"]:,} row(s) across '
            f'{results["ambiguous_itemids"]:,} itemid(s) are served as a number')
        add('  that the source gives more than one meaning. The text that')
        add('  distinguishes them is not written to any element of the resource.')
        if results['affected_concepts']:
            add('')
            add('  Official mimic-code concepts that consequently cannot be')
            add('  reproduced from MIMIC-on-FHIR:')
            for concept in results['affected_concepts']:
                add(f'    {concept}')
    else:
        add('  No ambiguity found. The report is NOT confirmed on full data.')
    return '\n'.join(out) + '\n'


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------

def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--chartevents-csv', type=Path, required=True,
                        help='mimic-iv-2.2/icu/chartevents.csv.gz')
    parser.add_argument('--d-items-csv', type=Path, required=True,
                        help='mimic-iv-2.2/icu/d_items.csv.gz')
    parser.add_argument('--oracle-db', type=Path, default=None,
                        help='prebuilt DuckDB load of the same CSVs')
    parser.add_argument('--out-dir', type=Path, required=True)
    parser.add_argument('--threads', type=int, default=8)
    parser.add_argument('--memory-limit', default='48GB')
    parser.add_argument('--temp-dir', type=Path, default=None)
    parser.add_argument('--skip-collisions', action='store_true',
                        help='skip the per-charttime collision pass (needs --oracle-db)')
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)

    for path in (args.chartevents_csv, args.d_items_csv):
        if not path.is_file():
            print(f'ERROR: not found: {path}', file=sys.stderr)
            return 2
    if args.oracle_db is not None and not args.oracle_db.is_file():
        print(f'ERROR: not found: {args.oracle_db}', file=sys.stderr)
        return 2

    args.out_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute(f'SET threads = {args.threads}')
    con.execute(f"SET memory_limit = {sql_literal(args.memory_limit)}")
    if args.temp_dir is not None:
        args.temp_dir.mkdir(parents=True, exist_ok=True)
        con.execute(f"SET temp_directory = {sql_literal(args.temp_dir)}")
    # Nothing here is timezone-dependent, but the sibling DST investigation
    # found this warehouse's timestamps are, so pin it and remove the doubt.
    con.execute("SET TimeZone = 'UTC'")

    config = {
        'chartevents_csv': str(args.chartevents_csv),
        'd_items_csv': str(args.d_items_csv),
        'oracle_db': str(args.oracle_db) if args.oracle_db else '(not used)',
        'out_dir': str(args.out_dir),
        'duckdb_version': duckdb.__version__,
    }

    print('[1/5] aggregating chartevents.csv.gz ...', flush=True)
    csv_agg = aggregate_from_csv(con, args.chartevents_csv, args.out_dir)
    print(f'      -> {csv_agg}', flush=True)

    agreement = None
    if args.oracle_db is not None:
        con.execute(f"ATTACH {sql_literal(args.oracle_db)} AS oracle (READ_ONLY)")
        print('[2/5] aggregating the prebuilt DuckDB ...', flush=True)
        oracle_agg = aggregate_from_oracle(con, args.out_dir)
        print('[3/5] comparing the two readings ...', flush=True)
        agreement = compare_aggregates(con, csv_agg, oracle_agg)
        print(f'      agree: {agreement["agree"]}', flush=True)
    else:
        print('[2/5] skipped (no --oracle-db)', flush=True)
        print('[3/5] skipped (no --oracle-db)', flush=True)

    print('[4/5] running the tests ...', flush=True)
    build_views(con, csv_agg, args.d_items_csv)
    results = collect_results(con, args.out_dir)

    if args.oracle_db is not None and not args.skip_collisions:
        print('[5/5] collision pass over the full table ...', flush=True)
        results.update(collect_collisions(con, args.out_dir))
    else:
        print('[5/5] skipped', flush=True)

    # The aggregate the report quotes is the CSV one; keep it under a stable
    # name so downstream references do not depend on which readings ran.
    (args.out_dir / 'source_value_agg.parquet').unlink(missing_ok=True)
    (args.out_dir / 'source_value_agg.parquet').symlink_to(csv_agg.name)

    report = render_report(results, config, agreement)
    (args.out_dir / 'report.txt').write_text(report)
    (args.out_dir / 'summary.json').write_text(json.dumps({
        'generated_utc': datetime.now(timezone.utc).isoformat(timespec='seconds'),
        'config': config,
        'source_agreement': agreement,
        'results': results,
    }, indent=2, default=str) + '\n')

    print()
    print(report)
    return 0


if __name__ == '__main__':
    sys.exit(main())
