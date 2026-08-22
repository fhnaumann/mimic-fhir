-- Why the shipped fix emits 27,464,811 components across 150 itemids where the
-- issue measured 27,459,516 across 147.
--
-- The two predicates are not the same test, and the commit says so: the issue
-- asks "is the text numeric-looking?", the fix asks "is the text numeric-looking
-- AND equal to the number?". They differ in two places, both of which make the
-- fix carry MORE text than the issue counted:
--
--   issue  chartevents_value_drop.py:79   ^\s*-?[0-9]*\.?[0-9]+\s*$   -- match => text is "just the number", drop
--   fix    fhir_observation_chartevents.sql ^\s*-?\d+(\.\d+)?\s*$     -- match => cast and compare to valuenum
--
--   (a) the fix's pattern requires a digit before the decimal point, so '.5'
--       matches the issue's pattern but not the fix's and is carried;
--   (b) text that IS numeric but does NOT equal valuenum ('98' beside 98.5) is
--       dropped by the issue's test and carried by the fix.
--
-- Source-only: never touches the 400 GB mimic_fhir table. One scan of
-- mimiciv_icu.chartevents.

\set ON_ERROR_STOP on
\timing on
SET max_parallel_workers_per_gather = 12;
SET work_mem = '2GB';

\echo
\echo '################ component predicate: fix vs issue ################'

CREATE TEMP TABLE v_delta AS
SELECT
    ce.itemid
  , ce.value
  , ce.valuenum
  , count(*) AS n_rows
  -- the issue's test: numeric-looking text is "just the number"
  , (ce.value ~ '^\s*-?[0-9]*\.?[0-9]+\s*$')            AS issue_numeric
  -- the fix's test, verbatim from fhir_observation_chartevents.sql
  , (CASE WHEN ce.value ~ '^\s*-?\d+(\.\d+)?\s*$'
          THEN CAST(ce.value AS DOUBLE PRECISION) IS DISTINCT FROM ce.valuenum
          ELSE TRUE END)                                AS fix_carries
  , (ce.value ~ '^\s*-?\d+(\.\d+)?\s*$')                AS fix_numeric
FROM mimiciv_icu.chartevents ce
WHERE ((ce.stay_id = 34934165) AND (ce.charttime = '2151-10-03 05:14:00.000')) = FALSE
  AND ce.value IS NOT NULL
  AND ce.valuenum IS NOT NULL
GROUP BY ce.itemid, ce.value, ce.valuenum;

\echo
\echo '--- D1 the two predicates side by side ---'
SELECT
    sum(n_rows)                                              AS rows_with_number_and_text
  , sum(n_rows) FILTER (WHERE NOT issue_numeric)             AS issue_set          -- expect 27,459,516
  , sum(n_rows) FILTER (WHERE fix_carries)                   AS fix_set            -- expect 27,464,811
  , sum(n_rows) FILTER (WHERE fix_carries AND issue_numeric) AS fix_only
  , coalesce(sum(n_rows) FILTER (WHERE NOT issue_numeric AND NOT fix_carries), 0) AS issue_only  -- must be 0
FROM v_delta;

\echo
\echo '--- D2 the extra rows, split by cause ---'
SELECT
    sum(n_rows) FILTER (WHERE fix_carries AND issue_numeric AND NOT fix_numeric)
      AS a_leading_dot_decimal      -- '.5': issue calls it the number, fix does not parse it
  , sum(n_rows) FILTER (WHERE fix_carries AND issue_numeric AND fix_numeric)
      AS b_numeric_but_unequal      -- '98' beside valuenum 98.5
FROM v_delta;

\echo
\echo '--- D3 itemid counts under each predicate ---'
SELECT
    count(DISTINCT itemid) FILTER (WHERE NOT issue_numeric)             AS issue_itemids   -- expect 147
  , count(DISTINCT itemid) FILTER (WHERE fix_carries)                   AS fix_itemids     -- expect 150
FROM v_delta;

\echo
\echo '--- D4 the itemids the fix adds ---'
SELECT d.itemid, di.label, di.param_type, sum(d.n_rows) AS rows_added
FROM v_delta d LEFT JOIN mimiciv_icu.d_items di ON di.itemid = d.itemid
WHERE d.fix_carries
GROUP BY d.itemid, di.label, di.param_type
HAVING NOT bool_or(NOT d.issue_numeric)          -- itemid has no non-numeric text at all
ORDER BY rows_added DESC;

\echo
\echo '--- D5 worked examples of the extra rows (top 30 by frequency) ---'
SELECT d.itemid, di.label, d.value AS source_text, d.valuenum AS source_valuenum, d.n_rows
     , CASE WHEN NOT d.fix_numeric THEN 'a: not parseable by the fix pattern'
            ELSE 'b: numeric but <> valuenum' END AS cause
FROM v_delta d LEFT JOIN mimiciv_icu.d_items di ON di.itemid = d.itemid
WHERE d.fix_carries AND d.issue_numeric
ORDER BY d.n_rows DESC LIMIT 30;

\echo
\echo '--- D6 sanity: does any carried component restate the number exactly? ---'
-- Anything here would be pure redundancy on the published resources.
SELECT count(*) AS distinct_combos, coalesce(sum(n_rows),0) AS rows
FROM v_delta
WHERE fix_carries AND fix_numeric
  AND CAST(value AS DOUBLE PRECISION) = valuenum;

\echo
\echo '################ done ################'
