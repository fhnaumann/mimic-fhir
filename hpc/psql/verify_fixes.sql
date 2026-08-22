-- Verify the three fixes against the completed full `mimic4` build.
--
--   #126 / 3048c88  Patient.birthDate anchored to the MIMIC anchor pair
--   #124 / ade10fb  FHIR tables generated under UTC
--   #125 / e7c326b  chartevents.value carried in a component when valuenum is set
--
-- Read-only: creates TEMP tables only, never touches mimic_fhir or mimiciv_*.
--
-- Each section reports its own PASS/FAIL line. Every headline number the three
-- commit messages claim is recomputed here from the source distribution, so a
-- section that passes is not merely self-consistent -- it reproduces the figure
-- the issue was filed on.

\set ON_ERROR_STOP on
\timing on

SET TimeZone = 'UTC';
SET max_parallel_workers_per_gather = 12;
SET work_mem = '2GB';

\echo
\echo '################ context ################'
SELECT version();
SHOW TimeZone;
SHOW DateStyle;
SELECT current_database();

-- The uuid namespaces the resource SQL derives every id from. Joining published
-- resources back to source through these is exact, not a heuristic match.
\echo
\echo 'uuid namespaces:'
SELECT name, uuid FROM fhir_etl.uuid_namespace
 WHERE name IN ('Patient','Encounter','ObservationChartevents') ORDER BY name;


-- ============================================================================
-- A.  #126 -- Patient.birthDate anchored to the MIMIC anchor pair
-- ============================================================================
\echo
\echo '################ A. birthDate anchor (#126) ################'

-- A1 -- coverage. The fix drops the INNER JOIN to transfers, which could only
-- ever add patients. Report both counts and the number of patients that have no
-- transfer row at all, so the coverage claim is measured rather than asserted.
\echo
\echo '--- A1 coverage ---'
SELECT
    (SELECT count(*) FROM mimic_fhir.patient)    AS fhir_patient_rows
  , (SELECT count(*) FROM mimiciv_hosp.patients) AS source_patient_rows
  , (SELECT count(*) FROM mimiciv_hosp.patients p
       WHERE NOT EXISTS (SELECT 1 FROM mimiciv_hosp.transfers t
                          WHERE t.subject_id = p.subject_id)) AS patients_with_no_transfer;

-- A2 -- published birthDate vs the fix's formula, every patient.
CREATE TEMP TABLE v_pat AS
SELECT
    p.subject_id
  , p.anchor_age
  , p.anchor_year
  , (f.fhir->>'birthDate')::date AS pub_bd
  , (make_date(p.anchor_year, 1, 1) - (p.anchor_age || ' years')::interval)::date AS exp_bd
FROM mimiciv_hosp.patients p
JOIN mimic_fhir.patient f
  ON f.id = uuid_generate_v5(
       (SELECT uuid FROM fhir_etl.uuid_namespace WHERE name = 'Patient'),
       p.subject_id::text);

\echo
\echo '--- A2 birthDate == make_date(anchor_year,1,1) - anchor_age years ---'
SELECT
    count(*)                                          AS matched_patients
  , count(*) FILTER (WHERE pub_bd IS NULL)            AS null_birthdate
  , count(*) FILTER (WHERE pub_bd IS DISTINCT FROM exp_bd) AS wrong_birthdate
  , count(*) FILTER (WHERE extract(month from pub_bd) = 1
                       AND extract(day   from pub_bd) = 1) AS on_jan_1
  , CASE WHEN count(*) FILTER (WHERE pub_bd IS DISTINCT FROM exp_bd) = 0
          AND count(*) FILTER (WHERE pub_bd IS NULL) = 0
         THEN 'PASS' ELSE 'FAIL' END                  AS verdict
FROM v_pat;

-- A3 -- the point of the fix: an age computed from the published birthDate must
-- equal the canonical mimic-code age (demographics/age.sql:30) for every
-- admission, under BOTH computations a consumer might apply.
CREATE TEMP TABLE v_age AS
SELECT
    a.hadm_id
  , a.subject_id
  , v.anchor_age + (extract(year from a.admittime)::int - v.anchor_year) AS canonical_age
  , (extract(year from a.admittime)::int - extract(year from v.pub_bd)::int) AS pub_calendar_age
  , extract(year from age(a.admittime::date, v.pub_bd))::int              AS pub_anniversary_age
  , (extract(year from a.admittime)::int - extract(year from o.old_bd)::int) AS old_calendar_age
  , extract(year from age(a.admittime::date, o.old_bd))::int              AS old_anniversary_age
FROM mimiciv_hosp.admissions a
JOIN v_pat v ON v.subject_id = a.subject_id
-- the pre-fix formula, recomputed, so the "0 wrong" below has a before to be
-- measured against rather than being trivially satisfiable
LEFT JOIN (
    SELECT p.subject_id
         , (MIN(t.intime)::date - (p.anchor_age || ' years')::interval)::date AS old_bd
    FROM mimiciv_hosp.patients p
    JOIN mimiciv_hosp.transfers t ON t.subject_id = p.subject_id
    GROUP BY p.subject_id, p.anchor_age
) o ON o.subject_id = a.subject_id;

\echo
\echo '--- A3 age reproduction over all admissions (before vs after) ---'
SELECT
    count(*)                                                              AS admissions
  , count(*) FILTER (WHERE old_calendar_age    IS DISTINCT FROM canonical_age) AS before_calendar_wrong
  , count(*) FILTER (WHERE old_anniversary_age IS DISTINCT FROM canonical_age) AS before_anniversary_wrong
  , count(*) FILTER (WHERE pub_calendar_age    IS DISTINCT FROM canonical_age) AS after_calendar_wrong
  , count(*) FILTER (WHERE pub_anniversary_age IS DISTINCT FROM canonical_age) AS after_anniversary_wrong
  , CASE WHEN count(*) FILTER (WHERE pub_calendar_age    IS DISTINCT FROM canonical_age) = 0
          AND count(*) FILTER (WHERE pub_anniversary_age IS DISTINCT FROM canonical_age) = 0
          AND count(*) FILTER (WHERE old_calendar_age    IS DISTINCT FROM canonical_age) > 0
         THEN 'PASS' ELSE 'FAIL' END                                      AS verdict
FROM v_age;

\echo
\echo '--- A3b distinct patients affected before the fix (expect 87 / 47,899) ---'
SELECT
    count(DISTINCT subject_id) FILTER (WHERE old_calendar_age    IS DISTINCT FROM canonical_age) AS before_calendar_patients
  , count(DISTINCT subject_id) FILTER (WHERE old_anniversary_age IS DISTINCT FROM canonical_age) AS before_anniversary_patients
FROM v_age;

-- A4 -- the worked example from the commit message. The subject is not in the
-- public demo, so its id and expected birth date are passed in rather than
-- committed:  -v worked_subject=<subject_id> -v worked_expect=<YYYY-MM-DD>
\echo
\if :{?worked_subject}
\echo '--- A4 worked example subject (expect the anchor-pair birth date) ---'
SELECT subject_id, anchor_age, anchor_year, exp_bd, pub_bd
     , CASE WHEN pub_bd = :'worked_expect'::date THEN 'PASS' ELSE 'FAIL' END AS verdict
FROM v_pat WHERE subject_id = :worked_subject;
\else
\echo '--- A4 skipped: pass -v worked_subject=... -v worked_expect=... to run it ---'
\endif


-- ============================================================================
-- B.  #124 -- FHIR tables generated under UTC
-- ============================================================================
\echo
\echo '################ B. UTC timestamps (#124) ################'
\echo 'NOTE: this cluster also carries timezone=UTC in postgresql.conf (a'
\echo '      deliberate belt-and-braces deviation, hpc/psql/setup_db.sh:40), so'
\echo '      B1-B3 verify the OUTPUT is correct. B4 tests the SQL-level fix in'
\echo '      isolation by rendering under America/New_York.'

-- B1 -- how many source admittimes land in a DST discontinuity under
-- America/New_York, the zone the published v2.1 release was generated in.
-- A wall clock value is in the spring-forward gap iff it does not round-trip.
\echo
\echo '--- B1 source admissions in a NY DST discontinuity (expect 44 gap / 53 ambiguous) ---'
CREATE TEMP TABLE v_gap AS
SELECT hadm_id, subject_id, admittime
     , ((admittime AT TIME ZONE 'America/New_York') AT TIME ZONE 'America/New_York')
         IS DISTINCT FROM admittime AS in_spring_gap
-- Ambiguity cannot be detected by a round-trip: PostgreSQL resolves a repeated
-- wall clock hour to the later offset and converting back returns the same
-- value. Detect it by the offset stepping UP between t-1h and t, which happens
-- only across a fall-back transition (spring-forward steps down).
     , (  ((admittime AT TIME ZONE 'America/New_York') AT TIME ZONE 'UTC') - admittime
        > ((((admittime - interval '1 hour') AT TIME ZONE 'America/New_York') AT TIME ZONE 'UTC')
           - (admittime - interval '1 hour'))) AS in_fallback_hour
FROM mimiciv_hosp.admissions;

SELECT count(*) FILTER (WHERE in_spring_gap)    AS spring_forward_gap
     , count(*) FILTER (WHERE in_fallback_hour) AS fallback_ambiguous_hour
     , count(*)                                 AS total_admissions
FROM v_gap;

-- B2 -- every published Encounter.period must carry the source wall clock
-- digits unchanged, with a +00:00 offset.
CREATE TEMP TABLE v_enc AS
SELECT
    a.hadm_id
  , a.admittime
  , a.dischtime
  , f.fhir #>> '{period,start}' AS pub_start
  , f.fhir #>> '{period,end}'   AS pub_end
FROM mimiciv_hosp.admissions a
JOIN mimic_fhir.encounter f
  ON f.id = uuid_generate_v5(
       (SELECT uuid FROM fhir_etl.uuid_namespace WHERE name = 'Encounter'),
       a.hadm_id::text);

\echo
\echo '--- B2 Encounter.period round-trips the source wall clock ---'
SELECT
    count(*) AS encounters
  , count(*) FILTER (WHERE left(pub_start,19) = to_char(admittime,'YYYY-MM-DD"T"HH24:MI:SS')) AS start_wallclock_ok
  , count(*) FILTER (WHERE left(pub_end,19)   = to_char(dischtime,'YYYY-MM-DD"T"HH24:MI:SS')
                        OR (pub_end IS NULL AND dischtime IS NULL))                          AS end_wallclock_ok
  , count(*) FILTER (WHERE right(pub_start,6) = '+00:00')                                    AS start_offset_utc
  , count(*) FILTER (WHERE pub_end IS NOT NULL AND right(pub_end,6) = '+00:00')              AS end_offset_utc
  , count(*) FILTER (WHERE dischtime IS NULL)                                                AS null_dischtime
  , CASE WHEN count(*) FILTER (WHERE left(pub_start,19) <> to_char(admittime,'YYYY-MM-DD"T"HH24:MI:SS')) = 0
          AND count(*) FILTER (WHERE right(pub_start,6) <> '+00:00') = 0
         THEN 'PASS' ELSE 'FAIL' END AS verdict
FROM v_enc;

-- B3 -- the rows the bug was about, shown individually. Pre-fix these published
-- 03:xx; they must now read 02:xx.
\echo
\echo '--- B3 the in-gap admissions, source vs published ---'
SELECT g.hadm_id
     , to_char(g.admittime,'YYYY-MM-DD"T"HH24:MI:SS') AS source_wallclock
     , e.pub_start                                    AS published
     , CASE WHEN left(e.pub_start,19) = to_char(g.admittime,'YYYY-MM-DD"T"HH24:MI:SS')
            THEN 'ok' ELSE 'SHIFTED' END              AS status
FROM v_gap g JOIN v_enc e USING (hadm_id)
WHERE g.in_spring_gap
ORDER BY g.admittime;

-- B3b -- a second resource file, so the evidence is not one SQL file wide.
-- fhir_encounter_icu.sql, 73k rows, casts icustays.intime/outtime the same way.
\echo
\echo '--- B3b EncounterICU.period round-trips icustays (second resource file) ---'
SELECT
    count(*) AS encounter_icu
  , count(*) FILTER (WHERE left(f.fhir #>> '{period,start}',19) = to_char(i.intime,'YYYY-MM-DD"T"HH24:MI:SS')) AS start_wallclock_ok
  , count(*) FILTER (WHERE right(f.fhir #>> '{period,start}',6) = '+00:00')                                    AS start_offset_utc
  , CASE WHEN count(*) FILTER (WHERE left(f.fhir #>> '{period,start}',19)
                                     <> to_char(i.intime,'YYYY-MM-DD"T"HH24:MI:SS')) = 0
          AND count(*) FILTER (WHERE right(f.fhir #>> '{period,start}',6) <> '+00:00') = 0
         THEN 'PASS' ELSE 'FAIL' END AS verdict
FROM mimiciv_icu.icustays i
JOIN mimic_fhir.encounter_icu f
  ON f.id = uuid_generate_v5(
       (SELECT uuid FROM fhir_etl.uuid_namespace WHERE name = 'EncounterICU'),
       i.stay_id::text);

-- B4 -- isolate the SQL-level fix. Render the same values the way the resource
-- SQL does (CAST to TIMESTAMPTZ, then jsonb) under NY and under UTC. This is
-- what `SET TimeZone='UTC'` in create_fhir_tables.sql is protecting against,
-- independent of what postgresql.conf happens to say.
\echo
\echo '--- B4 mechanism: same cast rendered under NY vs UTC ---'
SET TimeZone = 'America/New_York';
CREATE TEMP TABLE v_ny AS
SELECT hadm_id, admittime
     , to_jsonb(CAST(admittime AS TIMESTAMPTZ)) #>> '{}' AS rendered_ny
FROM v_gap WHERE in_spring_gap;
SET TimeZone = 'UTC';

SELECT n.hadm_id
     , to_char(n.admittime,'YYYY-MM-DD"T"HH24:MI:SS')     AS source_wallclock
     , n.rendered_ny                                      AS under_america_new_york
     , to_jsonb(CAST(n.admittime AS TIMESTAMPTZ)) #>> '{}' AS under_utc
FROM v_ny n ORDER BY n.admittime LIMIT 10;

\echo
\echo '--- B4b how many of the in-gap rows the NY rendering corrupts ---'
SELECT count(*) AS in_gap_rows
     , count(*) FILTER (WHERE left(rendered_ny,19) <> to_char(admittime,'YYYY-MM-DD"T"HH24:MI:SS')) AS ny_shifted
     , count(*) FILTER (WHERE left(to_jsonb(CAST(admittime AS TIMESTAMPTZ)) #>> '{}',19)
                              <> to_char(admittime,'YYYY-MM-DD"T"HH24:MI:SS'))                     AS utc_shifted
     , CASE WHEN count(*) FILTER (WHERE left(rendered_ny,19) <> to_char(admittime,'YYYY-MM-DD"T"HH24:MI:SS')) > 0
             AND count(*) FILTER (WHERE left(to_jsonb(CAST(admittime AS TIMESTAMPTZ)) #>> '{}',19)
                                        <> to_char(admittime,'YYYY-MM-DD"T"HH24:MI:SS')) = 0
            THEN 'PASS' ELSE 'FAIL' END AS verdict
FROM v_ny;


-- ============================================================================
-- C.  #125 -- chartevents.value carried in a component
-- ============================================================================
\echo
\echo '################ C. chartevents component (#125) ################'

-- C1 -- one pass over mimic_fhir.observation_chartevents (~400 GB). Everything
-- the published side needs comes out of this single scan; a second scan of a
-- table this size is minutes of wall clock, so do not add one.
--   itemid = '(none)' is the bucket of rows with no component.
\echo
\echo '--- C1 scanning published observation_chartevents (single pass) ---'
CREATE TEMP TABLE v_pub_ce AS
SELECT
    COALESCE(fhir->'component'->0->'code'->'coding'->0->>'code', '(none)') AS itemid
  , count(*)                                                        AS n
  , count(*) FILTER (WHERE fhir ? 'valueQuantity')                  AS n_value_quantity
  , count(*) FILTER (WHERE fhir ? 'valueString')                    AS n_value_string
  , count(*) FILTER (WHERE fhir->'component'->0 ? 'valueString')    AS n_component_text
  , count(*) FILTER (WHERE fhir->'component'->0->'code'->'coding'->0->>'system'
                           = 'http://mimic.mit.edu/fhir/mimic/CodeSystem/mimic-chartevents-d-items')
                                                                    AS n_component_system_ok
  , count(*) FILTER (WHERE jsonb_array_length(fhir->'component') <> 1) AS n_component_len_ne_1
FROM mimic_fhir.observation_chartevents
GROUP BY 1;

-- C2 -- the same predicate evaluated on the source, with the resource SQL's own
-- WHERE clause. Two independent aggregates compared by itemid, no 313M x 313M
-- join.
\echo
\echo '--- C2 scanning source mimiciv_icu.chartevents ---'
CREATE TEMP TABLE v_src_ce AS
SELECT
    ce.itemid::text AS itemid
  , count(*) AS n_rows
  , count(*) FILTER (
        WHERE ce.valuenum IS NOT NULL
          AND CASE WHEN ce.value ~ '^\s*-?\d+(\.\d+)?\s*$'
                   THEN CAST(ce.value AS DOUBLE PRECISION) IS DISTINCT FROM ce.valuenum
                   ELSE TRUE END
    ) AS n_expect_component
FROM mimiciv_icu.chartevents ce
WHERE ((ce.stay_id = 34934165) AND (ce.charttime = '2151-10-03 05:14:00.000')) = FALSE
  AND ce.value IS NOT NULL
GROUP BY 1;

\echo
\echo '--- C3 totals ---'
SELECT
    (SELECT sum(n)                 FROM v_pub_ce)                        AS published_rows
  , (SELECT sum(n_rows)            FROM v_src_ce)                        AS source_rows_after_filter
  , (SELECT sum(n)                 FROM v_pub_ce WHERE itemid <> '(none)') AS published_with_component
  , (SELECT sum(n_expect_component) FROM v_src_ce)                       AS expected_with_component
  , (SELECT count(*)               FROM v_pub_ce WHERE itemid <> '(none)') AS itemids_with_component
  , (SELECT count(*)               FROM v_src_ce WHERE n_expect_component > 0) AS itemids_expected;

\echo
\echo '--- C3b per-itemid agreement (published component count vs source predicate) ---'
CREATE TEMP TABLE v_ce_cmp AS
SELECT COALESCE(p.itemid, s.itemid)              AS itemid
     , COALESCE(p.n, 0)                          AS published
     , COALESCE(s.n_expect_component, 0)         AS expected
FROM (SELECT * FROM v_pub_ce WHERE itemid <> '(none)') p
FULL OUTER JOIN (SELECT * FROM v_src_ce WHERE n_expect_component > 0) s
  ON p.itemid = s.itemid;

SELECT count(*)                                            AS itemids
     , count(*) FILTER (WHERE published = expected)        AS agreeing
     , count(*) FILTER (WHERE published <> expected)       AS disagreeing
     , CASE WHEN count(*) FILTER (WHERE published <> expected) = 0
            THEN 'PASS' ELSE 'FAIL' END                    AS verdict
FROM v_ce_cmp;

\echo
\echo '--- C3c any disagreeing itemids (empty == pass) ---'
SELECT c.itemid, d.label, c.published, c.expected, c.published - c.expected AS delta
FROM v_ce_cmp c LEFT JOIN mimiciv_icu.d_items d ON d.itemid = c.itemid::int
WHERE c.published <> c.expected ORDER BY abs(c.published - c.expected) DESC LIMIT 40;

\echo
\echo '--- C3d component well-formedness (every component: 1 element, right system, has text) ---'
SELECT sum(n)                            AS component_rows
     , sum(n_component_text)             AS with_valuestring
     , sum(n_component_system_ok)        AS with_correct_system
     , sum(n_component_len_ne_1)         AS array_length_not_1
     , CASE WHEN sum(n) = sum(n_component_text)
             AND sum(n) = sum(n_component_system_ok)
             AND sum(n_component_len_ne_1) = 0
            THEN 'PASS' ELSE 'FAIL' END  AS verdict
FROM v_pub_ce WHERE itemid <> '(none)';

\echo
\echo '--- C3e value[x] unchanged: components ride alongside a value, never replace it ---'
SELECT sum(n)                                    AS component_rows
     , sum(n_value_quantity)                     AS of_which_valuequantity
     , sum(n_value_string)                       AS of_which_valuestring
     , CASE WHEN sum(n) = sum(n_value_quantity) AND sum(n_value_string) = 0
            THEN 'PASS' ELSE 'FAIL' END          AS verdict
FROM v_pub_ce WHERE itemid <> '(none)';

\echo
\echo '--- C3f top 20 itemids by component count ---'
SELECT c.itemid, d.label, c.published
FROM v_ce_cmp c LEFT JOIN mimiciv_icu.d_items d ON d.itemid = c.itemid::int
ORDER BY c.published DESC LIMIT 20;

-- C4 -- the ambiguity the issue is actually about. itemid 223900 (GCS - Verbal
-- Response) publishes 'No Response-ETT' and 'No Response' both at valuenum 1.
-- Look the published resources up by primary key, derived from source, so this
-- costs an index probe rather than another full scan.
\echo
\echo '--- C4 itemid 223900: distinct source texts at valuenum 1 ---'
SELECT value, count(*) AS n
FROM mimiciv_icu.chartevents
WHERE itemid = 223900 AND valuenum = 1 AND value IS NOT NULL
GROUP BY value ORDER BY n DESC;

\echo
\echo '--- C4b those same rows as published (one example each) ---'
WITH samp AS (
    SELECT DISTINCT ON (ce.value)
           ce.stay_id, ce.charttime, ce.itemid, ce.value, ce.valuenum
    FROM mimiciv_icu.chartevents ce
    WHERE ce.itemid = 223900 AND ce.valuenum = 1 AND ce.value IS NOT NULL
    ORDER BY ce.value, ce.charttime
)
SELECT s.value                                            AS source_text
     , s.valuenum                                         AS source_valuenum
     , o.fhir->'valueQuantity'->>'value'                  AS published_value
     , o.fhir->'component'->0->>'valueString'             AS published_component_text
     , CASE WHEN o.fhir->'component'->0->>'valueString' = s.value
            THEN 'ok' ELSE 'MISSING' END                  AS status
FROM samp s
CROSS JOIN (SELECT uuid FROM fhir_etl.uuid_namespace WHERE name = 'ObservationChartevents') ns
JOIN mimic_fhir.observation_chartevents o
  ON o.id = uuid_generate_v5(ns.uuid,
       s.stay_id || '-' || s.charttime || '-' || s.itemid || '-' || s.value)
ORDER BY s.value;

\echo
\echo '--- C4c full published resource for one 223900 row ---'
WITH samp AS (
    SELECT ce.stay_id, ce.charttime, ce.itemid, ce.value
    FROM mimiciv_icu.chartevents ce
    WHERE ce.itemid = 223900 AND ce.valuenum = 1 AND ce.value = 'No Response-ETT'
    ORDER BY ce.charttime LIMIT 1
)
SELECT jsonb_pretty(o.fhir)
FROM samp s
CROSS JOIN (SELECT uuid FROM fhir_etl.uuid_namespace WHERE name = 'ObservationChartevents') ns
JOIN mimic_fhir.observation_chartevents o
  ON o.id = uuid_generate_v5(ns.uuid,
       s.stay_id || '-' || s.charttime || '-' || s.itemid || '-' || s.value);

\echo
\echo '################ done ################'
