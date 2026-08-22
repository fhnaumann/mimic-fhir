-- Ports of the five plpgsql functions in sql/fn/.
--
-- These are the only hand-written translations in the port; everything else is
-- generated. They are also the most fragile code in the source repo — all but one
-- build JSON by string concatenation with manual comma placement, and several rely
-- on NULL propagating through `||` to collapse the whole result to NULL.
--
-- THAT NULL COLLAPSE IS LOAD-BEARING AND IS PRESERVED EXACTLY. Where a CASE has no
-- ELSE, the original returns NULL for the entire call. It looks like a bug in at
-- least two places. Do not fix it here — that is a mapping change, not a port, and
-- it would silently alter published output. duckdb/tests/test_functions.py pins the
-- current behaviour against the real plpgsql running in Postgres.
--
-- LOAD ORDER: this file must be loaded AFTER the transpiled fhir_etl/*.sql, because
-- fn_patient_extension resolves fhir_etl.map_ethnicity and map_race_omb at CREATE time,
-- not at call time. driver.py enforces the order; see its LOAD_ORDER constant.
--
-- fn_create_table_patient_dependent is deliberately absent: it only created tables
-- and indexes, and the DuckDB pipeline writes files. transpile.py drops its 25 call
-- sites (rule 10).

-- ---------------------------------------------------------------------------
-- fn_patient_extension(race, ethnicity, birthsex) -> JSON array
--
-- Called once, from fhir_patient.sql, as fn_patient_extension(adm_RACE, adm_RACE, ...)
-- — race and ethnicity are the same value.
--
-- Two inherited behaviours worth naming, both preserved:
--
--  1. The source does `WHERE map_eth.mimic_ethnicity = ethnicity`, so an unmatched
--     or NULL ethnicity yields no row, SELECT INTO leaves the variable NULL, and the
--     function returns NULL. A patient with no race therefore loses the us-core-birthsex
--     extension too, even though birthsex is present.
--  2. The opening '[' guard tests fhir_ethnicity_code while the closing ']' guard tests
--     ethnicity. The asymmetry is almost certainly unintended; both guards are encoded
--     below so the collapse behaviour matches whatever the source does.
--
-- Unlike the source, this builds a real list rather than concatenating text, and
-- strip()s each object that is a direct array element. Postgres relies on the caller's
-- jsonb_strip_nulls recursing into this array to clean up the LEFT JOIN's NULL race
-- codes; DuckDB's strip() does not recurse into arrays, so it has to happen here.
--
-- THE LOOKUPS ARE MAPS, NOT A JOIN, AND THAT IS NOT A STYLE CHOICE. Written the
-- obvious way -- `SELECT ... FROM map_ethnicity WHERE mimic_ethnicity = ethnicity`
-- -- the macro body is a CORRELATED scalar subquery, and DuckDB cannot plan one
-- inside a query that also has an outer join:
--     Not Implemented Error: Non-inner join on correlated columns not supported
-- fhir_patient.sql has seven LEFT JOINs, so the correlated form fails at the only
-- call site there is. Building the lookup as a constant MAP moves the table
-- reference into an UNCORRELATED subquery -- evaluated once, no correlation to
-- decorrelate -- and the correlated part becomes a plain map index.
--
-- The map reproduces the join's three-way outcome exactly, which is what makes
-- this a translation and not a behaviour change:
--     no matching row        -> map[k] IS NULL      (was: zero rows -> NULL call)
--     matching row, NULL code-> struct with NULL fields, itself NOT NULL
--     matching key is NULL   -> map[NULL] IS NULL
-- The middle case is the one that matters: 'UNKNOWN' has a row with a NULL
-- fhir_ethnicity_code, and it must stay distinguishable from having no row at all.
-- Both lookup tables are keyed 33/33 unique, so no entry is lost to a map collision
-- -- and a duplicate would already have been an error in the source, since the
-- scalar subquery form raises on more than one row.
-- ---------------------------------------------------------------------------

-- The lookup rows as constant maps. Uncorrelated on purpose: the subquery must not
-- mention a macro parameter, or the decorrelation problem comes straight back.
CREATE OR REPLACE MACRO map_eth_row(k) AS (
  (SELECT map_from_entries(list({'k': mimic_ethnicity, 'v': {
       'code':    fhir_ethnicity_code,
       'display': fhir_ethnicity_display,
       'system':  fhir_system}}))
   FROM fhir_etl.map_ethnicity)[k]
);

CREATE OR REPLACE MACRO map_race_row(k) AS (
  (SELECT map_from_entries(list({'k': mimic_race, 'v': {
       'code':    fhir_race_omb_code,
       'display': fhir_race_omb_display,
       'system':  fhir_system}}))
   FROM fhir_etl.map_race_omb)[k]
);

CREATE OR REPLACE MACRO fn_patient_extension(race, ethnicity, birthsex) AS (
    CASE
      -- The source's WHERE clause: no matching map_ethnicity row means no rows at
      -- all, so the whole call is NULL. This is behaviour 1 above, preserved.
      WHEN map_eth_row(ethnicity) IS NULL THEN NULL
      WHEN ((race IS NOT NULL) OR (map_eth_row(ethnicity).code IS NOT NULL) OR (birthsex IS NOT NULL))
       AND ((race IS NOT NULL) OR (ethnicity IS NOT NULL) OR (birthsex IS NOT NULL))
      THEN to_json(list_filter([
             CASE WHEN race IS NOT NULL THEN strip(json_object(
                 'extension', json_array(
                     strip(json_object(
                         'url', 'ombCategory',
                         'valueCoding', json_object(
                             'code',    map_race_row(race).code,
                             'display', map_race_row(race).display,
                             'system',  map_race_row(race).system
                         )
                     )),
                     strip(json_object(
                         'url', 'text',
                         'valueString', map_race_row(race).display
                     ))
                 ),
                 'url', 'http://hl7.org/fhir/us/core/StructureDefinition/us-core-race'
             )) END,
             CASE WHEN map_eth_row(ethnicity).code IS NOT NULL THEN strip(json_object(
                 'extension', json_array(
                     strip(json_object(
                         'url', 'ombCategory',
                         'valueCoding', json_object(
                             'code',    map_eth_row(ethnicity).code,
                             'display', map_eth_row(ethnicity).display,
                             'system',  map_eth_row(ethnicity).system
                         )
                     )),
                     strip(json_object(
                         'url', 'text',
                         'valueString', map_eth_row(ethnicity).display
                     ))
                 ),
                 'url', 'http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity'
             )) END,
             CASE WHEN birthsex IS NOT NULL THEN strip(json_object(
                 'valueCode', birthsex,
                 'url', 'http://hl7.org/fhir/us/core/StructureDefinition/us-core-birthsex'
             )) END
           ], lambda x: x IS NOT NULL))
    END
);

-- ---------------------------------------------------------------------------
-- fn_med_statement(med_GSN, med_NDC, med_ETC_CODES) -> JSON array
--
-- Transliterated rather than rebuilt as a list, because med_ETC_CODES is not a JSON
-- document — it is a raw comma-separated fragment from STRING_AGG in
-- fhir_medication_statement_ed.sql ("{...},{...}"), spliced directly into the array.
--
-- The med_GSN = '0' path returns NULL for the whole call: the leading CASE has no
-- ELSE, so '[' is NULL and `||` propagates it. The second CASE has an explicit
-- ELSE NULL doing the same thing. Both look intentional. Preserved.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE MACRO fn_med_statement(med_GSN, med_NDC, med_ETC_CODES) AS (
  CASE WHEN (med_GSN != '0') THEN '[' END
  || CASE WHEN med_GSN != '0'
       THEN json_object('code', med_GSN,
                        'system', 'http://mimic.mit.edu/fhir/mimic/CodeSystem/mimic-medication-gsn')::VARCHAR
       ELSE NULL
     END
  || CASE WHEN med_NDC != '0'
       THEN ',' || json_object('code', med_NDC, 'system', 'http://hl7.org/fhir/sid/ndc')::VARCHAR
       ELSE ''
     END
  || CASE WHEN med_ETC_CODES != '[null]'
       THEN ',' || med_ETC_CODES
       ELSE ''
     END
  || CASE WHEN (med_GSN != '0') THEN ']' END
)::JSON;

-- ---------------------------------------------------------------------------
-- fn_build_medication_identifier(ndc, formulary_drug_cd, drug) -> JSON array
--
-- Always brackets, so no NULL collapse here — but `drug != ''` is NULL when drug is
-- NULL, which sends the last CASE to ELSE '' and can yield an empty array "[]".
-- The source comment claims drug is never NULL. Preserved either way.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE MACRO fn_build_medication_identifier(ndc, formulary_drug_cd, drug) AS (
  '['
  || CASE WHEN ndc IS NOT NULL AND ndc != '0' AND ndc != ''
       THEN json_object('system', 'http://mimic.mit.edu/fhir/mimic/CodeSystem/mimic-medication-ndc',
                        'value', ndc)::VARCHAR
       ELSE ''
     END
  || CASE WHEN (ndc IS NOT NULL AND ndc != '0' AND ndc != '')
            AND (formulary_drug_cd IS NOT NULL AND formulary_drug_cd != '')
       THEN ',' ELSE '' END
  || CASE WHEN formulary_drug_cd IS NOT NULL AND formulary_drug_cd != ''
       THEN json_object('system', 'http://mimic.mit.edu/fhir/mimic/CodeSystem/mimic-medication-formulary-drug-cd',
                        'value', formulary_drug_cd)::VARCHAR
       ELSE ''
     END
  || CASE WHEN ((ndc IS NOT NULL AND ndc != '0' AND ndc != '')
                 OR (formulary_drug_cd IS NOT NULL AND formulary_drug_cd != ''))
                AND drug != ''
       THEN ',' ELSE '' END
  || CASE WHEN drug != ''
       THEN json_object('system', 'http://mimic.mit.edu/fhir/mimic/CodeSystem/mimic-medication-name',
                        'value', drug)::VARCHAR
       ELSE ''
     END
  || ']'
)::JSON;

-- ---------------------------------------------------------------------------
-- fn_prescriptions_medication_code(ndc, formulary_drug_cd, drug) -> JSON object
--
-- The only one of the five that is a plain CASE with no concatenation.
-- regexp_replace's 'g' flag is spelled the same in DuckDB.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE MACRO fn_prescriptions_medication_code(ndc, formulary_drug_cd, drug) AS (
  CASE
    WHEN ndc IS NOT NULL AND ndc != '0' AND ndc != '' THEN
      json_object('system', 'http://mimic.mit.edu/fhir/mimic/CodeSystem/mimic-medication-ndc',
                  'code', ndc)
    WHEN formulary_drug_cd IS NOT NULL AND formulary_drug_cd != '' THEN
      json_object('system', 'http://mimic.mit.edu/fhir/mimic/CodeSystem/mimic-medication-formulary-drug-cd',
                  'code', trim(regexp_replace(formulary_drug_cd, '\s+', ' ', 'g')))
    ELSE
      json_object('system', 'http://mimic.mit.edu/fhir/mimic/CodeSystem/mimic-medication-name',
                  'code', trim(regexp_replace(drug, '\s+', ' ', 'g')))
  END
);
