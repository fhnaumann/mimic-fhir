-- Macro assertions. driver.py runs this after loading the runtime and refuses to
-- build if anything here fails. Each row that violates its check raises.
--
-- The first two vectors are the same ones hpc/psql/setup_db.sh asserts against
-- uuid-ossp. That shared pair is what ties both backends to one identity scheme:
-- if DuckDB and Postgres agree on them, all 198 uuid_generate_v5 call sites agree.

CREATE OR REPLACE TEMP MACRO assert_eq(name, got, want) AS (
  CASE WHEN got IS NOT DISTINCT FROM want THEN 'ok'
       ELSE error('SELFTEST FAILED: ' || name || ' got=' || COALESCE(got::VARCHAR, '<null>')
                                      || ' want=' || COALESCE(want::VARCHAR, '<null>'))
  END
);

SELECT
  -- RFC 4122 v5 published test vector
  assert_eq('uuid_v5 rfc4122',
        uuid_v5('6ba7b810-9dad-11d1-80b4-00c04fd430c8', 'www.example.com'),
        '2ed6657d-e927-568b-95e1-2665a8aea6a2'),

  -- The MIMIC-IV root namespace: uuid_generate_v5(uuid_ns_oid(), 'MIMIC-IV')
  assert_eq('uuid_v5 mimic root',
        uuid_v5(ns_oid(), 'MIMIC-IV'),
        '24ba6d92-ae8e-56f9-8898-873d8cba02da'),

  -- One level deeper, as fhir_etl/uuid_namespace.sql builds every namespace
  assert_eq('uuid_v5 patient ns',
        uuid_v5(uuid_v5(ns_oid(), 'MIMIC-IV'), 'Patient'),
        '569c1027-384a-58ff-84f2-0338bab99bc1'),

  -- Postgres renders a jsonb timestamptz exactly like this
  assert_eq('fhir_instant format',
        fhir_instant(TIMESTAMP '2180-07-23 14:00:00'),
        '2180-07-23T14:00:00+00:00'),

  -- Must stay NULL so strip() removes the key rather than emitting "null"
  assert_eq('fhir_instant null',
        fhir_instant(NULL),
        NULL),

  -- A session timezone must not be able to reach the rendering (issue #124)
  assert_eq('fhir_instant tz-proof',
        fhir_instant(CAST('2180-07-23 14:00:00' AS TIMESTAMP)),
        '2180-07-23T14:00:00+00:00'),

  assert_eq('strip drops nulls',
        strip(json_object('a', NULL, 'b', 1))::VARCHAR,
        '{"b":1}'),

  assert_eq('strip recurses objects',
        strip(json_object('a', json_object('b', NULL, 'c', 1)))::VARCHAR,
        '{"a":{"c":1}}'),

  -- The bare DECIMAL cast this replaces would give 0.000 -- silent data loss
  assert_eq('fhir_decimal precision',
        fhir_decimal('0.000001')::VARCHAR,
        '1e-06'),

  assert_eq('fhir_decimal value',
        fhir_decimal('98.6'),
        98.6),

  -- sqlglot's own ARRAY_REMOVE translation returns [] here. This is the assertion
  -- that would have caught Encounter.location silently emptying on every ICU stay.
  assert_eq('list_denull keeps non-nulls',
        list_denull(['x', NULL, 'y'])::VARCHAR,
        '[x, y]'),

  assert_eq('list_denull all-null',
        list_denull([NULL, NULL])::VARCHAR,
        '[]'),

  -- med_GSN = '0' collapses the whole call to NULL. Inherited, intentional-looking.
  assert_eq('fn_med_statement gsn zero',
        fn_med_statement('0', '123', '[null]'),
        NULL),

  -- No matching map_ethnicity row -> NULL, even though birthsex is present
  assert_eq('fn_patient_extension unmatched',
        fn_patient_extension(NULL, 'NOT A REAL ETHNICITY', 'M'),
        NULL)
;
