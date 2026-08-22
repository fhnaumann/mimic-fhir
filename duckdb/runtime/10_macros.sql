-- DuckDB runtime macros for the MIMIC-IV-on-FHIR mappings.
--
-- These are the backend-specific seam. Everything else is generated from sql/*.sql
-- by transpile.py. Keep this file small and keep nothing backend-specific outside it.
--
-- Every definition here is pinned by an assertion in 99_selftest.sql. If you change one,
-- change the assertion in the same commit.

-- ---------------------------------------------------------------------------
-- uuid_generate_v5 (uuid-ossp) equivalent, pure SQL and fully vectorized.
--
-- RFC 4122 v5: sha1(namespace_bytes || name), then stamp version 5 into the high
-- nibble of octet 6 and the variant bits into the top two bits of octet 8.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE MACRO uuid_v5(ns, name) AS (
  WITH h AS (
    SELECT sha1(from_hex(replace(ns::VARCHAR, '-', '')) || encode(name)) AS s
  )
  SELECT substr(s, 1, 8) || '-' || substr(s, 9, 4) || '-5' || substr(s, 14, 3) || '-'
      || format('{:x}', ((('0x' || substr(s, 17, 1))::INT & 3) + 8))
      || substr(s, 18, 3) || '-' || substr(s, 21, 12)
  FROM h
);

-- uuid_ns_oid() from uuid-ossp: the RFC 4122 OID namespace constant.
CREATE OR REPLACE MACRO ns_oid() AS '6ba7b812-9dad-11d1-80b4-00c04fd430c8';

-- ---------------------------------------------------------------------------
-- jsonb_strip_nulls equivalent.
--
-- JSON Merge Patch against an empty object drops null-valued keys and recurses
-- into nested OBJECTS, but never into ARRAYS. Postgres jsonb_strip_nulls does
-- recurse into arrays, so transpile.py additionally wraps objects that are direct
-- elements of an array (rule 1b). Do not assume one strip() at the top is enough.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE MACRO strip(j) AS json_merge_patch('{}'::JSON, j);

-- ---------------------------------------------------------------------------
-- CAST(x AS TIMESTAMPTZ) equivalent, for values that land in a JSON document.
--
-- Postgres renders a jsonb timestamptz as 2180-07-23T14:00:00+00:00. DuckDB
-- renders a TIMESTAMPTZ in the SESSION timezone with a space separator, which is
-- both a format difference and the DST hazard that issue #124 fixed.
--
-- Casting to a naive TIMESTAMP first is what makes this timezone-proof: no session
-- setting can reach it. MIMIC timestamps are wall-clock values with no zone, and
-- the pipeline declares them UTC, so this is a relabel and not a conversion.
--
-- Returns NULL on NULL, so strip() still removes the key.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE MACRO fhir_instant(t) AS
  strftime(CAST(t AS TIMESTAMP), '%Y-%m-%dT%H:%M:%S+00:00');

-- ---------------------------------------------------------------------------
-- CAST(x AS NUMERIC) / CAST(x AS DECIMAL) equivalent.
--
-- DuckDB's bare DECIMAL is DECIMAL(18,3). Postgres NUMERIC is arbitrary precision.
-- Every cast site in sql/ is text -> numeric on a lab or medication value, so the
-- bare cast would silently truncate: '0.000001' becomes 0.000, no error raised.
--
-- DOUBLE keeps the value. It does not reproduce Postgres's trailing zeros
-- ('5.20' renders as 5.2, '7' as 7.0); those are the same FHIR decimal, and
-- parity.py compares numerics as decimals rather than as strings.
--
-- Deliberately CAST and not TRY_CAST: a value the mapping's guards fail to catch
-- should error loudly rather than become a silent null.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE MACRO fhir_decimal(x) AS CAST(x AS DOUBLE);

-- ---------------------------------------------------------------------------
-- ARRAY_REMOVE(a, NULL) equivalent.
--
-- Do NOT let sqlglot translate ARRAY_REMOVE itself. Its built-in duckdb rule
-- emits list_filter(a, _u -> _u <> NULL), and `x <> NULL` is NULL for every x,
-- so the filter keeps nothing and the list comes back EMPTY. Measured:
--   list_filter(['x', NULL, 'y'], _u -> _u <> NULL)     -> []
--   list_filter(['x', NULL, 'y'], lambda v: v IS NOT NULL) -> [x, y]
-- The only call site is Encounter.location in fhir_encounter_icu.sql, so that
-- mistranslation would silently drop the location array from every ICU
-- encounter without erroring. transpile.py rule 8 routes around it to here.
--
-- Wrapping the lambda in a macro also keeps DuckDB's lambda syntax churn in
-- this file: `->` is deprecated in 1.5 and warns on every call.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE MACRO list_denull(a) AS list_filter(a, lambda v: v IS NOT NULL);
