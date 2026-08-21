-- Match the timezone the full build runs under, so regenerating a single resource
-- cannot reintroduce the DST spring-forward shift. See create_fhir_tables.sql.
SET TimeZone = 'UTC';

\set outputdir '/tmp/mimic_output'
\set fhir_resource 'procedure_icu'
\! mkdir -p '/tmp/mimic_output'
\t

\! echo Creating table
\i fhir_:fhir_resource.sql

\! echo Generating json
\o :outputdir/:fhir_resource.json
SELECT fhir FROM mimic_fhir.:fhir_resource;