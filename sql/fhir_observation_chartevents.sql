-- Purpose: Generate a FHIR Observation resource for each chartevents row
-- Methods: uuid_generate_v5 --> requires uuid or text input, some inputs cast to text to fit

SELECT fhir_etl.fn_create_table_patient_dependent('observation_chartevents');

WITH fhir_observation_ce as (
    SELECT
        CAST(ce.itemid AS TEXT) AS ce_ITEMID
        , CAST(ce.charttime AS TIMESTAMPTZ) AS ce_CHARTTIME
        , CAST(ce.storetime AS TIMESTAMPTZ) AS ce_STORETIME
        , ce.valueuom AS ce_VALUEUOM
        , ce.valuenum AS ce_VALUENUM
        , ce.value AS ce_VALUE
        , di.label AS di_LABEL
        , di.category AS di_CATEGORY
        , di.lownormalvalue AS di_LOWNORMALVALUE
        , di.highnormalvalue AS di_HIGHNORMALVALUE

        -- reference uuids
        -- chartevents uuid dependent on 'value' to be unique (stay_id and itemid should be enough but a couple cases break this)
        , uuid_generate_v5(ns_observation_ce.uuid, ce.stay_id || '-' || ce.charttime || '-' || ce.itemid || '-' ||ce.value) AS uuid_CHARTEVENTS
        , uuid_generate_v5(ns_patient.uuid, CAST(ce.subject_id AS text)) AS uuid_SUBJECT_ID
        , uuid_generate_v5(ns_encounter_icu.uuid, CAST(ce.stay_id AS text)) AS uuid_STAY_ID
    FROM
        mimiciv_icu.chartevents ce
        LEFT JOIN mimiciv_icu.d_items di
            ON ce.itemid = di.itemid
        LEFT JOIN fhir_etl.uuid_namespace ns_encounter_icu
            ON ns_encounter_icu.name = 'EncounterICU'
        LEFT JOIN fhir_etl.uuid_namespace ns_patient
            ON ns_patient.name = 'Patient'
        LEFT JOIN fhir_etl.uuid_namespace ns_observation_ce
            ON ns_observation_ce.name = 'ObservationChartevents'
    WHERE   
        -- filter out the one duplicate value (one patient at one charttime)
        ((stay_id = 34934165) AND (charttime = '2151-10-03 05:14:00.000')) = FALSE
        AND value IS NOT NULL -- one value in the whole TABLE
)
INSERT INTO mimic_fhir.observation_chartevents
SELECT 
    uuid_CHARTEVENTS AS id
    , uuid_SUBJECT_ID AS patient_id
    , jsonb_strip_nulls(jsonb_build_object(
        'resourceType', 'Observation'
        , 'id', uuid_CHARTEVENTS
        , 'meta', jsonb_build_object(
            'profile', jsonb_build_array(
                'http://mimic.mit.edu/fhir/mimic/StructureDefinition/mimic-observation-chartevents'
            )
        )
        , 'status', 'final' -- All observations considered final
        , 'category', jsonb_build_array(jsonb_build_object(
            'coding', jsonb_build_array(jsonb_build_object(
                'system', 'http://mimic.mit.edu/fhir/mimic/CodeSystem/mimic-observation-category'
                , 'code', di_CATEGORY
            ))
        ))
        , 'code', jsonb_build_object(
            'coding', jsonb_build_array(jsonb_build_object(
                'system', 'http://mimic.mit.edu/fhir/mimic/CodeSystem/mimic-chartevents-d-items'
                , 'code', ce_ITEMID
                , 'display', di_LABEL
            ))
        )
        , 'subject', jsonb_build_object('reference', 'Patient/' || uuid_SUBJECT_ID)
        , 'encounter', jsonb_build_object('reference', 'Encounter/' || uuid_STAY_ID) 
        , 'effectiveDateTime', ce_CHARTTIME
        , 'issued', ce_STORETIME -- issued element is the instant the observation was available
        , 'valueQuantity',
            CASE WHEN ce_VALUENUM IS NOT NULL THEN
                jsonb_build_object(
                    'value', ce_VALUENUM
                    , 'unit', ce_VALUEUOM
                    , 'system', 'http://mimic.mit.edu/fhir/mimic/CodeSystem/mimic-units'
                    , 'code', ce_VALUEUOM 
            ) ELSE NULL END
        , 'valueString',
            CASE WHEN ce_VALUENUM IS NULL THEN
                ce_VALUE
            ELSE NULL END
        -- When a row carries both a number and text, valueQuantity above takes value[x] and
        -- the verbatim chartevents.value would otherwise be discarded. For 147 itemids the
        -- text carries meaning the number does not: itemid 223900 (GCS - Verbal Response)
        -- publishes both 'No Response-ETT' and 'No Response' as valuenum 1, and all seven
        -- domains of itemid 229381 (Orientation) share valuenum 1, so the number alone is
        -- ambiguous. Carry the source text in a component, coded with the same d-items
        -- coding as Observation.code - the component records the source representation of
        -- that same item. This mirrors the component use in fhir_observation_vitalsigns.sql,
        -- and needs no new terminology or profile artifact.
        --
        -- The component is emitted only where the text is not the number restated. On most
        -- rows value is just valuenum as a string ('87' against 87 for a heart rate), where
        -- the component would be pure redundancy on hundreds of millions of resources. The
        -- inner CASE guards the cast, so only text already matching the numeric pattern is
        -- cast; anything else (units, commas, scientific notation) fails the pattern and is
        -- carried, which is the safe direction.
        , 'component',
            CASE WHEN ce_VALUENUM IS NOT NULL AND ce_VALUE IS NOT NULL
                   AND CASE WHEN ce_VALUE ~ '^\s*-?\d+(\.\d+)?\s*$'
                            THEN CAST(ce_VALUE AS DOUBLE PRECISION) IS DISTINCT FROM ce_VALUENUM
                            ELSE TRUE END
            THEN
                jsonb_build_array(jsonb_build_object(
                    'code', jsonb_build_object(
                        'coding', jsonb_build_array(jsonb_build_object(
                            'system', 'http://mimic.mit.edu/fhir/mimic/CodeSystem/mimic-chartevents-d-items'
                            , 'code', ce_ITEMID
                            , 'display', di_LABEL
                        ))
                    )
                    , 'valueString', ce_VALUE
                ))
            ELSE NULL END
        , 'referenceRange',
            CASE WHEN di_LOWNORMALVALUE IS NOT NULL OR di_HIGHNORMALVALUE IS NOT NULL THEN
                jsonb_build_array(jsonb_build_object(
                    'low',
                        CASE WHEN di_LOWNORMALVALUE IS NOT NULL THEN
                            jsonb_build_object(
                                'value', di_LOWNORMALVALUE
                                , 'unit', ce_VALUEUOM
                                , 'system', 'http://mimic.mit.edu/fhir/mimic/CodeSystem/mimic-units'
                                , 'code', ce_VALUEUOM
                            )
                        ELSE NULL END
                    , 'high',
                        CASE WHEN di_HIGHNORMALVALUE IS NOT NULL THEN
                            jsonb_build_object(
                                'value', di_HIGHNORMALVALUE
                                , 'unit', ce_VALUEUOM
                                , 'system', 'http://mimic.mit.edu/fhir/mimic/CodeSystem/mimic-units'
                                , 'code', ce_VALUEUOM
                            )
                        ELSE NULL END
        )) ELSE NULL END
    )) AS fhir
FROM
    fhir_observation_ce;
