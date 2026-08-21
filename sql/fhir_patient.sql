-- Purpose: Generate a FHIR Patient resource for each row in patients
-- Methods: uuid_generate_v5 --> requires uuid or text input, some inputs cast to text to fit

DROP TABLE IF EXISTS mimic_fhir.patient;
CREATE TABLE mimic_fhir.patient(
    id      uuid PRIMARY KEY,
    fhir    jsonb NOT NULL 
);

-- Get the latest admission information/demographics
WITH tb_admissions AS (
    SELECT
        pat.subject_id
        -- Set a birth date from the anchor pair, since MIMIC no longer provides a birth date.
        -- anchor_age is the patient's age in anchor_year, and mimic-code defines age as
        -- anchor_age + DATETIME_DIFF(admittime, DATETIME(anchor_year,1,1), YEAR), so anchoring
        -- to Jan 1 of anchor_year reproduces that definition exactly. Jan 1 also makes the
        -- calendar-year and elapsed-time (anniversary rule) age computations agree, which they
        -- do not for an arbitrary month and day.
        , CAST(MAKE_DATE(pat.anchor_year, 1, 1) - CAST(pat.anchor_age || 'years' AS INTERVAL) AS DATE) AS pat_BIRTH_DATE
        , MIN(adm.marital_status) AS adm_MARITAL_STATUS
        , MIN(adm.race) AS adm_RACE
        , MIN(adm.language) AS adm_LANGUAGE
    FROM
        mimiciv_hosp.patients pat
        -- transfers is no longer joined: the birth date came from MIN(tfs.intime), and the
        -- INNER JOIN meant a patient with no transfer row got no birthDate at all. The anchor
        -- pair is on the patients row itself and is populated for every patient, including
        -- ED-only stays, so it has strictly better coverage and needs no join.
        -- Grab latest admittime to get the latest demographic info
        LEFT JOIN (SELECT subject_id, MAX(admittime) AS admittime
            FROM mimiciv_hosp.admissions
                GROUP BY subject_id) adm_max
            ON pat.subject_id = adm_max.subject_id
        LEFT JOIN mimiciv_hosp.admissions adm
            ON adm_max.subject_id = adm.subject_id
            AND adm_max.admittime = adm.admittime
    GROUP BY
        pat.subject_id
        , pat.anchor_age
        , pat.anchor_year
), ed_race AS (
    SELECT 
        ed.subject_id
        , ed.race
    FROM (
        SELECT 
            ed.subject_id,
            MAX(ed.outtime) OVER (PARTITION BY ed.subject_id) max_outtime,
            ROW_NUMBER() OVER (PARTITION BY ed.subject_id ORDER BY ed.outtime DESC) rn,
            race
        FROM mimiciv_ed.edstays ed
    ) AS ed
    WHERE rn = 1
), fhir_patient AS (
    SELECT
        CAST(pat.subject_id AS TEXT) AS pat_SUBJECT_ID
        , mg.fhir_gender AS pat_GENDER
        , pat.gender AS pat_BIRTHSEX
        , pat.dod AS pat_DOD
        , 'Patient_' || pat.subject_id as pat_NAME -- generate patient name
        , adm.pat_BIRTH_DATE
        , uuid_generate_v5(ns_patient.uuid, CAST(pat.subject_id AS TEXT)) AS UUID_patient
        
        , mms.fhir_system AS mms_FHIR_SYSTEM
        , mms.fhir_marital_status AS mms_FHIR_MARITAL_STATUS
        , COALESCE(adm.adm_RACE, ed.race) AS adm_RACE
        , CASE WHEN adm.adm_LANGUAGE = 'ENGLISH' THEN 'en'
          ELSE NULL END as adm_LANGUAGE
        , uuid_generate_v5(ns_organization.uuid, 'http://hl7.org/fhir/sid/us-npi/1194052720') AS  UUID_organization
    FROM  
        mimiciv_hosp.patients pat
        LEFT JOIN tb_admissions adm
            ON pat.subject_id = adm.subject_id
        LEFT JOIN ed_race ed
            ON pat.subject_id = ed.subject_id
        LEFT JOIN fhir_etl.uuid_namespace ns_patient
            ON ns_patient.name = 'Patient'
        LEFT JOIN fhir_etl.uuid_namespace ns_organization
            ON ns_organization.name = 'Organization'
        LEFT JOIN fhir_etl.map_gender mg
            ON pat.gender = mg.mimic_gender
        LEFT JOIN fhir_etl.map_marital_status mms 
            ON adm.adm_MARITAL_STATUS = mms.mimic_marital_status
            OR adm.adm_MARITAL_STATUS IS NULL AND mms.mimic_marital_status IS NULL
)

INSERT INTO mimic_fhir.patient
SELECT 
    UUID_patient AS id
    , jsonb_strip_nulls(jsonb_build_object(
        'resourceType', 'Patient'
        , 'meta', jsonb_build_object(
            'profile', jsonb_build_array(
                'http://mimic.mit.edu/fhir/mimic/StructureDefinition/mimic-patient'
            )
        ) 
        , 'gender', pat_GENDER
        , 'name', jsonb_build_array(jsonb_build_object(
            'use', 'official'
            , 'family', pat_NAME    
        ))		
        , 'identifier',  jsonb_build_array(jsonb_build_object(
            'value', pat_SUBJECT_ID
            , 'system', 'http://mimic.mit.edu/fhir/mimic/identifier/patient'
        ))		
        , 'maritalStatus', 
            CASE WHEN mms_FHIR_MARITAL_STATUS IS NOT NULL THEN
                jsonb_build_object(
                    'coding', jsonb_build_array(jsonb_build_object(
                        'system', mms_FHIR_SYSTEM
                        , 'code', mms_FHIR_MARITAL_STATUS
                    ))
                )
            ELSE NULL END
        , 'birthDate', pat_BIRTH_DATE
        , 'deceasedDateTime', pat_DOD
        , 'extension', fhir_etl.fn_patient_extension(adm_RACE, adm_RACE, pat_BIRTHSEX)
    
        -- Set preferred language if present
        , 'communication',
            CASE WHEN adm_LANGUAGE IS NOT NULL THEN
                jsonb_build_array(jsonb_build_object(
                    'language', jsonb_build_object(
                        'coding', jsonb_build_array(jsonb_build_object(
                            'system', 'urn:ietf:bcp:47'
                            , 'code', adm_LANGUAGE
                        ))                        
                    )
                ))              
            ELSE NULL END
        , 'id', UUID_patient
        , 'managingOrganization', json_build_object('reference', 'Organization/' || UUID_organization)
    )) AS fhir
FROM 
    fhir_patient
