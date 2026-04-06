SELECT
    encounter_id,
    patient_id,
    encounter_type,
    admit_time,
    discharge_time
FROM {{ source('public', 'silver_encounters') }}