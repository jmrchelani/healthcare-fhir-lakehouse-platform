select
    encounter_id,
    patient_id,
    provider_id,
    encounter_type,
    admit_time,
    discharge_time,
    hospital_unit
from {{ source('public', 'silver_encounters') }}