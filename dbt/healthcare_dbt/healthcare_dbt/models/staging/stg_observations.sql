select
    observation_id,
    patient_id,
    encounter_id,
    observation_code,
    observation_value,
    unit,
    effective_time,
    abnormal_flag
from {{ source('public', 'silver_observations') }}