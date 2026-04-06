select
    patient_id,
    given_name,
    family_name,
    gender,
    birth_date,
    city,
    source_system
from {{ source('public', 'silver_patients') }}