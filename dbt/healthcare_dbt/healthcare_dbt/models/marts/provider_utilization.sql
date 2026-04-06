with encounters as (

    select * from {{ ref('stg_encounters') }}

),

provider_summary as (

    select
        provider_id,
        count(*) as total_encounters,
        count(distinct patient_id) as total_patients_served,
        sum(case when encounter_type = 'inpatient' then 1 else 0 end) as inpatient_encounters,
        sum(case when encounter_type = 'outpatient' then 1 else 0 end) as outpatient_encounters,
        sum(case when encounter_type = 'emergency' then 1 else 0 end) as emergency_encounters,
        avg(
            extract(epoch from (discharge_time - admit_time)) / 3600.0
        ) as avg_length_of_stay_hours
    from encounters
    where provider_id is not null
    group by provider_id

)

select
    provider_id,
    total_encounters,
    total_patients_served,
    inpatient_encounters,
    outpatient_encounters,
    emergency_encounters,
    avg_length_of_stay_hours
from provider_summary