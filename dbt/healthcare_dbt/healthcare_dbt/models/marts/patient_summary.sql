with patients as (

    select * from {{ ref('stg_patients') }}

),

encounters as (

    select * from {{ ref('stg_encounters') }}

),

observations as (

    select * from {{ ref('stg_observations') }}

),

encounter_summary as (

    select
        patient_id,
        count(*) as total_encounters,
        min(admit_time) as first_admit_time,
        max(admit_time) as last_admit_time
    from encounters
    group by patient_id

),

observation_summary as (

    select
        patient_id,
        count(*) as total_observations,
        avg(observation_value) as avg_observation_value,
        sum(case when abnormal_flag = true then 1 else 0 end) as abnormal_observation_count
    from observations
    group by patient_id

)

select
    p.patient_id,
    p.given_name,
    p.family_name,
    p.gender,
    p.birth_date,
    p.city,
    p.source_system,
    coalesce(e.total_encounters, 0) as total_encounters,
    e.first_admit_time,
    e.last_admit_time,
    coalesce(o.total_observations, 0) as total_observations,
    o.avg_observation_value,
    coalesce(o.abnormal_observation_count, 0) as abnormal_observation_count
from patients p
left join encounter_summary e
    on p.patient_id = e.patient_id
left join observation_summary o
    on p.patient_id = o.patient_id