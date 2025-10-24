{{ config(
    materialized = 'table',
    schema = 'dw_oliver'
) }}

select
    d.date_key,
    em.employee_key,
    c.certification_name,
    c.certification_cost::number(10,2) as certification_cost
from {{ ref('stg_employee_certifications') }} c
inner join {{ ref('oliver_dim_employee') }} em
    on c.employee_id = em.employee_id
inner join {{ ref('oliver_dim_date') }} d
    on c.certification_awarded_date = d.date_day


