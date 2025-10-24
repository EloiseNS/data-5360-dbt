{{ config(
    materialized = 'table',
    schema = 'dw_oliver'
)}}

select
    employee_id,
    first_name,
    last_name,
    email,
    PARSE_JSON(certification_json):certification_name::string AS certification_name,
    PARSE_JSON(certification_json):certification_cost::number(10,2) AS certification_cost,
    PARSE_JSON(certification_json):certification_awarded_date::date AS certification_awarded_date
from {{ source('oliver_landing', 'employee_certifications')}}