{{ config(
    materialized='incremental',
    unique_key=['nx', 'ny', 'fcst_date_time', 'category'],
    partition_by={'field': 'fcst_date', 'data_type': 'date'}
) }}

with
source as (
    select * from {{ source('kma', 'short') }}
),

renamed as (
    select
        nx,
        ny,
        cast(fcstvalue as float64) as fcst_value,
        parse_date('%Y%m%d', fcstdate) as fcst_date,
        category,
        basetime as base_time,
        fcsttime as fcst_time,
        basedate as base_date,
        FORMAT_TIMESTAMP(
            '%Y-%m-%d %H:%M:%S',
            PARSE_DATETIME('%Y%m%d%H%M', CONCAT(fcstdate, fcsttime))
        ) as fcst_date_time
    from {{ source('kma', 'short') }}

    {% if is_incremental() %}
    where PARSE_DATETIME('%Y%m%d%H%M', CONCAT(fcstdate, fcsttime)) >
          (select max(fcst_date_time) from {{ this }})
    {% endif %}
)

select * from renamed
