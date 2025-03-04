with
source as (
    select * from {{ source('kma', 'short') }}
),

renamed as (

    select
        nx,
        ny,
        fcstValue as fcst_value,
        fcstDate as fcst_date,
        category,
        baseTime as base_time,
        fcstTime as fcst_time,
        baseDate as base_date
    from source
)


