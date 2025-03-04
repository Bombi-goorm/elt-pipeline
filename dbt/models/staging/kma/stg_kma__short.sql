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
        baseDate as base_date,
        FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', PARSE_DATETIME('%Y%m%d%H%M', CONCAT(fcstDate, fcstTime))) as fcst_date_time
    from source
)

select * from renamed

