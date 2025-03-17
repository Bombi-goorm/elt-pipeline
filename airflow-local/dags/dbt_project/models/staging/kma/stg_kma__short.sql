with
source as (
    select * from {{ source('kma', 'short') }}
),

renamed as (
    select
        nx,
        ny,
        fcstvalue as fcst_value,
        fcstdate as fcst_date,
        category,
        basetime as base_time,
        fcsttime as fcst_time,
        basedate as base_date,
        FORMAT_TIMESTAMP(
            '%Y-%m-%d %H:%M:%S',
            PARSE_DATETIME('%Y%m%d%H%M', CONCAT(fcstdate, fcsttime))
        ) as fcst_date_time
    from source
)

select * from renamed
