with
source as (
    select * from {{ source('kma', 'wrn') }}
),

renamed as (

    select
        stnId as station_id,
        title,
        FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', PARSE_DATETIME('%Y%m%d%H%M', tmFc)) as fcst_date_time
    from source
)

select * from renamed

