with
source as (
    select * from {{ source('kma', 'wrn') }}
),

renamed as (

    select
        stnid as stn_id,
        title,
        FORMAT_TIMESTAMP(
            '%Y-%m-%d %H:%M:%S', PARSE_DATETIME('%Y%m%d%H%M', tmfc)
        ) as fcst_date_time
    from source
)

select * from renamed
