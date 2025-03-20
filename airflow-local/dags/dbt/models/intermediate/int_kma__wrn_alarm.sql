with wrn as (
    select *
    from {{ ref('stg_kma__wrn') }}
),
wrn_alarm as (
    select
        wrn.stn_id,
        ws.stn_nm,
        trim(regexp_replace(wrn.title, r'\s*\(\*\)$', '')) as title,
        wrn.fcst_date_time
    from wrn
    left join {{ ref('weather_stations') }} as ws
    on wrn.stn_id = ws.stn_id
)

select * from wrn_alarm