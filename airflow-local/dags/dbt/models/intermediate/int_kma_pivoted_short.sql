{{
  config(
    materialized='incremental'
  )
}}
with short as (
    select *
    from {{ ref('stg_kma__short') }}
    {% if is_incremental() %}
    where fcst_date_time > (select max(fcst_date_time) from {{ this }})
    {% endif %}
),

pivoted_short as (
    select *
    from (select
        nx,
        ny,
        fcst_date_time,
        category,
        fcst_value
    from short) pivot (
        max(fcst_value)
        for category in (
            'PCP',
            'POP',
            'PTY',
            'REH',
            'SKY',
            'SNO',
            'TMN',
            'TMP',
            'UUU',
            'VEC',
            'VVV',
            'WAV',
            'WSD'
        )
    )
)

select *
from pivoted_short
