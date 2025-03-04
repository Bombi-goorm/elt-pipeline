with
short as (
   select * from {{ ref('stg_kma__short') }}
),

pivoted_short as (
    select *
    from (
        select
            nx,
            ny,
            fcst_date_time,
            category,
            fcst_value
        from short
    )
    pivot (
        max(fcst_value)
        for category in ('PCP', 'POP', 'PTY', 'REH', 'SKY', 'SNO', 'TMN', 'TMP', 'UUU', 'VEC', 'VVV', 'WAV', 'WSD')
    )
)

select * from pivoted_short


