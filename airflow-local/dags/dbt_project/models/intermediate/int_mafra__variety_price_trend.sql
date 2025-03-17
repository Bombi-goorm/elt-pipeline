with sale as (
    select
        date_time,
        SAFE_DIVIDE(avgprc, unit_qty) as avg_ppk,
        grd_nm,
        whsl_mrkt_nm,
        variety
    from {{ ref('stg_mafra_kat_sale') }}
),

variety_price_trend as (
    select
        date_time,
        safe_cast(avg(avg_ppk) as int64) as avg_ppk,
        variety
    from sale
{#    where whsl_mrkt_nm = '서울가락'#}
    group by variety, date_time
    order by variety, date_time
)

select *
from variety_price_trend