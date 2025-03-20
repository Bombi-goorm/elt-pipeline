with sales as (
    select
        grd_nm,
        safe_divide(avgprc, unit_qty) as avg_ppk,
        variety,
        item,
        date_time,
    from {{ ref('stg_mafra_kat_sale') }}
),


price_trend_by_grade as (
    select
        grd_nm,
        safe_cast(avg(avg_ppk) as int64) as avg_ppk,
        variety,
        date_time,
        item
    from sales
    group by date_time, variety, grd_nm, item
    order by date_time, variety, grd_nm
)

select *
from price_trend_by_grade

