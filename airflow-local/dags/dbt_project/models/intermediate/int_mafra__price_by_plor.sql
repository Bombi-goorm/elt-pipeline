with sales as (
    select
        plor_nm,
        safe_divide(avgprc, unit_qty) as avg_ppk,
        variety,
        date_time,
    from {{ ref('stg_mafra_kat_sale') }}
    where date_time = (
        select max(date_time)
        from  {{ ref('stg_mafra_kat_sale') }}
    )
),


price_by_plor as (
    select
        plor_nm,
        safe_cast(avg(avg_ppk) as int64) as avg_ppk,
        variety,
        ANY_VALUE(date_time) as date_time,
    from sales
    group by variety, plor_nm
    order by avg_ppk, variety, plor_nm
)

select *
from price_by_plor

