with sale as (
    select
                date_time,
                plor_nm,
                item,
                variety,
                totprc,
                whsl_mrkt_nm
    from        {{ ref('stg_mafra_kat_sale') }}
),
from_product_to_origin as (
    select
                date_time,
                item,
                variety    as src,
                plor_nm         as tgt,
                sum(totprc)     as val
    from        sale
    group by    item, date_time, variety, plor_nm
    order by    date_time
),
from_origin_to_whsal as (
    select
                date_time,
                item,
                plor_nm         as src,
                whsl_mrkt_nm    as tgt,
                sum(totprc)     as val
    from        sale
    group by    item, date_time, whsl_mrkt_nm, plor_nm
    order by    date_time
)

select          date_time, item, src, tgt, val
from            from_product_to_origin
union all
select          date_time, item, src, tgt, val
from            from_origin_to_whsal