with kat_sale as (
    select *
    from {{ ref('stg_mafra_kat_sale') }}
),

from_product_to_origin as (
    select
        gds_sclsf_nm as source,
        plor_nm as target,
        sum(cast(totprc as float64)) as value
    from kat_sale
    group by
        gds_sclsf_nm,
        plor_nm
),

from_origin_to_market as (
    select
        plor_nm as source,
        whsl_mrkt_nm as target,
        sum(cast(totprc as float64)) as value
    from kat_sale
    group by
        plor_nm,
        whsl_mrkt_nm
),

unioned as (
    select
        source,
        target,
        value
    from from_product_to_origin

    union all

    select
        source,
        target,
        value
    from from_origin_to_market
)

select
    source,
    target,
    value
from unioned
order by
    source,
    target
