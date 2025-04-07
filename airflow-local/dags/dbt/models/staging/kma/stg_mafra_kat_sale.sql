with source as (
    select distinct *
    from {{ source('mafra', 'kat_sale') }}
),

cleaned as (
    select
        gds_mclsf_nm as item,
        IFNULL(gds_sclsf_nm, gds_mclsf_nm) as variety,
        SAFE_CAST(SAFE_CAST(totprc AS FLOAT64) AS INT64) as totprc,
        SAFE_CAST(SAFE_CAST(avgprc AS FLOAT64) AS INT64) as avgprc,
        PARSE_DATE('%Y-%m-%d', trd_clcln_ymd) AS date_time,
        IFNULL(SPLIT(plor_nm, ' ')[SAFE_OFFSET(0)], '미상') as plor_nm,
        whsl_mrkt_nm,
        corp_nm,
        gds_lclsf_nm as category,
        grd_nm,
        SAFE_CAST(SAFE_CAST(hgprc AS FLOAT64) AS INT64) as hgprc,
        SAFE_CAST(SAFE_CAST(lwprc AS FLOAT64) AS INT64) as lwprc,
        pkg_nm,
        sz_nm,
        SAFE_CAST(SAFE_CAST(unit_qty AS FLOAT64) AS INT64) as unit_qty,
        SAFE_CAST(SAFE_CAST(unit_tot_qty AS FLOAT64) AS INT64) as unit_tot_qty,
        trd_se,
        whsl_mrkt_cd,
        plor_cd,
        sz_cd,
        pkg_cd,
        grd_cd,
        gds_sclsf_cd,
        gds_mclsf_cd as item_cd,
        gds_lclsf_cd as category_cd,
        corp_cd,
        unit_cd,
        unit_nm,
    from source
)

select *
from cleaned
