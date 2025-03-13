with source as (
    select *
    from {{ source('mafra', 'kat_sale') }}
),

cleaned as (
    select
        PARSE_DATE('%Y-%m-%d', trd_clcln_ymd) AS trd_clcln_ymd,
        SAFE_CAST(SAFE_CAST(totprc AS FLOAT64) AS INT64) as totprc,
        SAFE_CAST(avgprc AS FLOAT64) as avgprc,
        IFNULL(gds_sclsf_nm, gds_mclsf_nm) AS gds_sclsf_nm,
        IFNULL(SPLIT(plor_nm, ' ')[SAFE_OFFSET(0)], '미상') as plor_nm,
        whsl_mrkt_nm,
        whsl_mrkt_cd,
        corp_cd,
        corp_nm,
        gds_lclsf_cd,
        gds_lclsf_nm,
        gds_mclsf_cd,
        gds_mclsf_nm,
        gds_sclsf_cd,
        grd_cd,
        grd_nm,
        SAFE_CAST(SAFE_CAST(hgprc AS FLOAT64) AS INT64) as hgprc,
        SAFE_CAST(SAFE_CAST(lwprc AS FLOAT64) AS INT64) as lwprc,
        pkg_cd,
        pkg_nm,
        plor_cd,
        sz_cd,
        sz_nm,
        trd_se,
        unit_cd,
        unit_nm,
        SAFE_CAST(SAFE_CAST(unit_qty AS FLOAT64) AS INT64) as unit_qty,
        SAFE_CAST(SAFE_CAST(unit_tot_qty AS FLOAT64) AS INT64) as unit_tot_qty,
    from source
)

select *
from cleaned
