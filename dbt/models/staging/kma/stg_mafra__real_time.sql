with source as (
    select *
    from {{ source('mafra', 'real_time') }}
),

renamed as (
    select
        PARSE_DATETIME('%Y-%m-%d %H:%M:%S', scsbd_dt) AS scsbd_dt,
        trd_clcln_ymd,
        whsl_mrkt_cd,
        whsl_mrkt_nm,
        corp_cd,
        corp_nm,
        gds_lclsf_cd,
        gds_lclsf_nm,
        gds_mclsf_cd,
        gds_mclsf_nm,
        gds_sclsf_cd,
        gds_sclsf_nm,
        plor_cd,
        IFNULL(SPLIT(plor_nm, ' ')[SAFE_OFFSET(0)], '미상') as plor_nm,
        SAFE_CAST(SAFE_CAST(scsbd_prc AS FLOAT64) AS INT64) as rt_price,
        SAFE_CAST(SAFE_CAST(qty AS FLOAT64) AS INT64) as qty,
        SAFE_CAST(SAFE_CAST(unit_qty AS FLOAT64) AS INT64) as unit_qty,
        unit_cd,
        unit_nm,
        pkg_cd,
        pkg_nm,
        trd_se,
        SAFE_CAST(SAFE_CAST(scsbd_prc AS FLOAT64) AS INT64) as scsbd_prc,
        from source
)

select *
from renamed
