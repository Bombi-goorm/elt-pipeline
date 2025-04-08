{{ config(
    materialized='incremental',
    unique_key=['date_time', 'corp_cd', 'item_id', 'whsl_mrkt_cd'],
    partition_by={'field': 'date_time', 'data_type': 'timestamp'}
) }}

with source as (
    select *
    from {{ source('mafra', 'kat_sale') }}
),

cleaned as (
    select
        PARSE_DATETIME('%Y-%m-%d %H:%M:%S', scsbd_dt) AS date_time,
        SAFE_CAST(SAFE_DIVIDE(SAFE_CAST(scsbd_prc AS FLOAT64), SAFE_CAST(unit_qty AS FLOAT64)) as int64) AS rt_price,
        whsl_mrkt_nm,
        corp_nm,
        gds_lclsf_nm as category,
        gds_mclsf_nm as item,
        gds_sclsf_nm as variety,
        IFNULL(SPLIT(plor_nm, ' ')[SAFE_OFFSET(0)], '미상') as plor_nm,
        SAFE_CAST(SAFE_CAST(scsbd_prc AS FLOAT64) AS INT64) as scsbd_prc,
        SAFE_CAST(SAFE_CAST(qty AS FLOAT64) AS INT64) as qty,
        SAFE_CAST(SAFE_CAST(unit_qty AS FLOAT64) AS INT64) as unit_qty,
        unit_nm,
        pkg_nm,
        trd_se,
        gds_lclsf_cd as category_id,
        gds_mclsf_cd as item_id,
        gds_sclsf_cd as variety_id,
        whsl_mrkt_cd,
        unit_cd,
        pkg_cd,
        plor_cd,
        corp_cd,
        trd_clcln_ymd
    from {{ source('mafra', 'real_time') }}

    {% if is_incremental() %}
    where PARSE_DATETIME('%Y-%m-%d %H:%M:%S', scsbd_dt) >
          (select max(date_time) from {{ this }})
    {% endif %}
)

select *
from cleaned
