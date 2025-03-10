with source as (
    select *
    from {{ source('mafra', 'kat_sale') }}
),

renamed as (
    select
        IFNULL(gds_sclsf_nm, gds_mclsf_nm) AS gds_sclsf_nm,
        IFNULL(SPLIT(plor_nm, ' ')[SAFE_OFFSET(0)], '미상') as plor_nm,
        * EXCEPT(gds_sclsf_nm, plor_nm)
    from source
)

select *
from renamed
