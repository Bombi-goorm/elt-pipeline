
WITH node_table AS (
  SELECT node_id, node
  FROM {{ ref('int_mafra__sankey_node') }}
),

from_product_to_origin AS (
  -- 품종(gds_sclsf_nm) → 원산지명(plor_nm)
  SELECT
    stg.trd_clcln_ymd as date_time,
    src.node_id AS src_id,
    tgt.node_id AS tgt_id,
    SUM(CAST(stg.totprc AS NUMERIC)) AS val
  FROM {{ ref('stg_mafra_kat_sale') }} AS stg
  JOIN node_table AS src
    ON stg.gds_sclsf_nm = src.node
  JOIN node_table AS tgt
    ON stg.plor_nm = tgt.node
  GROUP BY 1,2,3
),

from_origin_to_market AS (
  -- 원산지명(plor_nm) → 도매시장명(whsl_mrkt_nm)
  SELECT
    stg.trd_clcln_ymd as date_time,
    src.node_id AS src_id,
    tgt.node_id AS tgt_id,
    SUM(CAST(stg.totprc AS NUMERIC)) AS val
  FROM {{ ref('stg_mafra_kat_sale') }} AS stg
  JOIN node_table AS src
    ON stg.plor_nm = src.node
  JOIN node_table AS tgt
    ON stg.whsl_mrkt_nm = tgt.node
  GROUP BY 1,2,3
)

SELECT
  date_time,
  src_id,
  tgt_id,
  val
FROM from_product_to_origin

UNION ALL

SELECT
  date_time,
  src_id,
  tgt_id,
  val
FROM from_origin_to_market
