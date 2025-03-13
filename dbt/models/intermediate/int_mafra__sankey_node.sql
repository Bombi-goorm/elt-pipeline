WITH distinct_nodes AS (
  SELECT gds_sclsf_nm AS node FROM {{ ref('stg_mafra_kat_sale') }}
  UNION ALL
  SELECT plor_nm      AS node FROM {{ ref('stg_mafra_kat_sale') }}
  UNION ALL
  SELECT whsl_mrkt_nm AS node FROM {{ ref('stg_mafra_kat_sale') }}
),
dedup_nodes AS (
  SELECT DISTINCT node
  FROM distinct_nodes
)

SELECT
  ROW_NUMBER() OVER (ORDER BY node) AS node_id,
  node
FROM dedup_nodes
