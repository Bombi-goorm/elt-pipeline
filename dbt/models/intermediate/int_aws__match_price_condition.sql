with real_time as (
    select *
    from {{ ref('stg_mafra__real_time') }}
),

matched_table as (
    SELECT
        notification_condition_id,
        member_id,
        target_price,
        price_direction,
        rt_price as matched_price,
        variety,
        whsl_mrkt_nm,
        scsbd_dt
    FROM {{ source('aws_rds', 'price_conditions') }} pc
    JOIN real_time rt
        ON pc.active = TRUE
        AND pc.variety = rt.gds_sclsf_nm
        AND (
            (pc.price_direction = 'U' AND rt.rt_price >= pc.target_price)
            OR
            (pc.price_direction = 'D' AND rt.rt_price < pc.target_price)
        )
        AND rt.whsl_mrkt_nm IN UNNEST(SPLIT(pc.region, '|'))
),

ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY member_id, variety, whsl_mrkt_nm, price_direction
               ORDER BY matched_price ASC
           ) AS rn
    FROM matched_table
)

SELECT member_id, target_price, matched_price, variety, whsl_mrkt_nm, price_direction, scsbd_dt
FROM ranked
WHERE rn = 1

