with links as (
    select *
    from {{ ref("int_mafra__sankey_temp") }}
),

node as (
    select   ROW_NUMBER() OVER (partition by date_time, item ORDER BY node) - 1 as node_id, node, item, date_time
    from
        (
            select date_time, item, src as node from links
            union distinct
            select date_time, item, tgt as node from links
        )
)

select *
from node
order by item, date_time