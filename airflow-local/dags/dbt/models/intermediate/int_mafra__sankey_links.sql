with temp_links as (
    select *
    from {{ ref('int_mafra__sankey_temp') }}
),

nodes as (
    select *
    from {{ ref("int_mafra__sankey_nodes") }}
),

final_links as (

    select
        l.date_time,
        l.item,
        n1.node_id as src,
        n2.node_id as tgt,
        l.val
    from temp_links l
    join nodes n1
        on l.date_time = n1.date_time
        and l.item = n1.item
        and l.src = n1.node
    join nodes n2
        on l.date_time = n2.date_time
        and l.item = n2.item
        and l.tgt = n2.node
)

select *
from final_links