{{
    config(
        materialized='table'
    )
}}

-- STUB: Session 2 — replace with real aggregations from stg_support_tickets

select
    account_id,
    0 as open_tickets_count,
    0 as p1_p2_tickets_30d,
    null::float as avg_ticket_resolution_days

from {{ ref('stg_accounts') }}
