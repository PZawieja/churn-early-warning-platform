{{
    config(
        materialized='table'
    )
}}

-- STUB: Session 2 — replace with real aggregations from stg_billing_events

select
    account_id,
    false as had_payment_failure_30d,
    false as had_downgrade_90d,
    1.0 as seat_contracted_ratio

from {{ ref('stg_accounts') }}
