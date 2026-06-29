{{
    config(
        materialized='table'
    )
}}

/*
    int_account_billing_signals
    ---------------------------
    Per-account billing health and seat utilization signals.

    Grain: one row per account_id.

    Sources: stg_billing_events, stg_product_events, stg_accounts
*/

with

accounts as (
    select
        account_id,
        seats_contracted
    from {{ ref('stg_accounts') }}
),

billing as (
    select * from {{ ref('stg_billing_events') }}
),

-- Distinct active users in last 30 days as proxy for active seats
active_users_30d as (
    select
        account_id,
        count(distinct user_id) as active_users_30d
    from {{ ref('stg_product_events') }}
    where event_ts >= current_date - interval '30 days'
    group by 1
),

billing_agg as (
    select
        account_id,

        -- Payment failure in last 30 days — strong churn predictor
        bool_or(
            event_type = 'payment_failure'
            and event_ts >= current_date - interval '30 days'
        ) as had_payment_failure_30d,

        -- Downgrade event in last 90 days — commitment signal
        bool_or(
            event_type = 'downgrade'
            and event_ts >= current_date - interval '90 days'
        ) as had_downgrade_90d

    from billing
    group by 1
)

select
    a.account_id,

    coalesce(b.had_payment_failure_30d, false) as had_payment_failure_30d,
    coalesce(b.had_downgrade_90d, false) as had_downgrade_90d,

    -- active_users_30d / seats_contracted; null when seats_contracted = 0
    case
        when a.seats_contracted > 0
            then round(
                coalesce(u.active_users_30d, 0) * 1.0 / a.seats_contracted,
                4
            )
    end as seat_contracted_ratio

from accounts as a
left join billing_agg as b on a.account_id = b.account_id
left join active_users_30d as u on a.account_id = u.account_id
