{{
    config(
        materialized='table'
    )
}}

/*
    int_churn_features
    ------------------
    One row per account: latest-week feature set ready for scoring.
    Joins activity, support, and billing signal layers.

    Grain: one row per account_id (current week snapshot).
*/

with

activity as (
    select *
    from {{ ref('int_account_activity_weekly') }}
    where week_start = date_trunc('week', current_date)
),

support as (
    select * from {{ ref('int_account_support_signals') }}
),

billing as (
    select * from {{ ref('int_account_billing_signals') }}
)

select
    a.account_id,
    a.week_start as feature_week,

    -- ── Usage signals ──────────────────────────────────────────────────────
    a.wau,
    a.wau_rolling_4w,
    a.wau_rolling_prev_4w,
    a.wau_wow_delta,
    a.usage_trend_pct,
    a.active_user_ratio,
    a.distinct_features_used,
    a.days_since_last_event,

    -- ── Commercial signals ─────────────────────────────────────────────────
    a.mrr,
    a.plan_name,
    a.renewal_date,
    a.days_to_renewal,
    a.seats_contracted,
    b.seat_contracted_ratio,
    b.had_payment_failure_30d,
    b.had_downgrade_90d,

    -- ── Support signals ────────────────────────────────────────────────────
    s.open_tickets_count,
    s.p1_p2_tickets_30d,
    s.avg_ticket_resolution_days,

    -- ── Derived flags ──────────────────────────────────────────────────────
    (a.days_to_renewal between 0 and 90) as is_in_renewal_window,
    (a.wau = 0) as is_zero_usage_week,
    (a.active_user_ratio < 0.25) as is_low_utilization

from activity as a
left join support as s on a.account_id = s.account_id
left join billing as b on a.account_id = b.account_id
