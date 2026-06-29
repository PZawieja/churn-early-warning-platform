{{
    config(
        materialized='table'
    )
}}

/*
    int_account_support_signals
    ---------------------------
    Per-account support health aggregates.

    Grain: one row per account_id.

    Sources: stg_support_tickets
*/

with

tickets as (
    select * from {{ ref('stg_support_tickets') }}
)

, ticket_agg as (
    select
        account_id

        -- Current open ticket backlog
        , count(*) filter (
            where status = 'open'
          )                                                    as open_tickets_count

        -- Critical tickets (P1/P2) raised in last 30 days — leading churn signal
        , count(*) filter (
            where severity in ('P1', 'P2')
            and created_at >= current_date - interval '30 days'
          )                                                    as p1_p2_tickets_30d

        -- Average resolution time in days (closed/resolved tickets only)
        , avg(
            case
                when resolved_at is not null
                then datediff('day', created_at, resolved_at)
            end
          )                                                    as avg_ticket_resolution_days

    from tickets
    group by 1
)

-- Left join from accounts so every account gets a row (even those with no tickets)
select
    a.account_id
    , coalesce(t.open_tickets_count, 0)            as open_tickets_count
    , coalesce(t.p1_p2_tickets_30d, 0)             as p1_p2_tickets_30d
    , t.avg_ticket_resolution_days                  as avg_ticket_resolution_days

from {{ ref('stg_accounts') }} a
left join ticket_agg t on t.account_id = a.account_id
