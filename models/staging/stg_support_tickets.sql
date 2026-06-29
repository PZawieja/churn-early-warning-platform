with

source as (
    select * from {{ source('raw', 'support_tickets') }}
)

select
    cast(ticket_id as varchar) as ticket_id,
    cast(account_id as varchar) as account_id,
    cast(severity as varchar) as severity,
    cast(status as varchar) as status,
    cast(created_at as timestamp) as created_at,
    cast(resolved_at as timestamp) as resolved_at

from source
