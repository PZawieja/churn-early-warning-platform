with

source as (
    select * from {{ source('raw', 'billing_events') }}
)

select
    cast(event_id as varchar)      as event_id
    , cast(account_id as varchar)  as account_id
    , cast(event_type as varchar)  as event_type
    , cast(amount as double)       as amount
    , cast(event_ts as timestamp)  as event_ts

from source
