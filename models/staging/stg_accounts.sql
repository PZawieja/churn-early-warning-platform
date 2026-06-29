with

source as (
    select * from {{ source('raw', 'accounts') }}
)

select
    cast(account_id as varchar)        as account_id
    , cast(account_name as varchar)    as account_name
    , cast(plan_name as varchar)       as plan_name
    , cast(mrr as double)              as mrr
    , cast(seats_contracted as integer) as seats_contracted
    , cast(renewal_date as date)       as renewal_date
    , cast(is_active as boolean)       as is_active
    , cast(created_at as timestamp)    as created_at

from source
