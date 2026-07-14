{{
  config(
    materialized='table'
  )
}}

with source_dates as (
    select
        rate_date,
        min(source_run_id) as source_run_id,
        min(pulled_at) as pulled_at
    from {{ ref('ecb_rates') }}
    {% if var('start_date', none) is not none and var('end_date', none) is not none %}
    where rate_date >= cast('{{ var("start_date") }}' as date)
      and rate_date <= cast('{{ var("end_date") }}' as date)
    {% endif %}
    group by rate_date
)

select
    rate_date,
    'EUR' as base_currency,
    'EUR' as quote_currency,
    cast(1 as decimal(38, 12)) as rate,
    'identity' as source,
    '' as source_url,
    repeat('0', 64) as source_payload_hash,
    source_run_id,
    pulled_at,
    '' as _dlt_load_id,
    sha256(
        concat_ws(
            '|',
            cast(rate_date as varchar),
            'EUR',
            'EUR',
            cast(1 as varchar),
            'identity',
            '',
            repeat('0', 64),
            source_run_id,
            cast(pulled_at as varchar)
        )
    ) as _dlt_id
from source_dates
