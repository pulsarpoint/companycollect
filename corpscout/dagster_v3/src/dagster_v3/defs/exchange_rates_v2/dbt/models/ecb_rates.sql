{{
  config(
    materialized='incremental',
    unique_key=['rate_date', 'base_currency', 'quote_currency', 'source'],
    incremental_strategy='delete+insert'
  )
}}

with raw_payloads as (
    select *
    from {{ source('exchange_rates_v2_stage', 'ecb_raw_payloads') }}
    {% if var('start_date', none) is not none and var('end_date', none) is not none %}
    where start_date >= '{{ var("start_date") }}'
      and end_date <= '{{ var("end_date") }}'
    {% endif %}
),

series as (
    select
        raw_payloads.*,
        series_item.key as series_key,
        series_item.value as series_value,
        cast(split_part(series_item.key, ':', 2) as integer) as currency_index
    from raw_payloads,
         json_each(raw_payloads.source_payload_json, '$.dataSets[0].series') as series_item
),

observations as (
    select
        series.*,
        observation_item.key as observation_key,
        observation_item.value as observation_value
    from series,
         json_each(series.series_value, '$.observations') as observation_item
),

typed_rows as (
    select
        cast(
            json_extract_string(
                source_payload_json,
                '$.structure.dimensions.observation[0].values[' || observation_key || '].id'
            ) as date
        ) as rate_date,
        'EUR' as base_currency,
        upper(
            json_extract_string(
                source_payload_json,
                '$.structure.dimensions.series[1].values[' || currency_index || '].id'
            )
        ) as quote_currency,
        cast(json_extract_string(observation_value, '$[0]') as decimal(38, 12)) as rate,
        'ECB EXR' as source,
        source_url,
        source_payload_hash,
        source_run_id,
        cast(replace(pulled_at, 'Z', '') as timestamp) as pulled_at
    from observations
    where observation_value is not null
)

select
    rate_date,
    base_currency,
    quote_currency,
    rate,
    source,
    source_url,
    source_payload_hash,
    source_run_id,
    pulled_at,
    '' as _dlt_load_id,
    sha256(
        concat_ws(
            '|',
            cast(rate_date as varchar),
            base_currency,
            quote_currency,
            cast(rate as varchar),
            source,
            source_url,
            source_payload_hash,
            source_run_id,
            cast(pulled_at as varchar)
        )
    ) as _dlt_id
from typed_rows
