{{ config(materialized='table') }}

{% set metric_columns = [
    'revenue', 'operating_profit_loss', 'profit_loss', 'total_assets', 'equity',
    'liabilities', 'cash_and_bank', 'current_assets', 'current_receivables',
    'current_liabilities', 'personnel_expenses', 'wages_and_salaries', 'employees'
] %}

with fact_counts as (
    select statement_key, count(*) as source_fact_count
    from {{ source('finland_prh_xbrl', 'fi_prh_xbrl_facts_raw') }}
    group by statement_key
),
current_numeric_facts as (
    select
        facts.statement_key,
        facts.concept_qname,
        facts.mcy_member_code,
        try_cast(nullif(facts.numeric_value, '') as double) as numeric_value,
        mapping.metric_code
    from {{ source('finland_prh_xbrl', 'fi_prh_xbrl_facts_raw') }} as facts
    left join {{ ref('xbrl_metric_map') }} as mapping
        on facts.concept_qname = mapping.concept_qname
       and facts.mcy_member_code = mapping.mcy_member_code
    where facts.value_kind = 'numeric'
      and coalesce(facts.is_comparative, false) = false
),
metric_pivot as (
    select
        statement_key,
        count(*) filter (where metric_code is not null) as mapped_fact_count,
        count(*) filter (where metric_code is null) as unmapped_numeric_fact_count,
        {% for m in metric_columns -%}
        max(numeric_value) filter (where metric_code = '{{ m }}') as {{ m }}{% if not loop.last %},{% endif %}
        {% endfor %}
    from current_numeric_facts
    group by statement_key
)
select
    statements.statement_key,
    statements.business_id,
    statements.financial_date,
    nullif(statements.reported_period_start, '') as period_start,
    coalesce(
        nullif(statements.reported_period_end, ''),
        nullif(statements.financial_date, '')
    ) as period_end,
    {% for m in metric_columns -%}
    metrics.{{ m }},
    {% endfor -%}
    coalesce(fact_counts.source_fact_count, 0) as source_fact_count,
    coalesce(metrics.mapped_fact_count, 0) as mapped_fact_count,
    coalesce(metrics.unmapped_numeric_fact_count, 0) as unmapped_numeric_fact_count,
    case
        when coalesce(metrics.unmapped_numeric_fact_count, 0) > 0
            then concat('["unmapped numeric facts: ', coalesce(metrics.unmapped_numeric_fact_count, 0)::varchar, '"]')
        when coalesce(metrics.mapped_fact_count, 0) = 0 then '["no mapped metrics"]'
        else '[]'
    end as metric_warnings,
    'finland-prh-xbrl-metrics-v1' as mapping_version,
    now() as built_at
from {{ source('finland_prh_xbrl', 'fi_prh_xbrl_statement_documents') }} as statements
left join fact_counts on statements.statement_key = fact_counts.statement_key
left join metric_pivot as metrics on statements.statement_key = metrics.statement_key
order by statements.business_id, statements.financial_date, statements.statement_key
