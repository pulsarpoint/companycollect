{{ config(materialized='table') }}

with extracted as (
  select
    *,
    fi_primary_industry_json(raw_company) as industry_json
  from {{ source('finland_prhytj', 'all_companies') }}
), normalized as (
  select
    *,
    json_extract_string(industry_json, '$.code') as source_industry_code,
    json_extract_string(industry_json, '$.codeSet') as source_industry_code_set
  from extracted
)
select
  business_id,
  source_industry_code,
  source_industry_code_set,
  json_extract_string(industry_json, '$.description') as description_original,
  coalesce(json_extract_string(industry_json, '$.language'), 'fi') as description_language,
  cast(null as varchar) as description_en,
  cast(null as timestamp) as description_translated_at,
  cast(null as varchar) as description_translation_provider,
  cast(null as varchar) as description_translation_model,
  case
    when source_industry_code_set in ('NACE_REV_2', 'NACE_REV_2_1') then source_industry_code_set
    else null
  end as nace_revision,
  case
    when source_industry_code_set in ('NACE_REV_2', 'NACE_REV_2_1') then source_industry_code
    else null
  end as nace_code,
  case
    when source_industry_code_set in ('NACE_REV_2', 'NACE_REV_2_1')
      then regexp_replace(source_industry_code, '[^0-9A-Za-z]', '', 'g')
    else null
  end as nace_normalized_code,
  case
    when source_industry_code is null then 'none'
    when source_industry_code_set in ('NACE_REV_2', 'NACE_REV_2_1') then 'direct_code'
    else 'none'
  end as nace_mapping_method,
  case
    when source_industry_code is null then 'missing_source_code'
    when source_industry_code_set in ('NACE_REV_2', 'NACE_REV_2_1') then 'mapped'
    else 'unmapped_source_code_set'
  end as nace_mapping_status,
  true as is_primary,
  source_slug as source_system,
  source_run_id,
  source_record_id,
  source_payload_hash,
  now() as resolved_at
from normalized
where business_id is not null and business_id != ''
