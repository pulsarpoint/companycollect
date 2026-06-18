{{ config(materialized='table') }}

with industry_rows as (
  select
    org_number,
    nace1_code as source_industry_code,
    nace1_description_original as description_original,
    nace1_description_en as description_en,
    true as is_primary,
    source_slug,
    source_run_id,
    source_record_id,
    source_payload_hash
  from {{ source('norway_brreg', 'entities') }}
  union all
  select
    org_number,
    nace2_code as source_industry_code,
    nace2_description_original as description_original,
    nace2_description_en as description_en,
    false as is_primary,
    source_slug,
    source_run_id,
    source_record_id,
    source_payload_hash
  from {{ source('norway_brreg', 'entities') }}
  union all
  select
    org_number,
    nace3_code as source_industry_code,
    nace3_description_original as description_original,
    nace3_description_en as description_en,
    false as is_primary,
    source_slug,
    source_run_id,
    source_record_id,
    source_payload_hash
  from {{ source('norway_brreg', 'entities') }}
)

select
  org_number,
  source_industry_code,
  'NACE_REV_2' as source_industry_code_set,
  nullif(description_original, '') as description_original,
  'no' as description_language,
  nullif(description_en, '') as description_en,
  cast(null as timestamp) as description_translated_at,
  cast(null as varchar) as description_translation_provider,
  cast(null as varchar) as description_translation_model,
  'NACE_REV_2' as nace_revision,
  source_industry_code as nace_code,
  regexp_replace(source_industry_code, '[^0-9A-Za-z]', '', 'g') as nace_normalized_code,
  'direct_code' as nace_mapping_method,
  'mapped' as nace_mapping_status,
  is_primary,
  source_slug as source_system,
  source_run_id,
  source_record_id,
  source_payload_hash,
  now() as resolved_at
from industry_rows
where org_number is not null
  and org_number != ''
  and source_industry_code is not null
  and source_industry_code != ''
