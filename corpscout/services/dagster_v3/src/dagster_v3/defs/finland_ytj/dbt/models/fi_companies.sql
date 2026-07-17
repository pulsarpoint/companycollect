{{ config(materialized='table') }}

with extracted as (
  select
    *,
    fi_legal_form_json(raw_company) as legal_form_payload,
    fi_registration_flags_json(raw_company) as registration_flags
  from {{ source('finland_prhytj', 'all_companies') }}
)
select
  business_id,
  try_cast(json_extract_string(raw_company, '$.businessId.registrationDate') as date) as business_id_registration_date,
  json_extract_string(raw_company, '$.euId.value') as eu_id,
  cast(null as varchar) as vat_id,
  country_iso2,
  primary_name as name,
  lower(primary_name) as name_normalized,
  try_cast(nullif(registration_date, '') as date) as registration_date,
  try_cast(nullif(end_date, '') as date) as end_date,
  lifecycle_status,
  coalesce(is_active, false) as is_active,
  coalesce(trade_register_status, '') as trade_register_status,
  nullif(status, '') as raw_status_code,
  cast(last_modified at time zone 'UTC' as timestamp) as last_modified,
  coalesce(json_extract_string(registration_flags, '$.is_vat_registered') = '1', false) as is_vat_registered,
  coalesce(json_extract_string(registration_flags, '$.is_employer_registered') = '1', false) as is_employer_registered,
  coalesce(json_extract_string(registration_flags, '$.is_prepayment_registered') = '1', false) as is_prepayment_registered,
  json_extract_string(legal_form_payload, '$.code') as legal_form_code,
  json_extract_string(legal_form_payload, '$.description_fi') as legal_form_description_original,
  case when json_extract_string(legal_form_payload, '$.description_fi') is not null then 'fi' end as legal_form_description_language,
  json_extract_string(legal_form_payload, '$.description_en') as legal_form_description_en,
  cast(null as timestamp) as legal_form_description_translated_at,
  cast(null as varchar) as legal_form_description_translation_provider,
  cast(null as varchar) as legal_form_description_translation_model,
  nullif(website_normalized_url, '') as primary_website_url,
  nullif(website_host, '') as primary_website_host,
  source_slug as source_system,
  source_run_id,
  source_record_id,
  source_payload_hash,
  now() as resolved_at
from extracted
where business_id is not null and business_id != ''
