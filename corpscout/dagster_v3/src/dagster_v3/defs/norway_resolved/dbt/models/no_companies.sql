{{ config(materialized='table') }}

select
  org_number,
  country_iso2,
  legal_name as name,
  lower(legal_name) as name_normalized,
  try_cast(nullif(registration_date, '') as date) as registration_date,
  try_cast(nullif(incorporation_date, '') as date) as incorporation_date,
  status as lifecycle_status,
  coalesce(is_active, false) as is_active,
  nullif(legal_form_code, '') as legal_form_code,
  nullif(legal_form_description_original, '') as legal_form_description_original,
  case
    when nullif(legal_form_description_original, '') is not null then 'no'
    else null
  end as legal_form_description_language,
  nullif(legal_form_description_en, '') as legal_form_description_en,
  cast(null as timestamp) as legal_form_description_translated_at,
  cast(null as varchar) as legal_form_description_translation_provider,
  cast(null as varchar) as legal_form_description_translation_model,
  nullif(normalized_url(website), '') as primary_website_url,
  nullif(website_host(website), '') as primary_website_host,
  source_slug as source_system,
  source_run_id,
  source_record_id,
  source_payload_hash,
  now() as resolved_at
from {{ source('norway_brreg', 'entities') }}
where org_number is not null
  and org_number != ''
