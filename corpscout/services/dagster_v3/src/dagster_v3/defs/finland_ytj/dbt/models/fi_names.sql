{{ config(materialized='table') }}

select
  companies.business_id,
  json_extract_string(names.value, '$.name') as name,
  json_extract_string(names.value, '$.type') as name_type_code,
  cast(null as varchar) as name_type_description_original,
  cast(null as varchar) as name_type_description_en,
  try_cast(nullif(json_extract_string(names.value, '$.registrationDate'), '') as date) as registration_date,
  try_cast(nullif(json_extract_string(names.value, '$.endDate'), '') as date) as end_date,
  try_cast(json_extract_string(names.value, '$.version') as uinteger) as version,
  json_extract_string(names.value, '$.endDate') is null
    or json_extract_string(names.value, '$.endDate') = '' as is_current,
  json_extract_string(names.value, '$.type') = '1'
    and (
      json_extract_string(names.value, '$.endDate') is null
      or json_extract_string(names.value, '$.endDate') = ''
    ) as is_primary,
  nullif(json_extract_string(names.value, '$.source'), '') as source_code,
  companies.source_slug as source_system,
  companies.source_run_id,
  concat(companies.source_record_id, ':name:', names.key) as source_record_id,
  companies.source_payload_hash,
  now() as resolved_at
from {{ source('finland_prhytj', 'all_companies') }} as companies,
  json_each(companies.raw_company, '$.names') as names
where companies.business_id is not null
  and companies.business_id != ''
  and json_extract_string(names.value, '$.name') is not null
  and json_extract_string(names.value, '$.name') != ''
