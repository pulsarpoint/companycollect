{{ config(materialized='table') }}

with addresses as (
  select
    companies.*,
    address.key as address_index,
    address.value as address_json,
    coalesce(
      (
        select json_extract_string(post_office.value, '$.city')
        from json_each(address.value, '$.postOffices') as post_office
        order by
          case json_extract_string(post_office.value, '$.languageCode')
            when '1' then 0
            else 1
          end,
          try_cast(post_office.key as integer)
        limit 1
      ),
      ''
    ) as city,
    coalesce(
      (
        select json_extract_string(post_office.value, '$.municipalityCode')
        from json_each(address.value, '$.postOffices') as post_office
        where nullif(json_extract_string(post_office.value, '$.municipalityCode'), '') is not null
        order by
          case json_extract_string(post_office.value, '$.languageCode')
            when '1' then 0
            else 1
          end,
          try_cast(post_office.key as integer)
        limit 1
      ),
      ''
    ) as municipality_code
  from {{ source('finland_prhytj', 'all_companies') }} as companies,
    json_each(companies.raw_company, '$.addresses') as address
), normalized as (
  select
    *,
    coalesce(
      nullif(upper(trim(json_extract_string(address_json, '$.country'))), ''),
      'FI'
    ) as normalized_country_code,
    concat_ws(
      ', ',
      nullif(trim(json_extract_string(address_json, '$.co')), ''),
      case
        when nullif(trim(json_extract_string(address_json, '$.freeAddressLine')), '') is not null
          then replace(trim(json_extract_string(address_json, '$.freeAddressLine')), '_', ' ')
        when nullif(trim(json_extract_string(address_json, '$.postOfficeBox')), '') is not null
          then concat('PL ', trim(json_extract_string(address_json, '$.postOfficeBox')))
        else trim(concat_ws(
          ' ',
          nullif(trim(json_extract_string(address_json, '$.street')), ''),
          nullif(trim(json_extract_string(address_json, '$.buildingNumber')), ''),
          nullif(trim(json_extract_string(address_json, '$.entrance')), ''),
          nullif(
            concat(
              trim(coalesce(json_extract_string(address_json, '$.apartmentNumber'), '')),
              trim(coalesce(json_extract_string(address_json, '$.apartmentIdSuffix'), ''))
            ),
            ''
          )
        ))
      end
    ) as address_lines
  from addresses
)
select
  country_iso2,
  'finland_ytj' as source_slug,
  source_run_id,
  concat(source_record_id, ':address:', address_index) as source_record_id,
  business_id as registry_id,
  case try_cast(json_extract_string(address_json, '$.type') as integer)
    when 1 then 'street'
    when 2 then 'postal'
    else 'other'
  end as address_type,
  nullif(address_lines, '') as address_lines,
  nullif(trim(json_extract_string(address_json, '$.postCode')), '') as postal_code,
  nullif(city, '') as city,
  cast(null as varchar) as municipality,
  nullif(municipality_code, '') as municipality_code,
  case normalized_country_code
    when 'FI' then 'Finland'
    else null
  end as country,
  normalized_country_code as country_code,
  try_cast(nullif(json_extract_string(address_json, '$.registrationDate'), '') as date)
    as registered_on,
  nullif(json_extract_string(address_json, '$.source'), '') as source_code,
  'addresses' as source_field,
  true as is_current,
  '' as source_url,
  source_payload_hash,
  now() as resolved_at
from normalized
where business_id is not null
  and business_id != ''
  and (
    nullif(address_lines, '') is not null
    or nullif(trim(json_extract_string(address_json, '$.postCode')), '') is not null
    or nullif(city, '') is not null
  )
