SELECT links.country_code, links.company_id, links.section, links.item_key, links.source_record_uid
FROM {{ ref('company_section_item_source_links_build') }} AS links
WHERE links.source_record_uid = ''
   OR links.record_kind = ''
   OR links.source_slug = ''
   OR links.source_record_key = ''
   OR links.payload_sha256 = ''
