-- name: NormalizeBrregSourceProfiles :one
WITH selected_raw_records AS (
  SELECT rr.*
  FROM brreg_workflow.raw_records rr
  JOIN brreg_workflow.v_raw_record_list ri ON ri.id = rr.id
  WHERE rr.is_current
    AND (
      COALESCE(cardinality(sqlc.arg('selected_ids')::text[]), 0) = 0
      OR rr.id::text = ANY(sqlc.arg('selected_ids')::text[])
    )
    AND (
      sqlc.narg('query')::text IS NULL
      OR ri.organization_name ILIKE '%' || sqlc.narg('query')::text || '%'
      OR ri.organization_number ILIKE '%' || sqlc.narg('query')::text || '%'
    )
    AND (sqlc.narg('lifecycle_state')::text IS NULL OR ri.lifecycle_state = sqlc.narg('lifecycle_state')::text)
    AND (sqlc.narg('translation_status')::text IS NULL OR ri.translation_status = sqlc.narg('translation_status')::text)
    AND (sqlc.narg('domain_status')::text IS NULL OR ri.domain_status = sqlc.narg('domain_status')::text)
    AND (sqlc.narg('financial_status')::text IS NULL OR ri.financial_status = sqlc.narg('financial_status')::text)
    AND (sqlc.narg('enhanced_status')::text IS NULL OR ri.enhanced_status = sqlc.narg('enhanced_status')::text)
    AND (
      COALESCE(cardinality(sqlc.arg('selected_ids')::text[]), 0) > 0
      OR NOT ri.synced
    )
  ORDER BY rr.organization_number
  LIMIT NULLIF(sqlc.arg('limit')::integer, 0)
),
company_source AS (
  SELECT
    raw.id AS raw_record_id,
    raw.source_native_id,
    raw.organization_number,
    COALESCE(NULLIF(btrim(raw.organization_name), ''), NULLIF(btrim(raw.raw_payload ->> 'navn'), ''), raw.organization_number) AS organization_name,
    lower(COALESCE(NULLIF(btrim(raw.organization_name), ''), NULLIF(btrim(raw.raw_payload ->> 'navn'), ''), raw.organization_number)) AS organization_name_normalized,
    raw.country_iso2,
    raw.registration_status,
    raw.raw_payload,
    raw.payload_hash,
    raw.source_updated_at,
    raw.raw_payload -> 'organisasjonsform' AS organization_form_payload,
    ARRAY(
      SELECT jsonb_array_elements_text(
        CASE
          WHEN jsonb_typeof(raw.raw_payload -> 'aktivitet') = 'array' THEN raw.raw_payload -> 'aktivitet'
          ELSE '[]'::jsonb
        END
      )
    ) AS activity_lines,
    ARRAY(
      SELECT jsonb_array_elements_text(
        CASE
          WHEN jsonb_typeof(raw.raw_payload -> 'vedtektsfestetFormaal') = 'array' THEN raw.raw_payload -> 'vedtektsfestetFormaal'
          ELSE '[]'::jsonb
        END
      )
    ) AS purpose_lines
  FROM selected_raw_records raw
),
upserted_companies AS (
  INSERT INTO brreg_source.companies (
    raw_record_id,
    source_native_id,
    organization_number,
    country_iso2,
    organization_name,
    organization_name_normalized,
    registration_status,
    registration_status_label,
    lifecycle_status,
    organization_form_code,
    organization_form_label,
    language_code,
    response_class,
    founded_date,
    unit_registry_registered_at,
    enterprise_registry_registered_at,
    vat_registry_registered_at,
    vat_registry_unit_registered_at,
    articles_date,
    last_annual_report_year,
    activity_description,
    statutory_purpose,
    is_bankrupt,
    is_in_group,
    is_under_liquidation,
    is_forced_dissolution,
    has_registered_employees,
    in_vat_register,
    in_business_register,
    in_voluntary_register,
    in_foundation_register,
    in_party_register,
    source_updated_at,
    payload_hash,
    normalized_payload,
    raw_company_payload,
    evidence,
    metadata,
    updated_at
  )
  SELECT
    source.raw_record_id,
    source.source_native_id,
    source.organization_number,
    source.country_iso2,
    source.organization_name,
    source.organization_name_normalized,
    source.registration_status,
    CASE source.registration_status
      WHEN 'active' THEN 'active'
      WHEN 'inactive' THEN 'inactive'
      ELSE source.registration_status
    END,
    CASE
      WHEN COALESCE((source.raw_payload ->> 'konkurs')::boolean, false) THEN 'bankrupt'
      WHEN COALESCE((source.raw_payload ->> 'underTvangsavviklingEllerTvangsopplosning')::boolean, false) THEN 'forced_dissolution'
      WHEN COALESCE((source.raw_payload ->> 'underAvvikling')::boolean, false) THEN 'liquidating'
      WHEN source.registration_status = 'active' THEN 'active'
      WHEN source.registration_status = 'inactive' THEN 'inactive'
      ELSE 'unknown'
    END,
    source.organization_form_payload ->> 'kode',
    source.organization_form_payload ->> 'beskrivelse',
    source.raw_payload ->> 'maalform',
    source.raw_payload ->> 'respons_klasse',
    CASE WHEN source.raw_payload ->> 'stiftelsesdato' ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN (source.raw_payload ->> 'stiftelsesdato')::date END,
    CASE WHEN source.raw_payload ->> 'registreringsdatoEnhetsregisteret' ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN (source.raw_payload ->> 'registreringsdatoEnhetsregisteret')::date END,
    CASE WHEN source.raw_payload ->> 'registreringsdatoForetaksregisteret' ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN (source.raw_payload ->> 'registreringsdatoForetaksregisteret')::date END,
    CASE WHEN source.raw_payload ->> 'registreringsdatoMerverdiavgiftsregisteret' ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN (source.raw_payload ->> 'registreringsdatoMerverdiavgiftsregisteret')::date END,
    CASE WHEN source.raw_payload ->> 'registreringsdatoMerverdiavgiftsregisteretEnhetsregisteret' ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN (source.raw_payload ->> 'registreringsdatoMerverdiavgiftsregisteretEnhetsregisteret')::date END,
    CASE WHEN source.raw_payload ->> 'vedtektsdato' ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN (source.raw_payload ->> 'vedtektsdato')::date END,
    CASE WHEN source.raw_payload ->> 'sisteInnsendteAarsregnskap' ~ '^[0-9]{4}$' THEN (source.raw_payload ->> 'sisteInnsendteAarsregnskap')::integer END,
    NULLIF(btrim(array_to_string(source.activity_lines, E'\n')), ''),
    NULLIF(btrim(array_to_string(source.purpose_lines, E'\n')), ''),
    (source.raw_payload ->> 'konkurs')::boolean,
    (source.raw_payload ->> 'erIKonsern')::boolean,
    (source.raw_payload ->> 'underAvvikling')::boolean,
    (source.raw_payload ->> 'underTvangsavviklingEllerTvangsopplosning')::boolean,
    (source.raw_payload ->> 'harRegistrertAntallAnsatte')::boolean,
    (source.raw_payload ->> 'registrertIMvaregisteret')::boolean,
    (source.raw_payload ->> 'registrertIForetaksregisteret')::boolean,
    (source.raw_payload ->> 'registrertIFrivillighetsregisteret')::boolean,
    (source.raw_payload ->> 'registrertIStiftelsesregisteret')::boolean,
    (source.raw_payload ->> 'registrertIPartiregisteret')::boolean,
    source.source_updated_at,
    source.payload_hash,
    jsonb_build_object('source', 'brreg', 'version', 'brreg.source_profile.v1'),
    source.raw_payload,
    jsonb_build_object('source', 'brreg_workflow.raw_records', 'raw_record_id', source.raw_record_id),
    jsonb_build_object('trigger', COALESCE(NULLIF(sqlc.narg('trigger')::text, ''), 'manual')),
    now()
  FROM company_source source
  ON CONFLICT (organization_number) WHERE row_status = 'active'
  DO UPDATE SET
    raw_record_id = EXCLUDED.raw_record_id,
    source_native_id = EXCLUDED.source_native_id,
    country_iso2 = EXCLUDED.country_iso2,
    organization_name = EXCLUDED.organization_name,
    organization_name_normalized = EXCLUDED.organization_name_normalized,
    registration_status = EXCLUDED.registration_status,
    registration_status_label = EXCLUDED.registration_status_label,
    lifecycle_status = EXCLUDED.lifecycle_status,
    organization_form_code = EXCLUDED.organization_form_code,
    organization_form_label = EXCLUDED.organization_form_label,
    language_code = EXCLUDED.language_code,
    response_class = EXCLUDED.response_class,
    founded_date = EXCLUDED.founded_date,
    unit_registry_registered_at = EXCLUDED.unit_registry_registered_at,
    enterprise_registry_registered_at = EXCLUDED.enterprise_registry_registered_at,
    vat_registry_registered_at = EXCLUDED.vat_registry_registered_at,
    vat_registry_unit_registered_at = EXCLUDED.vat_registry_unit_registered_at,
    articles_date = EXCLUDED.articles_date,
    last_annual_report_year = EXCLUDED.last_annual_report_year,
    activity_description = EXCLUDED.activity_description,
    statutory_purpose = EXCLUDED.statutory_purpose,
    is_bankrupt = EXCLUDED.is_bankrupt,
    is_in_group = EXCLUDED.is_in_group,
    is_under_liquidation = EXCLUDED.is_under_liquidation,
    is_forced_dissolution = EXCLUDED.is_forced_dissolution,
    has_registered_employees = EXCLUDED.has_registered_employees,
    in_vat_register = EXCLUDED.in_vat_register,
    in_business_register = EXCLUDED.in_business_register,
    in_voluntary_register = EXCLUDED.in_voluntary_register,
    in_foundation_register = EXCLUDED.in_foundation_register,
    in_party_register = EXCLUDED.in_party_register,
    source_updated_at = EXCLUDED.source_updated_at,
    payload_hash = EXCLUDED.payload_hash,
    normalized_payload = EXCLUDED.normalized_payload,
    raw_company_payload = EXCLUDED.raw_company_payload,
    evidence = EXCLUDED.evidence,
    metadata = brreg_source.companies.metadata || EXCLUDED.metadata,
    updated_at = now()
  RETURNING id, raw_record_id
),
selected_companies AS (
  SELECT
    upserted.id AS company_id,
    raw.*
  FROM upserted_companies upserted
  JOIN selected_raw_records raw ON raw.id = upserted.raw_record_id
),
address_source AS (
  SELECT
    company.company_id,
    company.id AS raw_record_id,
    'business'::text AS address_type,
    company.raw_payload -> 'forretningsadresse' AS raw_address,
    ARRAY(
      SELECT jsonb_array_elements_text(
        CASE
          WHEN jsonb_typeof((company.raw_payload -> 'forretningsadresse') -> 'adresse') = 'array' THEN (company.raw_payload -> 'forretningsadresse') -> 'adresse'
          ELSE '[]'::jsonb
        END
      )
    ) AS street_lines
  FROM selected_companies company
  WHERE jsonb_typeof(company.raw_payload -> 'forretningsadresse') = 'object'
  UNION ALL
  SELECT
    company.company_id,
    company.id AS raw_record_id,
    'postal'::text AS address_type,
    company.raw_payload -> 'postadresse' AS raw_address,
    ARRAY(
      SELECT jsonb_array_elements_text(
        CASE
          WHEN jsonb_typeof((company.raw_payload -> 'postadresse') -> 'adresse') = 'array' THEN (company.raw_payload -> 'postadresse') -> 'adresse'
          ELSE '[]'::jsonb
        END
      )
    ) AS street_lines
  FROM selected_companies company
  WHERE jsonb_typeof(company.raw_payload -> 'postadresse') = 'object'
),
upserted_addresses AS (
  INSERT INTO brreg_source.addresses (
    company_id,
    raw_record_id,
    address_type,
    street_lines,
    street_text,
    postal_code,
    city,
    municipality,
    municipality_number,
    country,
    country_code,
    formatted_address,
    raw_address_payload,
    evidence,
    updated_at
  )
  SELECT
    source.company_id,
    source.raw_record_id,
    source.address_type,
    source.street_lines,
    NULLIF(btrim(array_to_string(source.street_lines, ', ')), ''),
    NULLIF(btrim(source.raw_address ->> 'postnummer'), ''),
    NULLIF(btrim(source.raw_address ->> 'poststed'), ''),
    NULLIF(btrim(source.raw_address ->> 'kommune'), ''),
    NULLIF(btrim(source.raw_address ->> 'kommunenummer'), ''),
    NULLIF(btrim(source.raw_address ->> 'land'), ''),
    NULLIF(btrim(source.raw_address ->> 'landkode'), ''),
    NULLIF(btrim(concat_ws(', ',
      NULLIF(btrim(array_to_string(source.street_lines, ', ')), ''),
      NULLIF(btrim(concat_ws(' ', source.raw_address ->> 'postnummer', source.raw_address ->> 'poststed')), ''),
      NULLIF(btrim(source.raw_address ->> 'land'), '')
    )), ''),
    source.raw_address,
    jsonb_build_object('source', 'brreg_raw_payload', 'field', source.address_type),
    now()
  FROM address_source source
  ON CONFLICT (company_id, address_type)
  DO UPDATE SET
    raw_record_id = EXCLUDED.raw_record_id,
    street_lines = EXCLUDED.street_lines,
    street_text = EXCLUDED.street_text,
    postal_code = EXCLUDED.postal_code,
    city = EXCLUDED.city,
    municipality = EXCLUDED.municipality,
    municipality_number = EXCLUDED.municipality_number,
    country = EXCLUDED.country,
    country_code = EXCLUDED.country_code,
    formatted_address = EXCLUDED.formatted_address,
    raw_address_payload = EXCLUDED.raw_address_payload,
    evidence = EXCLUDED.evidence,
    updated_at = now()
  RETURNING id
),
industry_source_sections AS (
  SELECT
    company.company_id,
    company.id AS raw_record_id,
    'naeringskode1'::text AS source_field,
    'industry'::text AS classification_type,
    1::smallint AS position,
    company.raw_payload -> 'naeringskode1' AS raw_section
  FROM selected_companies company
  WHERE jsonb_typeof(company.raw_payload -> 'naeringskode1') = 'object'
    AND COALESCE((company.raw_payload -> 'naeringskode1') ->> 'kode', '') <> ''
  UNION ALL
  SELECT company.company_id, company.id, 'naeringskode2', 'industry', 2::smallint, company.raw_payload -> 'naeringskode2'
  FROM selected_companies company
  WHERE jsonb_typeof(company.raw_payload -> 'naeringskode2') = 'object'
    AND COALESCE((company.raw_payload -> 'naeringskode2') ->> 'kode', '') <> ''
  UNION ALL
  SELECT company.company_id, company.id, 'naeringskode3', 'industry', 3::smallint, company.raw_payload -> 'naeringskode3'
  FROM selected_companies company
  WHERE jsonb_typeof(company.raw_payload -> 'naeringskode3') = 'object'
    AND COALESCE((company.raw_payload -> 'naeringskode3') ->> 'kode', '') <> ''
  UNION ALL
  SELECT company.company_id, company.id, 'hjelpeenhetskode', 'helper_unit', 1::smallint, company.raw_payload -> 'hjelpeenhetskode'
  FROM selected_companies company
  WHERE jsonb_typeof(company.raw_payload -> 'hjelpeenhetskode') = 'object'
    AND COALESCE((company.raw_payload -> 'hjelpeenhetskode') ->> 'kode', '') <> ''
  UNION ALL
  SELECT company.company_id, company.id, 'institusjonellSektorkode', 'institutional_sector', 1::smallint, company.raw_payload -> 'institusjonellSektorkode'
  FROM selected_companies company
  WHERE jsonb_typeof(company.raw_payload -> 'institusjonellSektorkode') = 'object'
    AND COALESCE((company.raw_payload -> 'institusjonellSektorkode') ->> 'kode', '') <> ''
),
industry_source_codes AS (
  SELECT
    source.*,
    source.raw_section ->> 'kode' AS source_code,
    source.raw_section ->> 'beskrivelse' AS source_label,
    regexp_replace(upper(source.raw_section ->> 'kode'), '[^0-9A-Z]', '', 'g') AS normalized_source_code
  FROM industry_source_sections source
),
industry_mapped_codes AS (
  SELECT
    source.*,
    CASE
      WHEN source.classification_type IN ('industry', 'helper_unit') AND source.normalized_source_code ~ '^[0-9]{5}$'
        THEN substring(source.normalized_source_code from 1 for 2) || '.' || substring(source.normalized_source_code from 3 for 2)
      WHEN source.classification_type IN ('industry', 'helper_unit') AND source.normalized_source_code ~ '^[0-9]{4}$'
        THEN substring(source.normalized_source_code from 1 for 2) || '.' || substring(source.normalized_source_code from 3 for 2)
      ELSE NULL
    END AS mapped_nace_code,
    CASE
      WHEN source.classification_type IN ('industry', 'helper_unit') AND source.normalized_source_code ~ '^[0-9]{5}$' THEN 'sn_level_5_to_nace_class'
      WHEN source.classification_type IN ('industry', 'helper_unit') AND source.normalized_source_code ~ '^[0-9]{4}$' THEN 'nace_exact'
      ELSE NULL
    END AS mapping_method
  FROM industry_source_codes source
),
industry_resolved_codes AS (
  SELECT
    mapped.*,
    nace_code.id AS nace_code_id,
    nace_code.title AS nace_title,
    nace_classification.revision AS nace_revision
  FROM industry_mapped_codes mapped
  LEFT JOIN nace_classifications nace_classification
    ON nace_classification.code_system = 'NACE'
   AND nace_classification.revision = COALESCE(sqlc.narg('nace_revision')::text, '2.1')
  LEFT JOIN nace_codes nace_code
    ON nace_code.classification_id = nace_classification.id
   AND nace_code.code = mapped.mapped_nace_code
   AND nace_code.level_name = 'class'
   AND nace_code.active
),
upserted_industries AS (
  INSERT INTO brreg_source.industries (
    company_id,
    raw_record_id,
    nace_code_id,
    classification_type,
    source_field,
    position,
    source_code,
    source_label,
    mapped_nace_code,
    nace_revision,
    nace_title,
    nace_title_en,
    mapping_method,
    mapping_confidence,
    is_primary,
    raw_industry_payload,
    evidence,
    updated_at
  )
  SELECT
    source.company_id,
    source.raw_record_id,
    source.nace_code_id,
    source.classification_type,
    source.source_field,
    source.position,
    source.source_code,
    source.source_label,
    source.mapped_nace_code,
    source.nace_revision,
    source.nace_title,
    source.nace_title,
    source.mapping_method,
    CASE WHEN source.nace_code_id IS NOT NULL THEN 1::real END,
    source.classification_type = 'industry' AND source.position = 1,
    source.raw_section,
    jsonb_build_object(
      'source', 'brreg_raw_payload',
      'source_field', source.source_field,
      'normalized_source_code', source.normalized_source_code
    ),
    now()
  FROM industry_resolved_codes source
  ON CONFLICT (company_id, classification_type, position)
  DO UPDATE SET
    raw_record_id = EXCLUDED.raw_record_id,
    nace_code_id = EXCLUDED.nace_code_id,
    source_field = EXCLUDED.source_field,
    source_code = EXCLUDED.source_code,
    source_label = EXCLUDED.source_label,
    mapped_nace_code = EXCLUDED.mapped_nace_code,
    nace_revision = EXCLUDED.nace_revision,
    nace_title = EXCLUDED.nace_title,
    nace_title_en = EXCLUDED.nace_title_en,
    mapping_method = EXCLUDED.mapping_method,
    mapping_confidence = EXCLUDED.mapping_confidence,
    is_primary = EXCLUDED.is_primary,
    raw_industry_payload = EXCLUDED.raw_industry_payload,
    evidence = EXCLUDED.evidence,
    updated_at = now()
  RETURNING id
),
website_source AS (
  SELECT
    company.company_id,
    company.id AS raw_record_id,
    CASE
      WHEN btrim(COALESCE(company.website, company.raw_payload ->> 'hjemmeside', '')) ~* '^https?://' THEN btrim(COALESCE(company.website, company.raw_payload ->> 'hjemmeside', ''))
      ELSE 'https://' || btrim(COALESCE(company.website, company.raw_payload ->> 'hjemmeside', ''))
    END AS url
  FROM selected_companies company
  WHERE NULLIF(btrim(COALESCE(company.website, company.raw_payload ->> 'hjemmeside', '')), '') IS NOT NULL
),
website_prepared AS (
  SELECT
    source.*,
    lower(regexp_replace(regexp_replace(source.url, '^https?://', '', 'i'), '/.*$', '')) AS host,
    lower(trim(trailing '/' from source.url)) AS normalized_url
  FROM website_source source
),
upserted_websites AS (
  INSERT INTO brreg_source.websites (
    company_id,
    raw_record_id,
    url,
    normalized_url,
    host,
    website_type,
    source,
    status,
    confidence,
    is_primary,
    evidence,
    updated_at
  )
  SELECT
    source.company_id,
    source.raw_record_id,
    source.url,
    source.normalized_url,
    source.host,
    CASE
      WHEN source.host = ANY(ARRAY['facebook.com', 'instagram.com', 'linkedin.com', 'x.com', 'twitter.com', 'youtube.com']) THEN 'social_profile'
      ELSE 'official_site'
    END,
    'brreg',
    'active',
    90::smallint,
    NOT EXISTS (
      SELECT 1
      FROM brreg_source.websites existing
      WHERE existing.company_id = source.company_id
        AND existing.status = 'active'
        AND existing.is_primary
    ),
    jsonb_build_object('source', 'brreg_website'),
    now()
  FROM website_prepared source
  ON CONFLICT (company_id, normalized_url) WHERE status = 'active'
  DO UPDATE SET
    raw_record_id = EXCLUDED.raw_record_id,
    url = EXCLUDED.url,
    host = EXCLUDED.host,
    website_type = EXCLUDED.website_type,
    confidence = EXCLUDED.confidence,
    is_primary = EXCLUDED.is_primary,
    evidence = EXCLUDED.evidence,
    last_seen_at = now(),
    updated_at = now()
  RETURNING id, company_id, raw_record_id, host
),
domain_source AS (
  SELECT
    website.id AS website_id,
    website.company_id,
    website.raw_record_id,
    regexp_replace(website.host, '^www\.', '') AS normalized_domain
  FROM upserted_websites website
  WHERE website.host IS NOT NULL
    AND website.host LIKE '%.%'
    AND regexp_replace(website.host, '^www\.', '') <> ALL(ARRAY[
      'facebook.com',
      'instagram.com',
      'linkedin.com',
      'x.com',
      'twitter.com',
      'youtube.com',
      'proff.no',
      'brreg.no',
      'gulesider.no',
      '1881.no',
      'yra.no'
    ])
),
upserted_domains AS (
  INSERT INTO brreg_source.domains (
    company_id,
    raw_record_id,
    website_id,
    domain,
    normalized_domain,
    registrable_domain,
    domain_type,
    source,
    status,
    confidence,
    is_primary,
    best_signal,
    evidence,
    updated_at
  )
  SELECT
    source.company_id,
    source.raw_record_id,
    source.website_id,
    source.normalized_domain,
    source.normalized_domain,
    source.normalized_domain,
    'official',
    'brreg_website',
    'active',
    90::smallint,
    NOT EXISTS (
      SELECT 1
      FROM brreg_source.domains existing
      WHERE existing.company_id = source.company_id
        AND existing.status = 'active'
        AND existing.is_primary
    ),
    'brreg_website',
    jsonb_build_object('source', 'brreg_website'),
    now()
  FROM domain_source source
  ON CONFLICT (company_id, normalized_domain) WHERE status = 'active'
  DO UPDATE SET
    raw_record_id = EXCLUDED.raw_record_id,
    website_id = EXCLUDED.website_id,
    domain_type = EXCLUDED.domain_type,
    source = EXCLUDED.source,
    confidence = EXCLUDED.confidence,
    is_primary = EXCLUDED.is_primary,
    best_signal = EXCLUDED.best_signal,
    evidence = EXCLUDED.evidence,
    last_seen_at = now(),
    updated_at = now()
  RETURNING id
),
contact_source AS (
  SELECT
    company.company_id,
    company.id AS raw_record_id,
    'phone'::text AS contact_type,
    NULLIF(btrim(company.raw_payload ->> 'telefon'), '') AS value,
    NULLIF(regexp_replace(company.raw_payload ->> 'telefon', '[^0-9+]', '', 'g'), '') AS normalized_value,
    'phone'::text AS label
  FROM selected_companies company
  WHERE NULLIF(btrim(company.raw_payload ->> 'telefon'), '') IS NOT NULL
  UNION ALL
  SELECT
    company.company_id,
    company.id,
    'mobile',
    NULLIF(btrim(company.raw_payload ->> 'mobil'), ''),
    NULLIF(regexp_replace(company.raw_payload ->> 'mobil', '[^0-9+]', '', 'g'), ''),
    'mobile'
  FROM selected_companies company
  WHERE NULLIF(btrim(company.raw_payload ->> 'mobil'), '') IS NOT NULL
  UNION ALL
  SELECT
    company.company_id,
    company.id,
    'email',
    NULLIF(btrim(company.raw_payload ->> 'epostadresse'), ''),
    lower(NULLIF(btrim(company.raw_payload ->> 'epostadresse'), '')),
    'email'
  FROM selected_companies company
  WHERE NULLIF(btrim(company.raw_payload ->> 'epostadresse'), '') IS NOT NULL
),
upserted_contacts AS (
  INSERT INTO brreg_source.contacts (
    company_id,
    raw_record_id,
    contact_type,
    value,
    normalized_value,
    label,
    source,
    status,
    confidence,
    is_primary,
    evidence,
    updated_at
  )
  SELECT
    source.company_id,
    source.raw_record_id,
    source.contact_type,
    source.value,
    source.normalized_value,
    source.label,
    'brreg',
    'active',
    90::smallint,
    true,
    jsonb_build_object('source', 'brreg_raw_payload'),
    now()
  FROM contact_source source
  ON CONFLICT (company_id, contact_type, normalized_value) WHERE status = 'active' AND normalized_value IS NOT NULL
  DO UPDATE SET
    raw_record_id = EXCLUDED.raw_record_id,
    value = EXCLUDED.value,
    label = EXCLUDED.label,
    confidence = EXCLUDED.confidence,
    is_primary = EXCLUDED.is_primary,
    evidence = EXCLUDED.evidence,
    last_seen_at = now(),
    updated_at = now()
  RETURNING id
),
capital_source AS (
  SELECT
    company.company_id,
    company.id AS raw_record_id,
    company.raw_payload -> 'kapital' AS capital_payload
  FROM selected_companies company
  WHERE jsonb_typeof(company.raw_payload -> 'kapital') = 'object'
),
upserted_capital AS (
  INSERT INTO brreg_source.capital (
    company_id,
    raw_record_id,
    capital_type,
    original_amount,
    original_currency,
    introduced_at,
    share_count,
    raw_capital_payload,
    evidence,
    updated_at
  )
  SELECT
    source.company_id,
    source.raw_record_id,
    source.capital_payload ->> 'type',
    CASE WHEN source.capital_payload ->> 'belop' ~ '^-?[0-9]+([.][0-9]+)?$' THEN (source.capital_payload ->> 'belop')::numeric(20, 2) END,
    source.capital_payload ->> 'valuta',
    CASE WHEN source.capital_payload ->> 'innfortDato' ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN (source.capital_payload ->> 'innfortDato')::date END,
    CASE WHEN source.capital_payload ->> 'antallAksjer' ~ '^[0-9]+$' THEN (source.capital_payload ->> 'antallAksjer')::integer END,
    source.capital_payload,
    jsonb_build_object('source', 'brreg_raw_payload', 'field', 'kapital'),
    now()
  FROM capital_source source
  ON CONFLICT (company_id)
  DO UPDATE SET
    raw_record_id = EXCLUDED.raw_record_id,
    capital_type = EXCLUDED.capital_type,
    original_amount = EXCLUDED.original_amount,
    original_currency = EXCLUDED.original_currency,
    introduced_at = EXCLUDED.introduced_at,
    share_count = EXCLUDED.share_count,
    raw_capital_payload = EXCLUDED.raw_capital_payload,
    evidence = EXCLUDED.evidence,
    updated_at = now()
  RETURNING id
)
SELECT
  (SELECT count(*)::integer FROM selected_raw_records) AS records_seen,
  (SELECT count(*)::integer FROM upserted_companies) AS companies_upserted,
  (SELECT count(*)::integer FROM upserted_addresses) AS addresses_upserted,
  (SELECT count(*)::integer FROM upserted_industries) AS industries_upserted,
  (SELECT count(*)::integer FROM upserted_websites) AS websites_upserted,
  (SELECT count(*)::integer FROM upserted_domains) AS domains_upserted,
  (SELECT count(*)::integer FROM upserted_contacts) AS contacts_upserted,
  (SELECT count(*)::integer FROM upserted_capital) AS capital_upserted;

-- name: PrepareBrregSourceTranslationTasks :one
WITH missing AS (
  SELECT
    missing.company_id,
    missing.source_table,
    missing.source_row_id,
    missing.source_column,
    missing.target_column,
    missing.source_text_hash,
    missing.source_text,
    missing.priority
  FROM brreg_source.v_missing_translations missing
  JOIN brreg_source.companies company ON company.id = missing.company_id
  LEFT JOIN brreg_source.action_tasks existing
    ON existing.action_type = 'translate_field'
   AND existing.source_table = missing.source_table
   AND existing.source_row_id = missing.source_row_id
   AND existing.target_key = missing.target_column
   AND existing.source_fingerprint = missing.source_text_hash
  WHERE (
      COALESCE(cardinality(sqlc.arg('selected_ids')::text[]), 0) = 0
      OR company.raw_record_id::text = ANY(sqlc.arg('selected_ids')::text[])
      OR company.id::text = ANY(sqlc.arg('selected_ids')::text[])
    )
    AND (
      sqlc.narg('query')::text IS NULL
      OR company.organization_name ILIKE '%' || sqlc.narg('query')::text || '%'
      OR company.organization_number ILIKE '%' || sqlc.narg('query')::text || '%'
    )
    AND (
      sqlc.narg('lifecycle_state')::text IS NULL
      OR company.lifecycle_status = sqlc.narg('lifecycle_state')::text
    )
    AND (
      sqlc.narg('translation_status')::text IS NULL
      OR COALESCE(existing.status, 'pending') = sqlc.narg('translation_status')::text
    )
  ORDER BY
    missing.priority ASC,
    company.organization_number ASC,
    missing.source_table ASC,
    missing.source_row_id ASC,
    missing.target_column ASC
  LIMIT NULLIF(GREATEST(sqlc.arg('limit')::integer, 0), 0)
),
inserted AS (
  INSERT INTO brreg_source.action_tasks (
    company_id,
    action_type,
    source_table,
    source_row_id,
    target_key,
    source_fingerprint,
    source_column,
    target_column,
    source_text,
    max_attempts,
    metadata
  )
  SELECT
    missing.company_id,
    'translate_field',
    missing.source_table,
    missing.source_row_id,
    missing.target_column,
    missing.source_text_hash,
    missing.source_column,
    missing.target_column,
    missing.source_text,
    GREATEST(sqlc.arg('max_attempts')::integer, 1),
    jsonb_build_object('priority', missing.priority)
  FROM missing
  ON CONFLICT (action_type, source_table, source_row_id, target_key, source_fingerprint) DO NOTHING
  RETURNING id
)
SELECT count(*)::integer AS records_selected
FROM missing;

-- name: ClaimBrregSourceTranslationBatch :many
WITH lock_task AS (
  SELECT pg_advisory_xact_lock(hashtext('brreg_source.action_tasks.translate_field'))
),
active_slots AS (
  SELECT GREATEST(sqlc.arg('max_parallel_tasks')::integer - count(*)::integer, 0) AS available_slots
  FROM brreg_source.action_tasks task
  CROSS JOIN lock_task
  WHERE task.action_type = 'translate_field'
    AND task.status = 'running'
    AND COALESCE(task.lease_until, task.last_started_at + interval '30 minutes') > now()
),
eligible_tasks AS (
  SELECT
    task.id,
    task.source_table,
    task.source_row_id,
    task.source_column,
    task.target_column,
    task.source_text,
    task.attempt_count,
    missing.priority,
    company.organization_number,
    company.organization_name
  FROM brreg_source.action_tasks task
  JOIN brreg_source.v_missing_translations missing
    ON missing.company_id = task.company_id
   AND missing.source_table = task.source_table
   AND missing.source_row_id = task.source_row_id
   AND missing.target_column = task.target_key
   AND missing.source_text_hash = task.source_fingerprint
  JOIN brreg_source.companies company ON company.id = task.company_id
  WHERE task.action_type = 'translate_field'
    AND (
      task.status = 'pending'
      OR (
        task.status = 'failed_retryable'
        AND task.attempt_count < GREATEST(sqlc.arg('max_attempts')::integer, 1)
      )
      OR (
        task.status = 'running'
        AND task.attempt_count < GREATEST(sqlc.arg('max_attempts')::integer, 1)
        AND COALESCE(task.lease_until, task.last_started_at + interval '30 minutes') <= now()
      )
    )
  ORDER BY
    missing.priority ASC,
    company.organization_number ASC,
    task.updated_at ASC,
    task.id ASC
  LIMIT LEAST(
    GREATEST(sqlc.arg('batch_size')::integer, 1),
    (SELECT available_slots FROM active_slots)
  )
  FOR UPDATE OF task SKIP LOCKED
),
claimed AS (
  UPDATE brreg_source.action_tasks task
  SET
    status = 'running',
    attempt_count = task.attempt_count + 1,
    max_attempts = GREATEST(sqlc.arg('max_attempts')::integer, task.max_attempts, 1),
    lease_until = now() + make_interval(secs => GREATEST(sqlc.arg('lease_seconds')::integer, 1)),
    last_started_at = now(),
    last_finished_at = NULL,
    error = NULL,
    error_category = NULL,
    error_code = NULL,
    retry_strategy = NULL,
    metadata = task.metadata
      || COALESCE(sqlc.narg('metadata')::jsonb, '{}'::jsonb)
      || jsonb_strip_nulls(jsonb_build_object(
        'workflow_run_id', sqlc.arg('workflow_run_id')::uuid::text,
        'worker_id', sqlc.narg('worker_id')::text
      )),
    updated_at = now()
  FROM eligible_tasks eligible
  WHERE task.id = eligible.id
  RETURNING
    task.id,
    task.source_table,
    task.source_column,
    task.target_column,
    task.source_text,
    task.attempt_count,
    eligible.organization_number,
    eligible.organization_name
)
SELECT
  claimed.id AS raw_record_id,
  claimed.id AS task_attempt_id,
  claimed.organization_number,
  claimed.organization_name,
  (
  CASE
    WHEN claimed.source_table = 'brreg_source.companies'
      AND claimed.source_column = 'organization_form_label' THEN
      jsonb_build_object(
        'organisasjonsnummer', claimed.organization_number,
        'organisasjonsform', jsonb_build_object('beskrivelse', claimed.source_text)
      )
    WHEN claimed.source_table = 'brreg_source.industries'
      AND claimed.source_column = 'source_label' THEN
      jsonb_build_object(
        'organisasjonsnummer', claimed.organization_number,
        'naeringskode1', jsonb_build_object('beskrivelse', claimed.source_text)
      )
    WHEN claimed.source_table = 'brreg_source.capital'
      AND claimed.source_column = 'capital_type' THEN
      jsonb_build_object(
        'organisasjonsnummer', claimed.organization_number,
        'kapital', jsonb_build_object('type', claimed.source_text)
      )
    WHEN claimed.source_table = 'brreg_source.companies'
      AND claimed.source_column = 'statutory_purpose' THEN
      jsonb_build_object(
        'organisasjonsnummer', claimed.organization_number,
        'vedtektsfestetFormaal', jsonb_build_array(claimed.source_text)
      )
    ELSE
      jsonb_build_object(
        'organisasjonsnummer', claimed.organization_number,
        'aktivitet', jsonb_build_array(claimed.source_text)
      )
  END
  )::jsonb AS raw_payload,
  claimed.attempt_count::integer AS attempt
FROM claimed
ORDER BY claimed.organization_number ASC, claimed.id ASC;

-- name: CompleteBrregSourceTranslationTask :one
WITH updated_task AS (
  UPDATE brreg_source.action_tasks task
  SET
    status = CASE
      WHEN sqlc.arg('status')::text = 'succeeded' THEN 'succeeded'
      WHEN sqlc.arg('status')::text = 'skipped' THEN 'skipped'
      WHEN sqlc.arg('status')::text = 'failed'
        AND (
          task.attempt_count >= GREATEST(sqlc.arg('max_attempts')::integer, task.max_attempts, 1)
          OR sqlc.narg('retry_strategy')::text IN ('change_model_or_prompt', 'manual_config', 'manual_input', 'not_retryable')
        ) THEN 'failed_terminal'
      WHEN sqlc.arg('status')::text = 'failed' THEN 'failed_retryable'
      ELSE task.status
    END,
    result = CASE
      WHEN sqlc.arg('status')::text = 'succeeded'
        THEN jsonb_build_object('translated_text', sqlc.narg('translated_text')::text)
      ELSE task.result
    END,
    model = sqlc.narg('model')::text,
    prompt_version = sqlc.narg('prompt_version')::text,
    error = sqlc.narg('error')::text,
    error_category = sqlc.narg('error_category')::text,
    error_code = sqlc.narg('error_code')::text,
    retry_strategy = sqlc.narg('retry_strategy')::text,
    lease_until = NULL,
    last_finished_at = now(),
    updated_at = now()
  WHERE task.id = sqlc.arg('task_id')::uuid
    AND task.action_type = 'translate_field'
  RETURNING *
),
successful_task AS (
  SELECT updated_task.*, updated_task.result ->> 'translated_text' AS translated_text
  FROM updated_task
  WHERE status = 'succeeded'
    AND NULLIF(btrim(updated_task.result ->> 'translated_text'), '') IS NOT NULL
),
updated_companies AS (
  UPDATE brreg_source.companies company
  SET
    short_description_en = CASE WHEN task.target_column = 'short_description_en' THEN task.translated_text ELSE company.short_description_en END,
    description_en = CASE WHEN task.target_column = 'description_en' THEN task.translated_text ELSE company.description_en END,
    registration_status_label_en = CASE WHEN task.target_column = 'registration_status_label_en' THEN task.translated_text ELSE company.registration_status_label_en END,
    organization_form_label_en = CASE WHEN task.target_column = 'organization_form_label_en' THEN task.translated_text ELSE company.organization_form_label_en END,
    response_class_en = CASE WHEN task.target_column = 'response_class_en' THEN task.translated_text ELSE company.response_class_en END,
    activity_description_en = CASE WHEN task.target_column = 'activity_description_en' THEN task.translated_text ELSE company.activity_description_en END,
    statutory_purpose_en = CASE WHEN task.target_column = 'statutory_purpose_en' THEN task.translated_text ELSE company.statutory_purpose_en END,
    updated_at = now()
  FROM successful_task task
  WHERE task.source_table = 'brreg_source.companies'
    AND company.id = task.source_row_id
  RETURNING company.id
),
updated_addresses AS (
  UPDATE brreg_source.addresses address
  SET
    country_en = CASE WHEN task.target_column = 'country_en' THEN task.translated_text ELSE address.country_en END,
    updated_at = now()
  FROM successful_task task
  WHERE task.source_table = 'brreg_source.addresses'
    AND address.id = task.source_row_id
  RETURNING address.id
),
updated_industries AS (
  UPDATE brreg_source.industries industry
  SET
    source_label_en = CASE WHEN task.target_column = 'source_label_en' THEN task.translated_text ELSE industry.source_label_en END,
    updated_at = now()
  FROM successful_task task
  WHERE task.source_table = 'brreg_source.industries'
    AND industry.id = task.source_row_id
  RETURNING industry.id
),
updated_websites AS (
  UPDATE brreg_source.websites website
  SET
    title_en = CASE WHEN task.target_column = 'title_en' THEN task.translated_text ELSE website.title_en END,
    description_en = CASE WHEN task.target_column = 'description_en' THEN task.translated_text ELSE website.description_en END,
    updated_at = now()
  FROM successful_task task
  WHERE task.source_table = 'brreg_source.websites'
    AND website.id = task.source_row_id
  RETURNING website.id
),
updated_contacts AS (
  UPDATE brreg_source.contacts contact
  SET
    label_en = CASE WHEN task.target_column = 'label_en' THEN task.translated_text ELSE contact.label_en END,
    updated_at = now()
  FROM successful_task task
  WHERE task.source_table = 'brreg_source.contacts'
    AND contact.id = task.source_row_id
  RETURNING contact.id
),
updated_capital AS (
  UPDATE brreg_source.capital capital
  SET
    capital_type_en = CASE WHEN task.target_column = 'capital_type_en' THEN task.translated_text ELSE capital.capital_type_en END,
    updated_at = now()
  FROM successful_task task
  WHERE task.source_table = 'brreg_source.capital'
    AND capital.id = task.source_row_id
  RETURNING capital.id
),
updated_roles AS (
  UPDATE brreg_source.roles role
  SET
    role_label_en = CASE WHEN task.target_column = 'role_label_en' THEN task.translated_text ELSE role.role_label_en END,
    role_group_en = CASE WHEN task.target_column = 'role_group_en' THEN task.translated_text ELSE role.role_group_en END,
    updated_at = now()
  FROM successful_task task
  WHERE task.source_table = 'brreg_source.roles'
    AND role.id = task.source_row_id
  RETURNING role.id
)
SELECT
  (SELECT count(*)::integer FROM updated_task) AS tasks_updated,
  (
    (SELECT count(*)::integer FROM updated_companies) +
    (SELECT count(*)::integer FROM updated_addresses) +
    (SELECT count(*)::integer FROM updated_industries) +
    (SELECT count(*)::integer FROM updated_websites) +
    (SELECT count(*)::integer FROM updated_contacts) +
    (SELECT count(*)::integer FROM updated_capital) +
    (SELECT count(*)::integer FROM updated_roles)
  )::integer AS source_rows_updated;

-- name: FailRunningBrregSourceTranslationTasksForRun :one
WITH failed AS (
  UPDATE brreg_source.action_tasks task
  SET
    status = CASE
      WHEN task.attempt_count >= GREATEST(sqlc.arg('max_attempts')::integer, task.max_attempts, 1) THEN 'failed_terminal'
      ELSE 'failed_retryable'
    END,
    lease_until = NULL,
    last_finished_at = now(),
    error = sqlc.narg('error')::text,
    error_category = 'workflow_activity',
    error_code = 'activity_failed',
    retry_strategy = 'retry_with_backoff',
    updated_at = now()
  WHERE task.action_type = 'translate_field'
    AND task.status = 'running'
    AND task.metadata ->> 'workflow_run_id' = sqlc.arg('workflow_run_id')::uuid::text
  RETURNING task.id
)
SELECT count(*)::integer AS failed_tasks
FROM failed;

-- name: GetBrregSourceTranslationAssetState :one
WITH source_companies AS (
  SELECT id
  FROM brreg_source.companies
  WHERE row_status = 'active'
),
source_tasks AS (
  SELECT task.*
  FROM brreg_source.action_tasks task
  JOIN source_companies company ON company.id = task.company_id
  WHERE task.action_type = 'translate_field'
),
missing_tasks AS (
  SELECT
    task.id,
    task.status,
    task.attempt_count,
    task.max_attempts,
    task.lease_until,
    task.last_started_at
  FROM brreg_source.v_missing_translations missing
  JOIN source_companies company ON company.id = missing.company_id
  LEFT JOIN brreg_source.action_tasks task
    ON task.action_type = 'translate_field'
   AND task.source_table = missing.source_table
   AND task.source_row_id = missing.source_row_id
   AND task.target_key = missing.target_column
   AND task.source_fingerprint = missing.source_text_hash
)
SELECT
  'action_tasks'::text AS asset,
  (SELECT count(*) FROM source_companies)::bigint AS raw_records_current,
  (SELECT count(*) FROM missing_tasks WHERE id IS NULL)::bigint AS task_no_state,
  (SELECT count(*) FROM source_tasks WHERE status = 'pending')::bigint AS task_pending,
  (
    SELECT count(*)
    FROM source_tasks
    WHERE status = 'running'
      AND COALESCE(lease_until, last_started_at + interval '30 minutes') > now()
  )::bigint AS task_running_active,
  (
    SELECT count(*)
    FROM source_tasks
    WHERE status = 'running'
      AND COALESCE(lease_until, last_started_at + interval '30 minutes') <= now()
  )::bigint AS task_running_stale,
  (SELECT count(*) FROM source_tasks WHERE status = 'failed_retryable')::bigint AS task_failed_retryable,
  (SELECT count(*) FROM source_tasks WHERE status = 'failed_terminal')::bigint AS task_failed_terminal,
  (SELECT count(*) FROM source_tasks WHERE status = 'succeeded')::bigint AS task_succeeded,
  (SELECT count(*) FROM source_tasks WHERE status = 'skipped')::bigint AS task_skipped,
  (
    SELECT count(*)
    FROM missing_tasks
    WHERE id IS NULL
      OR status = 'pending'
      OR (
        status = 'failed_retryable'
        AND attempt_count < GREATEST(max_attempts, 1)
      )
      OR (
        status = 'running'
        AND attempt_count < GREATEST(max_attempts, 1)
        AND COALESCE(lease_until, last_started_at + interval '30 minutes') <= now()
      )
  )::bigint AS task_eligible_now,
  (SELECT count(*) FROM source_tasks WHERE status = 'succeeded')::bigint AS artifact_succeeded,
  (SELECT count(*) FROM source_tasks WHERE status = 'skipped')::bigint AS artifact_skipped,
  (SELECT count(*) FROM source_tasks WHERE status = 'failed_terminal')::bigint AS artifact_failed,
  (SELECT count(*) FROM missing_tasks)::bigint AS artifact_missing;

-- name: CountBrregSourceEntries :one
SELECT count(*)::bigint
FROM brreg_source.mv_company_explorer entry
WHERE (
    sqlc.narg('query')::text IS NULL
    OR entry.organization_name ILIKE '%' || sqlc.narg('query')::text || '%'
    OR entry.organization_number ILIKE '%' || sqlc.narg('query')::text || '%'
    OR entry.primary_industry_label ILIKE '%' || sqlc.narg('query')::text || '%'
    OR entry.city ILIKE '%' || sqlc.narg('query')::text || '%'
    OR entry.municipality ILIKE '%' || sqlc.narg('query')::text || '%'
  )
  AND (
    sqlc.narg('lifecycle_status')::text IS NULL
    OR entry.lifecycle_status = sqlc.narg('lifecycle_status')::text
  )
  AND (
    sqlc.narg('registration_status')::text IS NULL
    OR entry.registration_status = sqlc.narg('registration_status')::text
  )
  AND (
    sqlc.narg('translation_status')::text IS NULL
    OR (
      sqlc.narg('translation_status')::text = 'missing'
      AND entry.translation_missing_count > 0
    )
    OR (
      sqlc.narg('translation_status')::text = 'complete'
      AND entry.translation_missing_count = 0
    )
  )
  AND (
    sqlc.narg('website_status')::text IS NULL
    OR (
      sqlc.narg('website_status')::text = 'with'
      AND entry.website_count > 0
    )
    OR (
      sqlc.narg('website_status')::text = 'without'
      AND entry.website_count = 0
    )
  );

-- name: ListBrregSourceEntries :many
SELECT
  entry.company_id,
  entry.organization_number,
  entry.organization_name,
  entry.description_en,
  entry.lifecycle_status,
  entry.registration_status,
  entry.organization_form_code,
  entry.organization_form_label,
  entry.primary_industry_code,
  entry.primary_industry_label,
  entry.primary_nace_code,
  entry.primary_nace_title,
  entry.city,
  entry.municipality,
  entry.municipality_number,
  entry.county,
  entry.postal_code,
  entry.formatted_address,
  entry.employee_count,
  entry.employee_band,
  coalesce(primary_website.url, '') AS website_url,
  primary_website.host AS website_host,
  entry.website_count,
  entry.domain_count,
  entry.contact_count,
  entry.latest_financial_year,
  entry.latest_revenue_usd_cents,
  entry.latest_total_assets_usd_cents,
  entry.latest_net_income_usd_cents,
  entry.translation_missing_count,
  entry.translation_pending_count,
  entry.translation_running_count,
  entry.translation_succeeded_count,
  entry.translation_failed_count,
  entry.domain_pending_count,
  entry.domain_running_count,
  entry.domain_succeeded_count,
  entry.updated_at
FROM brreg_source.mv_company_explorer entry
LEFT JOIN LATERAL (
  SELECT
    website.url,
    website.host
  FROM brreg_source.websites website
  WHERE website.company_id = entry.company_id
    AND website.status = 'active'
  ORDER BY
    website.is_primary DESC,
    website.confidence DESC NULLS LAST,
    website.created_at DESC
  LIMIT 1
) primary_website ON true
WHERE (
    sqlc.narg('query')::text IS NULL
    OR entry.organization_name ILIKE '%' || sqlc.narg('query')::text || '%'
    OR entry.organization_number ILIKE '%' || sqlc.narg('query')::text || '%'
    OR entry.primary_industry_label ILIKE '%' || sqlc.narg('query')::text || '%'
    OR entry.city ILIKE '%' || sqlc.narg('query')::text || '%'
    OR entry.municipality ILIKE '%' || sqlc.narg('query')::text || '%'
  )
  AND (
    sqlc.narg('lifecycle_status')::text IS NULL
    OR entry.lifecycle_status = sqlc.narg('lifecycle_status')::text
  )
  AND (
    sqlc.narg('registration_status')::text IS NULL
    OR entry.registration_status = sqlc.narg('registration_status')::text
  )
  AND (
    sqlc.narg('translation_status')::text IS NULL
    OR (
      sqlc.narg('translation_status')::text = 'missing'
      AND entry.translation_missing_count > 0
    )
    OR (
      sqlc.narg('translation_status')::text = 'complete'
      AND entry.translation_missing_count = 0
    )
  )
  AND (
    sqlc.narg('website_status')::text IS NULL
    OR (
      sqlc.narg('website_status')::text = 'with'
      AND entry.website_count > 0
    )
    OR (
      sqlc.narg('website_status')::text = 'without'
      AND entry.website_count = 0
    )
  )
ORDER BY
  CASE WHEN sqlc.arg('sort_by')::text = 'organization' AND sqlc.arg('sort_dir')::text = 'asc' THEN entry.organization_name END ASC NULLS LAST,
  CASE WHEN sqlc.arg('sort_by')::text = 'organization' AND sqlc.arg('sort_dir')::text = 'desc' THEN entry.organization_name END DESC NULLS LAST,
  CASE WHEN sqlc.arg('sort_by')::text = 'industry' AND sqlc.arg('sort_dir')::text = 'asc' THEN entry.primary_industry_label END ASC NULLS LAST,
  CASE WHEN sqlc.arg('sort_by')::text = 'industry' AND sqlc.arg('sort_dir')::text = 'desc' THEN entry.primary_industry_label END DESC NULLS LAST,
  CASE WHEN sqlc.arg('sort_by')::text = 'location' AND sqlc.arg('sort_dir')::text = 'asc' THEN entry.city END ASC NULLS LAST,
  CASE WHEN sqlc.arg('sort_by')::text = 'location' AND sqlc.arg('sort_dir')::text = 'desc' THEN entry.city END DESC NULLS LAST,
  CASE WHEN sqlc.arg('sort_by')::text = 'employees' AND sqlc.arg('sort_dir')::text = 'asc' THEN entry.employee_count END ASC NULLS LAST,
  CASE WHEN sqlc.arg('sort_by')::text = 'employees' AND sqlc.arg('sort_dir')::text = 'desc' THEN entry.employee_count END DESC NULLS LAST,
  CASE WHEN sqlc.arg('sort_by')::text = 'revenue' AND sqlc.arg('sort_dir')::text = 'asc' THEN entry.latest_revenue_usd_cents END ASC NULLS LAST,
  CASE WHEN sqlc.arg('sort_by')::text = 'revenue' AND sqlc.arg('sort_dir')::text = 'desc' THEN entry.latest_revenue_usd_cents END DESC NULLS LAST,
  CASE WHEN sqlc.arg('sort_by')::text = 'translation_missing' AND sqlc.arg('sort_dir')::text = 'asc' THEN entry.translation_missing_count END ASC NULLS LAST,
  CASE WHEN sqlc.arg('sort_by')::text = 'translation_missing' AND sqlc.arg('sort_dir')::text = 'desc' THEN entry.translation_missing_count END DESC NULLS LAST,
  CASE WHEN sqlc.arg('sort_by')::text = 'updated_at' AND sqlc.arg('sort_dir')::text = 'asc' THEN entry.updated_at END ASC NULLS LAST,
  CASE WHEN sqlc.arg('sort_by')::text = 'updated_at' AND sqlc.arg('sort_dir')::text = 'desc' THEN entry.updated_at END DESC NULLS LAST,
  entry.updated_at DESC,
  entry.organization_number ASC
LIMIT GREATEST(sqlc.arg('limit')::integer, 1)
OFFSET GREATEST(sqlc.arg('offset')::integer, 0);

-- name: GetBrregSourceCompanyExplorerRefreshSummary :one
SELECT
  count(*)::bigint AS source_entries,
  max(updated_at)::text AS latest_source_updated_at
FROM brreg_source.mv_company_explorer;

-- name: GetBrregSourceCompanyDetail :one
SELECT detail.*
FROM brreg_source.v_company_detail detail
WHERE detail.id = sqlc.arg('company_id')::uuid;

-- name: ConvertBrregSourceCapitalToUSD :one
WITH selected_sheet AS (
  SELECT
    sheet.id,
    sheet.provider,
    sheet.rate_date,
    sheet.base_currency
  FROM exchange_rate_sheets sheet
  WHERE sheet.provider = 'ecb'
    AND (
      sqlc.narg('rate_date')::date IS NULL
      OR sheet.rate_date = sqlc.narg('rate_date')::date
    )
  ORDER BY sheet.rate_date DESC
  LIMIT 1
),
usd_rate AS (
  SELECT
    rate.sheet_id,
    rate.rate_per_base
  FROM exchange_rates rate
  JOIN selected_sheet sheet ON sheet.id = rate.sheet_id
  WHERE rate.currency = 'USD'
),
scoped_capital AS (
  SELECT
    capital.id,
    capital.company_id,
    capital.original_amount,
    upper(btrim(capital.original_currency)) AS original_currency,
    capital.amount_usd_cents,
    company.organization_number,
    company.organization_name,
    company.updated_at
  FROM brreg_source.capital capital
  JOIN brreg_source.companies company ON company.id = capital.company_id
  JOIN brreg_source.v_company_explorer entry ON entry.company_id = company.id
  WHERE capital.original_amount IS NOT NULL
    AND nullif(btrim(capital.original_currency), '') IS NOT NULL
    AND (
      COALESCE(cardinality(sqlc.arg('selected_ids')::text[]), 0) = 0
      OR company.id::text = ANY(sqlc.arg('selected_ids')::text[])
      OR company.raw_record_id::text = ANY(sqlc.arg('selected_ids')::text[])
    )
    AND (
      sqlc.narg('query')::text IS NULL
      OR entry.organization_name ILIKE '%' || sqlc.narg('query')::text || '%'
      OR entry.organization_number ILIKE '%' || sqlc.narg('query')::text || '%'
      OR entry.primary_industry_label ILIKE '%' || sqlc.narg('query')::text || '%'
      OR entry.city ILIKE '%' || sqlc.narg('query')::text || '%'
      OR entry.municipality ILIKE '%' || sqlc.narg('query')::text || '%'
    )
    AND (
      sqlc.narg('lifecycle_status')::text IS NULL
      OR entry.lifecycle_status = sqlc.narg('lifecycle_status')::text
    )
    AND (
      sqlc.narg('registration_status')::text IS NULL
      OR entry.registration_status = sqlc.narg('registration_status')::text
    )
    AND (
      sqlc.narg('translation_status')::text IS NULL
      OR (
        sqlc.narg('translation_status')::text = 'missing'
        AND entry.translation_missing_count > 0
      )
      OR (
        sqlc.narg('translation_status')::text = 'complete'
        AND entry.translation_missing_count = 0
      )
    )
    AND (
      sqlc.narg('website_status')::text IS NULL
      OR (
        sqlc.narg('website_status')::text = 'with'
        AND entry.website_count > 0
      )
      OR (
        sqlc.narg('website_status')::text = 'without'
        AND entry.website_count = 0
      )
    )
),
limited_capital AS (
  SELECT *
  FROM (
    SELECT
      scoped_capital.*,
      row_number() OVER (
        ORDER BY scoped_capital.updated_at DESC, scoped_capital.organization_number ASC, scoped_capital.id ASC
      ) AS row_number
    FROM scoped_capital
  ) numbered
  WHERE sqlc.arg('limit')::integer = 0
     OR numbered.row_number <= sqlc.arg('limit')::integer
),
eligible_capital AS (
  SELECT *
  FROM limited_capital
  WHERE sqlc.arg('force_reprocess')::boolean
     OR amount_usd_cents IS NULL
),
capital_with_rates AS (
  SELECT
    capital.id,
    capital.original_amount,
    capital.original_currency,
    sheet.id AS sheet_id,
    sheet.provider,
    sheet.rate_date,
    sheet.base_currency,
    usd.rate_per_base AS usd_rate_per_base,
    CASE
      WHEN capital.original_currency = sheet.base_currency THEN 1::numeric
      ELSE source_rate.rate_per_base
    END AS source_rate_per_base
  FROM eligible_capital capital
  LEFT JOIN selected_sheet sheet ON true
  LEFT JOIN usd_rate usd ON usd.sheet_id = sheet.id
  LEFT JOIN exchange_rates source_rate
    ON source_rate.sheet_id = sheet.id
   AND source_rate.currency = capital.original_currency
),
missing_rate AS (
  SELECT *
  FROM capital_with_rates
  WHERE sheet_id IS NULL
     OR usd_rate_per_base IS NULL
     OR source_rate_per_base IS NULL
),
converted AS (
  UPDATE brreg_source.capital capital
  SET
    amount_usd_cents = round(capital_with_rates.original_amount * capital_with_rates.usd_rate_per_base / capital_with_rates.source_rate_per_base * 100)::bigint,
    fx_source = capital_with_rates.provider,
    fx_rate_date = capital_with_rates.rate_date,
    fx_metadata = jsonb_build_object(
      'provider', capital_with_rates.provider,
      'sheet_id', capital_with_rates.sheet_id::text,
      'rate_date', capital_with_rates.rate_date,
      'base_currency', capital_with_rates.base_currency,
      'source_currency', capital_with_rates.original_currency,
      'target_currency', 'USD',
      'source_rate_per_base', capital_with_rates.source_rate_per_base,
      'target_rate_per_base', capital_with_rates.usd_rate_per_base,
      'trigger', COALESCE(sqlc.narg('trigger')::text, 'manual'),
      'force_reprocess', sqlc.arg('force_reprocess')::boolean
    ),
    updated_at = now()
  FROM capital_with_rates
  WHERE capital.id = capital_with_rates.id
    AND capital_with_rates.sheet_id IS NOT NULL
    AND capital_with_rates.usd_rate_per_base IS NOT NULL
    AND capital_with_rates.source_rate_per_base IS NOT NULL
  RETURNING capital.id
)
SELECT
  (SELECT count(*)::integer FROM limited_capital) AS capital_seen,
  (SELECT count(*)::integer FROM converted) AS capital_converted,
  (SELECT count(*)::integer FROM missing_rate) AS capital_skipped_missing_rate,
  (
    SELECT count(*)::integer
    FROM limited_capital
    WHERE NOT sqlc.arg('force_reprocess')::boolean
      AND amount_usd_cents IS NOT NULL
  ) AS capital_skipped_already_converted,
  COALESCE((SELECT rate_date::text FROM selected_sheet), '')::text AS rate_date;

-- name: InsertPendingBrregTranslationTerms :one
WITH inserted AS (
  INSERT INTO brreg_source.translation_terms (
    source,
    source_lang,
    target_lang,
    source_text_normalized,
    source_text,
    term_key,
    status,
    provider,
    model,
    prompt_version,
    metadata,
    last_requested_at,
    updated_at
  )
  SELECT
    'brreg',
    missing.source_lang,
    missing.target_lang,
    missing.source_text_normalized,
    min(missing.source_text),
    missing.term_key,
    'pending',
    sqlc.narg('provider')::text,
    sqlc.narg('model')::text,
    sqlc.arg('prompt_version')::text,
    jsonb_build_object('workflow_id', sqlc.narg('workflow_id')::text),
    now(),
    now()
  FROM brreg_source.v_missing_translation_fields missing
  LEFT JOIN brreg_source.translation_terms existing
    ON existing.source = 'brreg'
   AND existing.source_lang = missing.source_lang
   AND existing.target_lang = missing.target_lang
   AND existing.prompt_version = sqlc.arg('prompt_version')::text
   AND existing.term_key = missing.term_key
  WHERE existing.id IS NULL
  GROUP BY missing.source_lang, missing.target_lang, missing.source_text_normalized, missing.term_key
  LIMIT CASE WHEN sqlc.arg('limit')::integer <= 0 THEN NULL ELSE sqlc.arg('limit')::integer END
  ON CONFLICT (source, source_lang, target_lang, prompt_version, term_key) DO UPDATE
  SET last_requested_at = now(),
      updated_at = now()
  RETURNING id
)
SELECT count(*)::integer AS terms_inserted
FROM inserted;

-- name: MarkBrregTranslationTermsQueued :many
WITH picked AS (
  SELECT id
  FROM brreg_source.translation_terms
  WHERE source = 'brreg'
    AND status IN ('pending', 'failed_retryable')
    AND prompt_version = sqlc.arg('prompt_version')::text
    AND attempt_count < sqlc.arg('max_attempts')::integer
  ORDER BY updated_at, id
  LIMIT sqlc.arg('limit')::integer
  FOR UPDATE SKIP LOCKED
),
queued AS (
  UPDATE brreg_source.translation_terms term
  SET status = 'queued',
      attempt_count = term.attempt_count + 1,
      provider = sqlc.narg('provider')::text,
      model = sqlc.narg('model')::text,
      last_requested_at = now(),
      updated_at = now()
  FROM picked
  WHERE term.id = picked.id
  RETURNING
    term.id,
    term.source_lang,
    term.target_lang,
    term.source_text_normalized,
    term.source_text,
    term.term_key,
    term.attempt_count
)
SELECT * FROM queued;

-- name: UpsertBrregTranslationTermResult :exec
INSERT INTO brreg_source.translation_terms (
  source,
  source_lang,
  target_lang,
  source_text_normalized,
  source_text,
  term_key,
  translated_text,
  status,
  provider,
  model,
  prompt_version,
  error,
  error_code,
  metadata,
  translated_at,
  updated_at
) VALUES (
  'brreg',
  sqlc.arg('source_lang')::text,
  sqlc.arg('target_lang')::text,
  sqlc.arg('source_text_normalized')::text,
  sqlc.arg('source_text')::text,
  sqlc.arg('term_key')::text,
  sqlc.narg('translated_text')::text,
  sqlc.arg('status')::text,
  sqlc.narg('provider')::text,
  sqlc.narg('model')::text,
  sqlc.arg('prompt_version')::text,
  sqlc.narg('error')::text,
  sqlc.narg('error_code')::text,
  sqlc.arg('metadata')::jsonb,
  CASE WHEN sqlc.arg('status')::text = 'succeeded' THEN now() ELSE NULL END,
  now()
)
ON CONFLICT (source, source_lang, target_lang, prompt_version, term_key) DO UPDATE
SET translated_text = EXCLUDED.translated_text,
    status = EXCLUDED.status,
    provider = EXCLUDED.provider,
    model = EXCLUDED.model,
    error = EXCLUDED.error,
    error_code = EXCLUDED.error_code,
    metadata = brreg_source.translation_terms.metadata || EXCLUDED.metadata,
    translated_at = CASE WHEN EXCLUDED.status = 'succeeded' THEN now() ELSE brreg_source.translation_terms.translated_at END,
    updated_at = now();

-- name: ApplyBrregSourceCompanyCachedTranslationTerms :one
WITH matched AS (
  SELECT missing.source_row_id, missing.target_column, term.translated_text
  FROM brreg_source.v_missing_translation_fields missing
  JOIN brreg_source.translation_terms term
    ON term.source = 'brreg'
   AND term.source_lang = missing.source_lang
   AND term.target_lang = missing.target_lang
   AND term.prompt_version = sqlc.arg('prompt_version')::text
   AND term.term_key = missing.term_key
   AND term.status = 'succeeded'
  WHERE missing.source_table = 'brreg_source.companies'
    AND missing.target_column IN (
      'organization_form_label_en',
      'response_class_en',
      'activity_description_en',
      'statutory_purpose_en'
    )
  ORDER BY missing.source_row_id, missing.target_column
  LIMIT CASE WHEN sqlc.arg('limit')::integer <= 0 THEN NULL ELSE sqlc.arg('limit')::integer END
),
matched_rows AS (
  SELECT
    source_row_id,
    max(translated_text) FILTER (WHERE target_column = 'organization_form_label_en') AS organization_form_label_en,
    max(translated_text) FILTER (WHERE target_column = 'response_class_en') AS response_class_en,
    max(translated_text) FILTER (WHERE target_column = 'activity_description_en') AS activity_description_en,
    max(translated_text) FILTER (WHERE target_column = 'statutory_purpose_en') AS statutory_purpose_en,
    count(*)::integer AS field_count
  FROM matched
  GROUP BY source_row_id
),
updated AS (
  UPDATE brreg_source.companies company
  SET organization_form_label_en = COALESCE(matched_rows.organization_form_label_en, company.organization_form_label_en),
      response_class_en = COALESCE(matched_rows.response_class_en, company.response_class_en),
      activity_description_en = COALESCE(matched_rows.activity_description_en, company.activity_description_en),
      statutory_purpose_en = COALESCE(matched_rows.statutory_purpose_en, company.statutory_purpose_en),
      updated_at = now()
  FROM matched_rows
  WHERE company.id = matched_rows.source_row_id
  RETURNING matched_rows.field_count
)
SELECT COALESCE(sum(field_count), 0)::integer AS fields_applied FROM updated;

-- name: ApplyBrregSourceCapitalCachedTranslationTerms :one
WITH matched AS (
  SELECT missing.source_row_id, term.translated_text
  FROM brreg_source.v_missing_translation_fields missing
  JOIN brreg_source.translation_terms term
    ON term.source = 'brreg'
   AND term.source_lang = missing.source_lang
   AND term.target_lang = missing.target_lang
   AND term.prompt_version = sqlc.arg('prompt_version')::text
   AND term.term_key = missing.term_key
   AND term.status = 'succeeded'
  WHERE missing.source_table = 'brreg_source.capital'
    AND missing.target_column = 'capital_type_en'
  ORDER BY missing.source_row_id
  LIMIT CASE WHEN sqlc.arg('limit')::integer <= 0 THEN NULL ELSE sqlc.arg('limit')::integer END
),
updated AS (
  UPDATE brreg_source.capital capital
  SET capital_type_en = matched.translated_text,
      updated_at = now()
  FROM matched
  WHERE capital.id = matched.source_row_id
  RETURNING capital.id
)
SELECT count(*)::integer AS fields_applied FROM updated;

-- name: CountBrregMissingTranslationFields :one
SELECT count(*)::integer AS missing_fields
FROM brreg_source.v_missing_translation_fields;
