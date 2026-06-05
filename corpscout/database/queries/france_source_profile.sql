-- name: SelectFranceSourceProfileLegalUnitIDs :many
SELECT rr.id
FROM france_workflow.raw_legal_units rr
LEFT JOIN france_source.companies source_company
  ON source_company.siren = rr.siren
 AND source_company.row_status = 'active'
WHERE rr.is_current
  AND (
    COALESCE(cardinality(sqlc.arg('ids')::text[]), 0) = 0
    OR rr.id::text = ANY(sqlc.arg('ids')::text[])
    OR rr.siren = ANY(sqlc.arg('ids')::text[])
    OR source_company.id::text = ANY(sqlc.arg('ids')::text[])
    OR EXISTS (
      SELECT 1
      FROM france_workflow.raw_establishments selected_establishment
      WHERE selected_establishment.siren = rr.siren
        AND (
          selected_establishment.id::text = ANY(sqlc.arg('ids')::text[])
          OR selected_establishment.siret = ANY(sqlc.arg('ids')::text[])
        )
    )
  )
  AND (
    sqlc.narg('query')::text IS NULL
    OR rr.siren ILIKE '%' || sqlc.narg('query')::text || '%'
    OR rr.legal_name ILIKE '%' || sqlc.narg('query')::text || '%'
    OR rr.usual_name_1 ILIKE '%' || sqlc.narg('query')::text || '%'
    OR rr.primary_activity_code ILIKE '%' || sqlc.narg('query')::text || '%'
    OR EXISTS (
      SELECT 1
      FROM france_workflow.raw_establishments searched_establishment
      WHERE searched_establishment.siren = rr.siren
        AND searched_establishment.is_current
        AND (
          searched_establishment.siret ILIKE '%' || sqlc.narg('query')::text || '%'
          OR searched_establishment.trade_name_1 ILIKE '%' || sqlc.narg('query')::text || '%'
          OR searched_establishment.usual_name ILIKE '%' || sqlc.narg('query')::text || '%'
          OR searched_establishment.city_label ILIKE '%' || sqlc.narg('query')::text || '%'
        )
    )
  )
  AND (
    COALESCE(cardinality(sqlc.arg('ids')::text[]), 0) > 0
    OR source_company.id IS NULL
    OR source_company.payload_hash IS DISTINCT FROM rr.payload_hash
    OR EXISTS (
      SELECT 1
      FROM france_workflow.raw_establishments current_establishment
      LEFT JOIN france_source.establishments source_establishment
        ON source_establishment.siret = current_establishment.siret
       AND source_establishment.row_status = 'active'
      WHERE current_establishment.siren = rr.siren
        AND current_establishment.is_current
        AND (
          source_establishment.id IS NULL
          OR source_establishment.payload_hash IS DISTINCT FROM current_establishment.payload_hash
        )
    )
  )
ORDER BY rr.siren
LIMIT GREATEST(sqlc.arg('limit')::integer, 1);

-- name: SupersedeFranceSourceCompaniesForLegalUnits :exec
UPDATE france_source.companies source_company
SET
  row_status = 'superseded',
  superseded_at = now(),
  updated_at = now()
FROM france_workflow.raw_legal_units rr
WHERE rr.id = ANY(sqlc.arg('raw_legal_unit_ids')::uuid[])
  AND rr.is_current
  AND source_company.siren = rr.siren
  AND source_company.row_status = 'active'
  AND source_company.payload_hash IS DISTINCT FROM rr.payload_hash;

-- name: UpsertFranceSourceCompanies :one
WITH selected AS (
  SELECT
    rr.*,
    COALESCE(
      NULLIF(btrim(rr.legal_name), ''),
      NULLIF(btrim(rr.usage_name), ''),
      NULLIF(btrim(rr.birth_name), ''),
      NULLIF(btrim(rr.usual_name_1), ''),
      rr.siren
    ) AS organization_name_value
  FROM france_workflow.raw_legal_units rr
  WHERE rr.id = ANY(sqlc.arg('raw_legal_unit_ids')::uuid[])
    AND rr.is_current
),
upserted AS (
  INSERT INTO france_source.companies (
    raw_legal_unit_id,
    siren,
    source_native_id,
    country_iso2,
    organization_name,
    organization_name_normalized,
    acronym,
    diffusion_status,
    is_partial_diffusion,
    registration_status,
    registration_status_label,
    lifecycle_status,
    is_purged,
    legal_form_code,
    company_category,
    company_category_year,
    primary_activity_code,
    primary_activity_nomenclature,
    primary_activity_naf25_code,
    employee_band,
    employee_band_year,
    founded_date,
    period_started_at,
    period_count,
    headquarters_nic,
    headquarters_siret,
    association_identifier,
    social_solidarity,
    mission_company,
    is_employer,
    source_updated_at,
    payload_hash,
    row_status,
    normalized_payload,
    raw_company_payload,
    evidence,
    metadata,
    updated_at
  )
  SELECT
    selected.id,
    selected.siren,
    selected.siren,
    'FR',
    selected.organization_name_value,
    upper(selected.organization_name_value),
    NULLIF(btrim(selected.acronym), ''),
    selected.diffusion_status,
    COALESCE(selected.diffusion_status = 'P', false),
    selected.administrative_status,
    CASE selected.administrative_status
      WHEN 'A' THEN 'Active'
      WHEN 'C' THEN 'Cessée'
      ELSE NULL
    END,
    CASE selected.administrative_status
      WHEN 'A' THEN 'active'
      WHEN 'C' THEN 'inactive'
      ELSE 'unknown'
    END,
    selected.is_purged,
    selected.legal_form_code,
    selected.company_category,
    selected.company_category_year,
    selected.primary_activity_code,
    selected.primary_activity_nomenclature,
    selected.primary_activity_naf25_code,
    selected.employee_band,
    selected.employee_year,
    selected.created_date,
    selected.period_started_at,
    selected.period_count,
    selected.headquarters_nic,
    CASE
      WHEN selected.headquarters_nic ~ '^[0-9]{5}$' THEN selected.siren || selected.headquarters_nic
      ELSE NULL
    END,
    selected.association_identifier,
    selected.social_solidarity,
    selected.mission_company,
    selected.is_employer,
    selected.source_updated_at,
    selected.payload_hash,
    'active',
    jsonb_strip_nulls(jsonb_build_object(
      'siren', selected.siren,
      'organization_name', selected.organization_name_value,
      'administrative_status', selected.administrative_status,
      'legal_form_code', selected.legal_form_code,
      'company_category', selected.company_category,
      'primary_activity_code', selected.primary_activity_code,
      'headquarters_nic', selected.headquarters_nic
    )),
    selected.raw_payload,
    jsonb_build_object(
      'source_table', 'france_workflow.raw_legal_units',
      'raw_legal_unit_id', selected.id,
      'source_native_id', selected.source_native_id
    ),
    jsonb_build_object(
      'source', 'france',
      'trigger', sqlc.arg('trigger')::text,
      'normalized_at', now()
    ),
    now()
  FROM selected
  ON CONFLICT (siren) WHERE row_status = 'active'
  DO UPDATE SET
    raw_legal_unit_id = EXCLUDED.raw_legal_unit_id,
    organization_name = EXCLUDED.organization_name,
    organization_name_normalized = EXCLUDED.organization_name_normalized,
    acronym = EXCLUDED.acronym,
    diffusion_status = EXCLUDED.diffusion_status,
    is_partial_diffusion = EXCLUDED.is_partial_diffusion,
    registration_status = EXCLUDED.registration_status,
    registration_status_label = EXCLUDED.registration_status_label,
    lifecycle_status = EXCLUDED.lifecycle_status,
    is_purged = EXCLUDED.is_purged,
    legal_form_code = EXCLUDED.legal_form_code,
    company_category = EXCLUDED.company_category,
    company_category_year = EXCLUDED.company_category_year,
    primary_activity_code = EXCLUDED.primary_activity_code,
    primary_activity_nomenclature = EXCLUDED.primary_activity_nomenclature,
    primary_activity_naf25_code = EXCLUDED.primary_activity_naf25_code,
    employee_band = EXCLUDED.employee_band,
    employee_band_year = EXCLUDED.employee_band_year,
    founded_date = EXCLUDED.founded_date,
    period_started_at = EXCLUDED.period_started_at,
    period_count = EXCLUDED.period_count,
    headquarters_nic = EXCLUDED.headquarters_nic,
    headquarters_siret = EXCLUDED.headquarters_siret,
    association_identifier = EXCLUDED.association_identifier,
    social_solidarity = EXCLUDED.social_solidarity,
    mission_company = EXCLUDED.mission_company,
    is_employer = EXCLUDED.is_employer,
    source_updated_at = EXCLUDED.source_updated_at,
    payload_hash = EXCLUDED.payload_hash,
    normalized_payload = EXCLUDED.normalized_payload,
    raw_company_payload = EXCLUDED.raw_company_payload,
    evidence = EXCLUDED.evidence,
    metadata = france_source.companies.metadata || EXCLUDED.metadata,
    updated_at = now()
  RETURNING id
)
SELECT count(*)::integer AS rows_upserted
FROM upserted;

-- name: SupersedeFranceSourceEstablishmentsForLegalUnits :exec
UPDATE france_source.establishments source_establishment
SET
  row_status = 'superseded',
  superseded_at = now(),
  updated_at = now()
FROM france_workflow.raw_establishments raw_establishment
JOIN france_source.companies source_company
  ON source_company.siren = raw_establishment.siren
 AND source_company.row_status = 'active'
WHERE raw_establishment.siren IN (
    SELECT siren
    FROM france_workflow.raw_legal_units
    WHERE id = ANY(sqlc.arg('raw_legal_unit_ids')::uuid[])
      AND is_current
  )
  AND raw_establishment.is_current
  AND source_establishment.siret = raw_establishment.siret
  AND source_establishment.row_status = 'active'
  AND (
    source_establishment.payload_hash IS DISTINCT FROM raw_establishment.payload_hash
    OR source_establishment.company_id <> source_company.id
  );

-- name: DemoteFranceSourceHeadquartersForLegalUnits :exec
UPDATE france_source.establishments source_establishment
SET
  is_headquarters = false,
  updated_at = now()
FROM france_source.companies source_company
WHERE source_company.id = source_establishment.company_id
  AND source_company.row_status = 'active'
  AND source_establishment.row_status = 'active'
  AND source_establishment.is_headquarters
  AND source_company.raw_legal_unit_id = ANY(sqlc.arg('raw_legal_unit_ids')::uuid[]);

-- name: UpsertFranceSourceEstablishments :one
WITH selected AS (
  SELECT
    raw_establishment.*,
    source_company.id AS company_id,
    COALESCE(
      NULLIF(btrim(raw_establishment.trade_name_1), ''),
      NULLIF(btrim(raw_establishment.usual_name), ''),
      NULLIF(btrim(source_company.organization_name), '')
    ) AS establishment_name_value
  FROM france_workflow.raw_establishments raw_establishment
  JOIN france_source.companies source_company
    ON source_company.siren = raw_establishment.siren
   AND source_company.row_status = 'active'
  WHERE raw_establishment.siren IN (
      SELECT siren
      FROM france_workflow.raw_legal_units
      WHERE id = ANY(sqlc.arg('raw_legal_unit_ids')::uuid[])
        AND is_current
    )
    AND raw_establishment.is_current
),
upserted AS (
  INSERT INTO france_source.establishments (
    company_id,
    raw_establishment_id,
    siren,
    siret,
    nic,
    source_native_id,
    country_iso2,
    establishment_name,
    establishment_name_normalized,
    trade_name,
    usual_name,
    is_headquarters,
    diffusion_status,
    is_partial_diffusion,
    registration_status,
    registration_status_label,
    lifecycle_status,
    primary_activity_code,
    primary_activity_nomenclature,
    primary_activity_naf25_code,
    crafts_registry_activity,
    employee_band,
    employee_band_year,
    founded_date,
    period_started_at,
    period_count,
    is_employer,
    source_updated_at,
    payload_hash,
    row_status,
    normalized_payload,
    raw_establishment_payload,
    evidence,
    metadata,
    updated_at
  )
  SELECT
    selected.company_id,
    selected.id,
    selected.siren,
    selected.siret,
    selected.nic,
    selected.siret,
    'FR',
    selected.establishment_name_value,
    CASE WHEN selected.establishment_name_value IS NULL THEN NULL ELSE upper(selected.establishment_name_value) END,
    NULLIF(btrim(concat_ws(' ', selected.trade_name_1, selected.trade_name_2, selected.trade_name_3)), ''),
    NULLIF(btrim(selected.usual_name), ''),
    COALESCE(selected.is_headquarters, false),
    selected.diffusion_status,
    COALESCE(selected.diffusion_status = 'P', false),
    selected.administrative_status,
    CASE selected.administrative_status
      WHEN 'A' THEN 'Active'
      WHEN 'F' THEN 'Fermé'
      ELSE NULL
    END,
    CASE selected.administrative_status
      WHEN 'A' THEN 'active'
      WHEN 'F' THEN 'inactive'
      ELSE 'unknown'
    END,
    selected.primary_activity_code,
    selected.primary_activity_nomenclature,
    selected.primary_activity_naf25_code,
    selected.crafts_registry_activity,
    selected.employee_band,
    selected.employee_year,
    selected.created_date,
    selected.period_started_at,
    selected.period_count,
    selected.is_employer,
    selected.source_updated_at,
    selected.payload_hash,
    'active',
    jsonb_strip_nulls(jsonb_build_object(
      'siren', selected.siren,
      'siret', selected.siret,
      'nic', selected.nic,
      'is_headquarters', selected.is_headquarters,
      'administrative_status', selected.administrative_status,
      'primary_activity_code', selected.primary_activity_code
    )),
    selected.raw_payload,
    jsonb_build_object(
      'source_table', 'france_workflow.raw_establishments',
      'raw_establishment_id', selected.id,
      'source_native_id', selected.source_native_id
    ),
    jsonb_build_object(
      'source', 'france',
      'trigger', sqlc.arg('trigger')::text,
      'normalized_at', now()
    ),
    now()
  FROM selected
  ON CONFLICT (siret) WHERE row_status = 'active'
  DO UPDATE SET
    company_id = EXCLUDED.company_id,
    raw_establishment_id = EXCLUDED.raw_establishment_id,
    establishment_name = EXCLUDED.establishment_name,
    establishment_name_normalized = EXCLUDED.establishment_name_normalized,
    trade_name = EXCLUDED.trade_name,
    usual_name = EXCLUDED.usual_name,
    is_headquarters = EXCLUDED.is_headquarters,
    diffusion_status = EXCLUDED.diffusion_status,
    is_partial_diffusion = EXCLUDED.is_partial_diffusion,
    registration_status = EXCLUDED.registration_status,
    registration_status_label = EXCLUDED.registration_status_label,
    lifecycle_status = EXCLUDED.lifecycle_status,
    primary_activity_code = EXCLUDED.primary_activity_code,
    primary_activity_nomenclature = EXCLUDED.primary_activity_nomenclature,
    primary_activity_naf25_code = EXCLUDED.primary_activity_naf25_code,
    crafts_registry_activity = EXCLUDED.crafts_registry_activity,
    employee_band = EXCLUDED.employee_band,
    employee_band_year = EXCLUDED.employee_band_year,
    founded_date = EXCLUDED.founded_date,
    period_started_at = EXCLUDED.period_started_at,
    period_count = EXCLUDED.period_count,
    is_employer = EXCLUDED.is_employer,
    source_updated_at = EXCLUDED.source_updated_at,
    payload_hash = EXCLUDED.payload_hash,
    normalized_payload = EXCLUDED.normalized_payload,
    raw_establishment_payload = EXCLUDED.raw_establishment_payload,
    evidence = EXCLUDED.evidence,
    metadata = france_source.establishments.metadata || EXCLUDED.metadata,
    updated_at = now()
  RETURNING id
)
SELECT count(*)::integer AS rows_upserted
FROM upserted;

-- name: UpsertFranceSourceAddresses :one
WITH base AS (
  SELECT
    source_establishment.company_id,
    source_establishment.id AS establishment_id,
    source_establishment.raw_establishment_id,
    source_establishment.is_headquarters AS source_is_headquarters,
    raw_establishment.address_complement,
    raw_establishment.street_number,
    raw_establishment.street_number_suffix,
    raw_establishment.last_street_number,
    raw_establishment.last_street_number_suffix,
    raw_establishment.street_type,
    raw_establishment.street_label,
    raw_establishment.postal_code,
    raw_establishment.city_label,
    raw_establishment.foreign_city_label,
    raw_establishment.special_distribution,
    raw_establishment.commune_code,
    raw_establishment.cedex_code,
    raw_establishment.cedex_label,
    raw_establishment.foreign_country_code,
    raw_establishment.foreign_country_label,
    raw_establishment.address_identifier,
    raw_establishment.lambert_x,
    raw_establishment.lambert_y,
    raw_establishment.address_complement_2,
    raw_establishment.street_number_2,
    raw_establishment.street_number_suffix_2,
    raw_establishment.street_type_2,
    raw_establishment.street_label_2,
    raw_establishment.postal_code_2,
    raw_establishment.city_label_2,
    raw_establishment.foreign_city_label_2,
    raw_establishment.special_distribution_2,
    raw_establishment.commune_code_2,
    raw_establishment.cedex_code_2,
    raw_establishment.cedex_label_2,
    raw_establishment.foreign_country_code_2,
    raw_establishment.foreign_country_label_2
  FROM france_source.establishments source_establishment
  JOIN france_workflow.raw_establishments raw_establishment
    ON raw_establishment.id = source_establishment.raw_establishment_id
  JOIN france_source.companies source_company
    ON source_company.id = source_establishment.company_id
   AND source_company.row_status = 'active'
  WHERE source_company.raw_legal_unit_id = ANY(sqlc.arg('raw_legal_unit_ids')::uuid[])
    AND source_establishment.row_status = 'active'
),
address_rows AS (
  SELECT
    company_id,
    establishment_id,
    raw_establishment_id,
    CASE WHEN source_is_headquarters THEN 'headquarters' ELSE 'establishment' END::text AS address_type,
    1::smallint AS address_rank,
    address_complement AS complement,
    street_number,
    street_number_suffix,
    last_street_number,
    last_street_number_suffix,
    street_type,
    street_label AS street_name,
    postal_code,
    city_label AS city,
    foreign_city_label AS foreign_city,
    special_distribution,
    commune_code,
    cedex_code,
    cedex_label,
    foreign_country_code,
    foreign_country_label AS foreign_country,
    address_identifier,
    lambert_x,
    lambert_y
  FROM base
  UNION ALL
  SELECT
    company_id,
    establishment_id,
    raw_establishment_id,
    'secondary'::text AS address_type,
    2::smallint AS address_rank,
    address_complement_2 AS complement,
    street_number_2 AS street_number,
    street_number_suffix_2 AS street_number_suffix,
    NULL::text AS last_street_number,
    NULL::text AS last_street_number_suffix,
    street_type_2 AS street_type,
    street_label_2 AS street_name,
    postal_code_2 AS postal_code,
    city_label_2 AS city,
    foreign_city_label_2 AS foreign_city,
    special_distribution_2 AS special_distribution,
    commune_code_2 AS commune_code,
    cedex_code_2 AS cedex_code,
    cedex_label_2 AS cedex_label,
    foreign_country_code_2 AS foreign_country_code,
    foreign_country_label_2 AS foreign_country,
    NULL::text AS address_identifier,
    NULL::text AS lambert_x,
    NULL::text AS lambert_y
  FROM base
  WHERE COALESCE(
    NULLIF(btrim(address_complement_2), ''),
    NULLIF(btrim(street_number_2), ''),
    NULLIF(btrim(street_label_2), ''),
    NULLIF(btrim(postal_code_2), ''),
    NULLIF(btrim(city_label_2), ''),
    NULLIF(btrim(foreign_city_label_2), ''),
    NULLIF(btrim(commune_code_2), '')
  ) IS NOT NULL
),
shaped AS (
  SELECT
    *,
    NULLIF(btrim(concat_ws(' ',
      NULLIF(btrim(complement), ''),
      NULLIF(btrim(street_number), ''),
      NULLIF(btrim(street_number_suffix), ''),
      NULLIF(btrim(street_type), ''),
      NULLIF(btrim(street_name), ''),
      NULLIF(btrim(postal_code), ''),
      NULLIF(btrim(city), ''),
      NULLIF(btrim(foreign_city), '')
    )), '') AS formatted_address
  FROM address_rows
),
upserted AS (
  INSERT INTO france_source.addresses (
    company_id,
    establishment_id,
    raw_establishment_id,
    address_type,
    address_rank,
    complement,
    street_number,
    street_number_suffix,
    last_street_number,
    last_street_number_suffix,
    street_type,
    street_name,
    postal_code,
    city,
    foreign_city,
    special_distribution,
    commune_code,
    cedex_code,
    cedex_label,
    foreign_country_code,
    foreign_country,
    address_identifier,
    formatted_address,
    lambert_x,
    lambert_y,
    raw_address_payload,
    evidence,
    metadata,
    updated_at
  )
  SELECT
    company_id,
    establishment_id,
    raw_establishment_id,
    address_type,
    address_rank,
    NULLIF(btrim(complement), ''),
    NULLIF(btrim(street_number), ''),
    NULLIF(btrim(street_number_suffix), ''),
    NULLIF(btrim(last_street_number), ''),
    NULLIF(btrim(last_street_number_suffix), ''),
    NULLIF(btrim(street_type), ''),
    NULLIF(btrim(street_name), ''),
    NULLIF(btrim(postal_code), ''),
    NULLIF(btrim(city), ''),
    NULLIF(btrim(foreign_city), ''),
    NULLIF(btrim(special_distribution), ''),
    NULLIF(btrim(commune_code), ''),
    NULLIF(btrim(cedex_code), ''),
    NULLIF(btrim(cedex_label), ''),
    NULLIF(btrim(foreign_country_code), ''),
    NULLIF(btrim(foreign_country), ''),
    NULLIF(btrim(address_identifier), ''),
    formatted_address,
    NULLIF(btrim(lambert_x), ''),
    NULLIF(btrim(lambert_y), ''),
    jsonb_strip_nulls(jsonb_build_object(
      'address_rank', address_rank,
      'complement', complement,
      'street_number', street_number,
      'street_number_suffix', street_number_suffix,
      'last_street_number', last_street_number,
      'last_street_number_suffix', last_street_number_suffix,
      'street_type', street_type,
      'street_name', street_name,
      'postal_code', postal_code,
      'city', city,
      'foreign_city', foreign_city,
      'special_distribution', special_distribution,
      'commune_code', commune_code,
      'cedex_code', cedex_code,
      'cedex_label', cedex_label,
      'foreign_country_code', foreign_country_code,
      'foreign_country', foreign_country,
      'address_identifier', address_identifier,
      'lambert_x', lambert_x,
      'lambert_y', lambert_y
    )),
    jsonb_build_object(
      'source_table', 'france_workflow.raw_establishments',
      'raw_establishment_id', raw_establishment_id
    ),
    jsonb_build_object(
      'source', 'france',
      'trigger', sqlc.arg('trigger')::text,
      'normalized_at', now()
    ),
    now()
  FROM shaped
  ON CONFLICT (establishment_id, address_rank)
  DO UPDATE SET
    company_id = EXCLUDED.company_id,
    raw_establishment_id = EXCLUDED.raw_establishment_id,
    address_type = EXCLUDED.address_type,
    complement = EXCLUDED.complement,
    street_number = EXCLUDED.street_number,
    street_number_suffix = EXCLUDED.street_number_suffix,
    last_street_number = EXCLUDED.last_street_number,
    last_street_number_suffix = EXCLUDED.last_street_number_suffix,
    street_type = EXCLUDED.street_type,
    street_name = EXCLUDED.street_name,
    postal_code = EXCLUDED.postal_code,
    city = EXCLUDED.city,
    foreign_city = EXCLUDED.foreign_city,
    special_distribution = EXCLUDED.special_distribution,
    commune_code = EXCLUDED.commune_code,
    cedex_code = EXCLUDED.cedex_code,
    cedex_label = EXCLUDED.cedex_label,
    foreign_country_code = EXCLUDED.foreign_country_code,
    foreign_country = EXCLUDED.foreign_country,
    address_identifier = EXCLUDED.address_identifier,
    formatted_address = EXCLUDED.formatted_address,
    lambert_x = EXCLUDED.lambert_x,
    lambert_y = EXCLUDED.lambert_y,
    raw_address_payload = EXCLUDED.raw_address_payload,
    evidence = EXCLUDED.evidence,
    metadata = france_source.addresses.metadata || EXCLUDED.metadata,
    updated_at = now()
  RETURNING id
)
SELECT count(*)::integer AS rows_upserted
FROM upserted;

-- name: DeleteFranceSourceIndustriesForLegalUnits :exec
DELETE FROM france_source.industries industry
USING france_source.companies source_company
WHERE source_company.id = industry.company_id
  AND source_company.row_status = 'active'
  AND source_company.raw_legal_unit_id = ANY(sqlc.arg('raw_legal_unit_ids')::uuid[])
  AND industry.source_field IN (
    'raw_legal_units.primary_activity_code',
    'raw_establishments.primary_activity_code'
  );

-- name: InsertFranceSourceIndustries :one
WITH company_industries AS (
  SELECT
    source_company.id AS company_id,
    NULL::uuid AS establishment_id,
    source_company.raw_legal_unit_id,
    NULL::uuid AS raw_establishment_id,
    'raw_legal_units.primary_activity_code'::text AS source_field,
    1::smallint AS position,
    source_company.primary_activity_code AS source_code,
    source_company.primary_activity_nomenclature AS source_nomenclature,
    source_company.primary_activity_naf25_code AS naf25_code,
    true AS is_primary,
    jsonb_strip_nulls(jsonb_build_object(
      'source_code', source_company.primary_activity_code,
      'source_nomenclature', source_company.primary_activity_nomenclature,
      'naf25_code', source_company.primary_activity_naf25_code
    )) AS raw_industry_payload
  FROM france_source.companies source_company
  WHERE source_company.raw_legal_unit_id = ANY(sqlc.arg('raw_legal_unit_ids')::uuid[])
    AND source_company.row_status = 'active'
    AND NULLIF(btrim(source_company.primary_activity_code), '') IS NOT NULL
),
establishment_industries AS (
  SELECT
    source_establishment.company_id,
    source_establishment.id AS establishment_id,
    NULL::uuid AS raw_legal_unit_id,
    source_establishment.raw_establishment_id,
    'raw_establishments.primary_activity_code'::text AS source_field,
    1::smallint AS position,
    source_establishment.primary_activity_code AS source_code,
    source_establishment.primary_activity_nomenclature AS source_nomenclature,
    source_establishment.primary_activity_naf25_code AS naf25_code,
    source_establishment.is_headquarters AS is_primary,
    jsonb_strip_nulls(jsonb_build_object(
      'source_code', source_establishment.primary_activity_code,
      'source_nomenclature', source_establishment.primary_activity_nomenclature,
      'naf25_code', source_establishment.primary_activity_naf25_code,
      'siret', source_establishment.siret
    )) AS raw_industry_payload
  FROM france_source.establishments source_establishment
  JOIN france_source.companies source_company
    ON source_company.id = source_establishment.company_id
   AND source_company.row_status = 'active'
  WHERE source_company.raw_legal_unit_id = ANY(sqlc.arg('raw_legal_unit_ids')::uuid[])
    AND source_establishment.row_status = 'active'
    AND NULLIF(btrim(source_establishment.primary_activity_code), '') IS NOT NULL
),
inserted AS (
  INSERT INTO france_source.industries (
    company_id,
    establishment_id,
    raw_legal_unit_id,
    raw_establishment_id,
    classification_type,
    source_field,
    position,
    source_code,
    source_nomenclature,
    naf25_code,
    is_primary,
    raw_industry_payload,
    evidence,
    metadata,
    updated_at
  )
  SELECT
    industry_rows.company_id,
    industry_rows.establishment_id,
    industry_rows.raw_legal_unit_id,
    industry_rows.raw_establishment_id,
    'industry',
    industry_rows.source_field,
    industry_rows.position,
    industry_rows.source_code,
    industry_rows.source_nomenclature,
    industry_rows.naf25_code,
    industry_rows.is_primary,
    industry_rows.raw_industry_payload,
    jsonb_strip_nulls(jsonb_build_object(
      'source_table', CASE
        WHEN industry_rows.raw_establishment_id IS NULL THEN 'france_workflow.raw_legal_units'
        ELSE 'france_workflow.raw_establishments'
      END,
      'raw_legal_unit_id', industry_rows.raw_legal_unit_id,
      'raw_establishment_id', industry_rows.raw_establishment_id
    )),
    jsonb_build_object(
      'source', 'france',
      'trigger', sqlc.arg('trigger')::text,
      'normalized_at', now()
    ),
    now()
  FROM (
    SELECT * FROM company_industries
    UNION ALL
    SELECT * FROM establishment_industries
  ) industry_rows
  RETURNING id
)
SELECT count(*)::integer AS rows_inserted
FROM inserted;
