-- name: GetBrregSourceTranslationAssetState :one
SELECT
  'mv_company_translation_status'::text AS asset,
  count(*)::bigint AS raw_records_current,
  COALESCE(sum(translation_missing_count), 0)::bigint AS task_no_state,
  COALESCE(sum(translation_pending_count), 0)::bigint AS task_pending,
  COALESCE(sum(translation_running_count), 0)::bigint AS task_running_active,
  0::bigint AS task_running_stale,
  COALESCE(sum(translation_failed_count), 0)::bigint AS task_failed_retryable,
  0::bigint AS task_failed_terminal,
  COALESCE(sum(translation_succeeded_count), 0)::bigint AS task_succeeded,
  0::bigint AS task_skipped,
  COALESCE(sum(GREATEST(translation_missing_count - translation_cached_count, 0)), 0)::bigint AS task_eligible_now,
  COALESCE(sum(translation_succeeded_count), 0)::bigint AS artifact_succeeded,
  0::bigint AS artifact_skipped,
  COALESCE(sum(translation_failed_count), 0)::bigint AS artifact_failed,
  COALESCE(sum(translation_missing_count), 0)::bigint AS artifact_missing
FROM brreg_source.mv_company_translation_status;

-- name: GetBrregSourceDomainAssetState :one
WITH active_companies AS (
  SELECT id
  FROM brreg_source.companies
  WHERE row_status = 'active'
),
domain_companies AS (
  SELECT DISTINCT domain.company_id
  FROM brreg_source.domains domain
  JOIN active_companies company ON company.id = domain.company_id
  WHERE domain.status = 'active'
)
SELECT
  'brreg_source.domains'::text AS asset,
  count(*)::bigint AS raw_records_current,
  count(*) FILTER (WHERE domain_companies.company_id IS NULL)::bigint AS task_no_state,
  0::bigint AS task_pending,
  0::bigint AS task_running_active,
  0::bigint AS task_running_stale,
  0::bigint AS task_failed_retryable,
  0::bigint AS task_failed_terminal,
  count(*) FILTER (WHERE domain_companies.company_id IS NOT NULL)::bigint AS task_succeeded,
  0::bigint AS task_skipped,
  count(*) FILTER (WHERE domain_companies.company_id IS NULL)::bigint AS task_eligible_now,
  count(*) FILTER (WHERE domain_companies.company_id IS NOT NULL)::bigint AS artifact_succeeded,
  0::bigint AS artifact_skipped,
  0::bigint AS artifact_failed,
  count(*) FILTER (WHERE domain_companies.company_id IS NULL)::bigint AS artifact_missing
FROM active_companies company
LEFT JOIN domain_companies ON domain_companies.company_id = company.id;

-- name: GetBrregSourceFinancialAssetState :one
WITH active_companies AS (
  SELECT id
  FROM brreg_source.companies
  WHERE row_status = 'active'
),
statement_companies AS (
  SELECT DISTINCT statement.company_id
  FROM brreg_source.financial_statements statement
  JOIN active_companies company ON company.id = statement.company_id
),
states AS (
  SELECT
    company.id AS company_id,
    process_status.financial_status,
    process_status.financial_lease_until,
    statement_companies.company_id IS NOT NULL AS has_statement
  FROM active_companies company
  LEFT JOIN brreg_source.company_process_status process_status ON process_status.company_id = company.id
  LEFT JOIN statement_companies ON statement_companies.company_id = company.id
)
SELECT
  'brreg_source.financial_statements'::text AS asset,
  count(*)::bigint AS raw_records_current,
  count(*) FILTER (WHERE financial_status IS NULL)::bigint AS task_no_state,
  count(*) FILTER (WHERE financial_status IN ('pending', 'dirty'))::bigint AS task_pending,
  count(*) FILTER (
    WHERE financial_status = 'running'
      AND coalesce(financial_lease_until, '-infinity'::timestamptz) > now()
  )::bigint AS task_running_active,
  count(*) FILTER (
    WHERE financial_status = 'running'
      AND coalesce(financial_lease_until, '-infinity'::timestamptz) <= now()
  )::bigint AS task_running_stale,
  count(*) FILTER (WHERE financial_status = 'failed_retryable')::bigint AS task_failed_retryable,
  count(*) FILTER (WHERE financial_status = 'failed_terminal')::bigint AS task_failed_terminal,
  count(*) FILTER (WHERE financial_status = 'succeeded')::bigint AS task_succeeded,
  count(*) FILTER (WHERE financial_status = 'skipped')::bigint AS task_skipped,
  count(*) FILTER (
    WHERE NOT has_statement
      AND (
        financial_status IS NULL
        OR financial_status IN ('pending', 'dirty', 'failed_retryable')
        OR (
          financial_status = 'running'
          AND coalesce(financial_lease_until, '-infinity'::timestamptz) <= now()
        )
      )
  )::bigint AS task_eligible_now,
  count(*) FILTER (WHERE has_statement)::bigint AS artifact_succeeded,
  count(*) FILTER (WHERE financial_status = 'skipped' AND NOT has_statement)::bigint AS artifact_skipped,
  count(*) FILTER (WHERE financial_status IN ('failed_retryable', 'failed_terminal') AND NOT has_statement)::bigint AS artifact_failed,
  count(*) FILTER (WHERE NOT has_statement AND coalesce(financial_status, '') <> 'skipped')::bigint AS artifact_missing
FROM states;

-- name: GetBrregSourceResultTableCounts :one
SELECT
  (SELECT count(*)::bigint FROM brreg_source.companies WHERE row_status = 'active') AS companies,
  (SELECT count(*)::bigint FROM brreg_source.mv_company_translation_status) AS translation_status,
  (SELECT count(*)::bigint FROM brreg_source.websites WHERE status = 'active') AS websites,
  (SELECT count(*)::bigint FROM brreg_source.domains WHERE status = 'active') AS domains,
  (SELECT count(*)::bigint FROM brreg_source.financial_statements) AS financial_statements;

-- name: CountBrregSourceEntries :one
SELECT count(*)::bigint
FROM brreg_source.mv_company_explorer entry
LEFT JOIN brreg_source.mv_company_translation_status translation_status
  ON translation_status.company_id = entry.company_id
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
      AND coalesce(translation_status.translation_missing_count, entry.translation_missing_count, 0) > 0
    )
    OR (
      sqlc.narg('translation_status')::text = 'complete'
      AND coalesce(translation_status.translation_missing_count, entry.translation_missing_count, 0) = 0
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
  AND (
    sqlc.narg('financial_status')::text IS NULL
    OR entry.financial_status = sqlc.narg('financial_status')::text
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
  entry.financial_status,
  coalesce(translation_status.translation_missing_count, entry.translation_missing_count, 0) AS translation_missing_count,
  coalesce(translation_status.translation_pending_count, entry.translation_pending_count, 0) AS translation_pending_count,
  coalesce(translation_status.translation_running_count, entry.translation_running_count, 0) AS translation_running_count,
  coalesce(translation_status.translation_succeeded_count, entry.translation_succeeded_count, 0) AS translation_succeeded_count,
  coalesce(translation_status.translation_failed_count, entry.translation_failed_count, 0) AS translation_failed_count,
  entry.domain_pending_count,
  entry.domain_running_count,
  entry.domain_succeeded_count,
  entry.updated_at
FROM brreg_source.mv_company_explorer entry
LEFT JOIN brreg_source.mv_company_translation_status translation_status
  ON translation_status.company_id = entry.company_id
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
      AND coalesce(translation_status.translation_missing_count, entry.translation_missing_count, 0) > 0
    )
    OR (
      sqlc.narg('translation_status')::text = 'complete'
      AND coalesce(translation_status.translation_missing_count, entry.translation_missing_count, 0) = 0
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
  AND (
    sqlc.narg('financial_status')::text IS NULL
    OR entry.financial_status = sqlc.narg('financial_status')::text
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
  CASE WHEN sqlc.arg('sort_by')::text = 'translation_missing' AND sqlc.arg('sort_dir')::text = 'asc' THEN coalesce(translation_status.translation_missing_count, entry.translation_missing_count, 0) END ASC NULLS LAST,
  CASE WHEN sqlc.arg('sort_by')::text = 'translation_missing' AND sqlc.arg('sort_dir')::text = 'desc' THEN coalesce(translation_status.translation_missing_count, entry.translation_missing_count, 0) END DESC NULLS LAST,
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

-- name: UpsertBrregSourceFinancialStatement :one
WITH input_values AS (
  SELECT
    sqlc.arg('company_id')::uuid AS company_id,
    sqlc.arg('raw_record_id')::uuid AS raw_record_id,
    sqlc.arg('fiscal_year')::integer AS fiscal_year,
    CASE
      WHEN NULLIF(sqlc.narg('period_start')::text, '') IS NULL THEN NULL::date
      ELSE NULLIF(sqlc.narg('period_start')::text, '')::date
    END AS period_start,
    CASE
      WHEN NULLIF(sqlc.narg('period_end')::text, '') IS NULL THEN NULL::date
      ELSE NULLIF(sqlc.narg('period_end')::text, '')::date
    END AS period_end,
    COALESCE(NULLIF(sqlc.arg('statement_type')::text, ''), 'annual_accounts') AS statement_type,
    sqlc.arg('is_consolidated')::boolean AS is_consolidated,
    NULLIF(upper(btrim(sqlc.narg('original_currency')::text)), '') AS original_currency,
    NULLIF(sqlc.narg('revenue_original_amount')::text, '')::numeric AS revenue_original_amount,
    NULLIF(sqlc.narg('operating_income_original_amount')::text, '')::numeric AS operating_income_original_amount,
    NULLIF(sqlc.narg('operating_profit_original_amount')::text, '')::numeric AS operating_profit_original_amount,
    NULLIF(sqlc.narg('profit_before_tax_original_amount')::text, '')::numeric AS profit_before_tax_original_amount,
    NULLIF(sqlc.narg('net_income_original_amount')::text, '')::numeric AS net_income_original_amount,
    NULLIF(sqlc.narg('total_assets_original_amount')::text, '')::numeric AS total_assets_original_amount,
    NULLIF(sqlc.narg('current_assets_original_amount')::text, '')::numeric AS current_assets_original_amount,
    NULLIF(sqlc.narg('fixed_assets_original_amount')::text, '')::numeric AS fixed_assets_original_amount,
    NULLIF(sqlc.narg('total_equity_original_amount')::text, '')::numeric AS total_equity_original_amount,
    NULLIF(sqlc.narg('total_liabilities_original_amount')::text, '')::numeric AS total_liabilities_original_amount,
    NULLIF(sqlc.narg('current_liabilities_original_amount')::text, '')::numeric AS current_liabilities_original_amount,
    NULLIF(sqlc.narg('long_term_liabilities_original_amount')::text, '')::numeric AS long_term_liabilities_original_amount,
    NULLIF(sqlc.narg('source_url')::text, '') AS source_url,
    COALESCE(sqlc.narg('facts')::jsonb, '{}'::jsonb) AS facts,
    COALESCE(sqlc.narg('evidence')::jsonb, '{}'::jsonb) AS evidence,
    COALESCE(sqlc.narg('raw_financial_payload')::jsonb, '{}'::jsonb) AS raw_financial_payload,
    COALESCE(sqlc.narg('metadata')::jsonb, '{}'::jsonb) AS metadata,
    COALESCE(NULLIF(sqlc.narg('trigger')::text, ''), 'manual') AS trigger
),
selected_sheet AS (
  SELECT
    sheet.id,
    sheet.provider,
    sheet.rate_date,
    sheet.base_currency
  FROM exchange_rate_sheets sheet
  WHERE sheet.provider = 'ecb'
  ORDER BY sheet.rate_date DESC
  LIMIT 1
),
rate_context AS (
  SELECT
    sheet.id AS sheet_id,
    sheet.provider,
    sheet.rate_date,
    sheet.base_currency,
    input.original_currency AS source_currency,
    usd.rate_per_base AS usd_rate_per_base,
    CASE
      WHEN input.original_currency = sheet.base_currency THEN 1::numeric
      ELSE source_rate.rate_per_base
    END AS source_rate_per_base
  FROM input_values input
  LEFT JOIN selected_sheet sheet ON true
  LEFT JOIN exchange_rates usd ON usd.sheet_id = sheet.id AND usd.currency = 'USD'
  LEFT JOIN exchange_rates source_rate ON source_rate.sheet_id = sheet.id AND source_rate.currency = input.original_currency
),
prepared AS (
  SELECT
    input.*,
    rate_context.sheet_id,
    rate_context.provider,
    rate_context.rate_date,
    rate_context.base_currency,
    rate_context.source_currency,
    rate_context.usd_rate_per_base,
    rate_context.source_rate_per_base,
    CASE
      WHEN rate_context.sheet_id IS NULL
        OR rate_context.usd_rate_per_base IS NULL
        OR rate_context.source_rate_per_base IS NULL
      THEN '{}'::jsonb
      ELSE jsonb_build_object(
        'provider', rate_context.provider,
        'sheet_id', rate_context.sheet_id::text,
        'rate_date', rate_context.rate_date,
        'base_currency', rate_context.base_currency,
        'source_currency', rate_context.source_currency,
        'target_currency', 'USD',
        'source_rate_per_base', rate_context.source_rate_per_base,
        'target_rate_per_base', rate_context.usd_rate_per_base,
        'trigger', input.trigger
      )
    END AS fx_metadata
  FROM input_values input
  LEFT JOIN rate_context ON true
),
upserted AS (
  INSERT INTO brreg_source.financial_statements (
    company_id,
    raw_record_id,
    fiscal_year,
    period_start,
    period_end,
    statement_type,
    is_consolidated,
    original_currency,
    revenue_original_amount,
    revenue_usd_cents,
    operating_income_original_amount,
    operating_income_usd_cents,
    operating_profit_original_amount,
    operating_profit_usd_cents,
    profit_before_tax_original_amount,
    profit_before_tax_usd_cents,
    net_income_original_amount,
    net_income_usd_cents,
    total_assets_original_amount,
    total_assets_usd_cents,
    current_assets_original_amount,
    current_assets_usd_cents,
    fixed_assets_original_amount,
    fixed_assets_usd_cents,
    total_equity_original_amount,
    total_equity_usd_cents,
    total_liabilities_original_amount,
    total_liabilities_usd_cents,
    current_liabilities_original_amount,
    current_liabilities_usd_cents,
    long_term_liabilities_original_amount,
    long_term_liabilities_usd_cents,
    fx_source,
    fx_rate_date,
    fx_metadata,
    source_url,
    facts,
    evidence,
    raw_financial_payload,
    metadata,
    updated_at
  )
  SELECT
    company_id,
    raw_record_id,
    fiscal_year,
    period_start,
    period_end,
    statement_type,
    is_consolidated,
    original_currency,
    revenue_original_amount,
    CASE WHEN revenue_original_amount IS NULL OR usd_rate_per_base IS NULL OR source_rate_per_base IS NULL THEN NULL ELSE round(revenue_original_amount * usd_rate_per_base / source_rate_per_base * 100)::bigint END,
    operating_income_original_amount,
    CASE WHEN operating_income_original_amount IS NULL OR usd_rate_per_base IS NULL OR source_rate_per_base IS NULL THEN NULL ELSE round(operating_income_original_amount * usd_rate_per_base / source_rate_per_base * 100)::bigint END,
    operating_profit_original_amount,
    CASE WHEN operating_profit_original_amount IS NULL OR usd_rate_per_base IS NULL OR source_rate_per_base IS NULL THEN NULL ELSE round(operating_profit_original_amount * usd_rate_per_base / source_rate_per_base * 100)::bigint END,
    profit_before_tax_original_amount,
    CASE WHEN profit_before_tax_original_amount IS NULL OR usd_rate_per_base IS NULL OR source_rate_per_base IS NULL THEN NULL ELSE round(profit_before_tax_original_amount * usd_rate_per_base / source_rate_per_base * 100)::bigint END,
    net_income_original_amount,
    CASE WHEN net_income_original_amount IS NULL OR usd_rate_per_base IS NULL OR source_rate_per_base IS NULL THEN NULL ELSE round(net_income_original_amount * usd_rate_per_base / source_rate_per_base * 100)::bigint END,
    total_assets_original_amount,
    CASE WHEN total_assets_original_amount IS NULL OR usd_rate_per_base IS NULL OR source_rate_per_base IS NULL THEN NULL ELSE round(total_assets_original_amount * usd_rate_per_base / source_rate_per_base * 100)::bigint END,
    current_assets_original_amount,
    CASE WHEN current_assets_original_amount IS NULL OR usd_rate_per_base IS NULL OR source_rate_per_base IS NULL THEN NULL ELSE round(current_assets_original_amount * usd_rate_per_base / source_rate_per_base * 100)::bigint END,
    fixed_assets_original_amount,
    CASE WHEN fixed_assets_original_amount IS NULL OR usd_rate_per_base IS NULL OR source_rate_per_base IS NULL THEN NULL ELSE round(fixed_assets_original_amount * usd_rate_per_base / source_rate_per_base * 100)::bigint END,
    total_equity_original_amount,
    CASE WHEN total_equity_original_amount IS NULL OR usd_rate_per_base IS NULL OR source_rate_per_base IS NULL THEN NULL ELSE round(total_equity_original_amount * usd_rate_per_base / source_rate_per_base * 100)::bigint END,
    total_liabilities_original_amount,
    CASE WHEN total_liabilities_original_amount IS NULL OR usd_rate_per_base IS NULL OR source_rate_per_base IS NULL THEN NULL ELSE round(total_liabilities_original_amount * usd_rate_per_base / source_rate_per_base * 100)::bigint END,
    current_liabilities_original_amount,
    CASE WHEN current_liabilities_original_amount IS NULL OR usd_rate_per_base IS NULL OR source_rate_per_base IS NULL THEN NULL ELSE round(current_liabilities_original_amount * usd_rate_per_base / source_rate_per_base * 100)::bigint END,
    long_term_liabilities_original_amount,
    CASE WHEN long_term_liabilities_original_amount IS NULL OR usd_rate_per_base IS NULL OR source_rate_per_base IS NULL THEN NULL ELSE round(long_term_liabilities_original_amount * usd_rate_per_base / source_rate_per_base * 100)::bigint END,
    provider,
    rate_date,
    fx_metadata,
    source_url,
    facts,
    evidence,
    raw_financial_payload,
    metadata,
    now()
  FROM prepared
  ON CONFLICT (company_id, fiscal_year, statement_type, is_consolidated)
  DO UPDATE SET
    raw_record_id = EXCLUDED.raw_record_id,
    period_start = EXCLUDED.period_start,
    period_end = EXCLUDED.period_end,
    original_currency = EXCLUDED.original_currency,
    revenue_original_amount = EXCLUDED.revenue_original_amount,
    revenue_usd_cents = EXCLUDED.revenue_usd_cents,
    operating_income_original_amount = EXCLUDED.operating_income_original_amount,
    operating_income_usd_cents = EXCLUDED.operating_income_usd_cents,
    operating_profit_original_amount = EXCLUDED.operating_profit_original_amount,
    operating_profit_usd_cents = EXCLUDED.operating_profit_usd_cents,
    profit_before_tax_original_amount = EXCLUDED.profit_before_tax_original_amount,
    profit_before_tax_usd_cents = EXCLUDED.profit_before_tax_usd_cents,
    net_income_original_amount = EXCLUDED.net_income_original_amount,
    net_income_usd_cents = EXCLUDED.net_income_usd_cents,
    total_assets_original_amount = EXCLUDED.total_assets_original_amount,
    total_assets_usd_cents = EXCLUDED.total_assets_usd_cents,
    current_assets_original_amount = EXCLUDED.current_assets_original_amount,
    current_assets_usd_cents = EXCLUDED.current_assets_usd_cents,
    fixed_assets_original_amount = EXCLUDED.fixed_assets_original_amount,
    fixed_assets_usd_cents = EXCLUDED.fixed_assets_usd_cents,
    total_equity_original_amount = EXCLUDED.total_equity_original_amount,
    total_equity_usd_cents = EXCLUDED.total_equity_usd_cents,
    total_liabilities_original_amount = EXCLUDED.total_liabilities_original_amount,
    total_liabilities_usd_cents = EXCLUDED.total_liabilities_usd_cents,
    current_liabilities_original_amount = EXCLUDED.current_liabilities_original_amount,
    current_liabilities_usd_cents = EXCLUDED.current_liabilities_usd_cents,
    long_term_liabilities_original_amount = EXCLUDED.long_term_liabilities_original_amount,
    long_term_liabilities_usd_cents = EXCLUDED.long_term_liabilities_usd_cents,
    fx_source = EXCLUDED.fx_source,
    fx_rate_date = EXCLUDED.fx_rate_date,
    fx_metadata = EXCLUDED.fx_metadata,
    source_url = EXCLUDED.source_url,
    facts = EXCLUDED.facts,
    evidence = EXCLUDED.evidence,
    raw_financial_payload = EXCLUDED.raw_financial_payload,
    metadata = brreg_source.financial_statements.metadata || EXCLUDED.metadata,
    updated_at = now()
  RETURNING id
)
SELECT id FROM upserted;

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
