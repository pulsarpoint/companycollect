-- name: InsertSuggestion :one
INSERT INTO suggestions (
    target_company_id,
    source_id,
    source_type,
    source_input_table,
    source_input_id,
    source_native_id,
    source_payload_hash,
    source_pull_run_id,
    confidence
)
VALUES (
    sqlc.narg('target_company_id')::uuid,
    sqlc.arg('source_id')::uuid,
    sqlc.arg('source_type')::text,
    sqlc.arg('source_input_table')::text,
    sqlc.arg('source_input_id')::text,
    sqlc.narg('source_native_id')::text,
    sqlc.narg('source_payload_hash')::text,
    sqlc.narg('source_pull_run_id')::uuid,
    sqlc.narg('confidence')::real
)
ON CONFLICT (source_input_table, source_input_id, source_payload_hash)
DO UPDATE SET
    target_company_id = COALESCE(EXCLUDED.target_company_id, suggestions.target_company_id),
    confidence = EXCLUDED.confidence,
    updated_at = now()
RETURNING *;

-- name: GetSuggestionByID :one
SELECT * FROM suggestions WHERE id = $1;

-- name: ListCompanySuggestionReviews :many
WITH child_counts AS (
    SELECT
        suggestion_id,
        COUNT(*)::int AS total_count,
        COUNT(*) FILTER (WHERE status = 'pending')::int AS pending_count,
        COUNT(*) FILTER (WHERE status = 'applied')::int AS applied_count,
        COUNT(*) FILTER (WHERE status = 'rejected')::int AS rejected_count
    FROM (
        SELECT suggestion_id, status FROM suggestion_company_profiles
        UNION ALL SELECT suggestion_id, status FROM suggestion_company_domains
        UNION ALL SELECT suggestion_id, status FROM suggestion_company_locations
        UNION ALL SELECT suggestion_id, status FROM suggestion_company_emails
        UNION ALL SELECT suggestion_id, status FROM suggestion_company_phones
        UNION ALL SELECT suggestion_id, status FROM suggestion_company_financials
        UNION ALL SELECT suggestion_id, status FROM suggestion_company_industries
        UNION ALL SELECT suggestion_id, status FROM suggestion_company_markets
        UNION ALL SELECT suggestion_id, status FROM suggestion_company_services
        UNION ALL SELECT suggestion_id, status FROM suggestion_company_relationships
    ) children
    GROUP BY suggestion_id
),
profile_summary AS (
    SELECT DISTINCT ON (suggestion_id)
        suggestion_id,
        COALESCE(name, display_name, registration_number, lei) AS proposed_name,
        registration_number,
        lei,
        country_id
    FROM suggestion_company_profiles
    ORDER BY suggestion_id, created_at DESC
)
SELECT
    s.id,
    s.target_company_id,
    s.created_company_id,
    s.source_id,
    s.source_type,
    s.source_input_table,
    s.source_input_id,
    s.source_native_id,
    s.source_payload_hash,
    s.confidence,
    s.status,
    s.created_at,
    COALESCE(tc.name, cc.name, '') AS company_name,
    ps.proposed_name,
    ps.registration_number,
    ps.lei,
    ps.country_id,
    COALESCE(child_counts.total_count, 0)::int AS total_count,
    COALESCE(child_counts.pending_count, 0)::int AS pending_count,
    COALESCE(child_counts.applied_count, 0)::int AS applied_count,
    COALESCE(child_counts.rejected_count, 0)::int AS rejected_count
FROM suggestions s
LEFT JOIN companies tc ON tc.id = s.target_company_id
LEFT JOIN companies cc ON cc.id = s.created_company_id
LEFT JOIN child_counts ON child_counts.suggestion_id = s.id
LEFT JOIN profile_summary ps ON ps.suggestion_id = s.id
WHERE (sqlc.narg('status')::text IS NULL OR s.status = sqlc.narg('status')::text)
  AND (sqlc.narg('source_type')::text IS NULL OR s.source_type = sqlc.narg('source_type')::text)
  AND (
    sqlc.narg('q')::text IS NULL
    OR COALESCE(tc.name, cc.name, ps.proposed_name, '') ILIKE '%' || sqlc.narg('q')::text || '%'
    OR COALESCE(s.source_native_id, '') ILIKE '%' || sqlc.narg('q')::text || '%'
  )
ORDER BY s.created_at DESC
LIMIT sqlc.arg('limit')::int OFFSET sqlc.arg('offset')::int;

-- name: CountCompanySuggestionReviews :one
WITH profile_summary AS (
    SELECT DISTINCT ON (suggestion_id)
        suggestion_id,
        COALESCE(name, display_name, registration_number, lei) AS proposed_name
    FROM suggestion_company_profiles
    ORDER BY suggestion_id, created_at DESC
)
SELECT COUNT(*)::int
FROM suggestions s
LEFT JOIN companies tc ON tc.id = s.target_company_id
LEFT JOIN companies cc ON cc.id = s.created_company_id
LEFT JOIN profile_summary ps ON ps.suggestion_id = s.id
WHERE (sqlc.narg('status')::text IS NULL OR s.status = sqlc.narg('status')::text)
  AND (sqlc.narg('source_type')::text IS NULL OR s.source_type = sqlc.narg('source_type')::text)
  AND (
    sqlc.narg('q')::text IS NULL
    OR COALESCE(tc.name, cc.name, ps.proposed_name, '') ILIKE '%' || sqlc.narg('q')::text || '%'
    OR COALESCE(s.source_native_id, '') ILIKE '%' || sqlc.narg('q')::text || '%'
  );

-- name: ListCompanySuggestionReviewIDs :many
SELECT id
FROM suggestions
WHERE status IN ('pending', 'partially_applied')
ORDER BY created_at DESC;

-- name: ListPendingCompanySuggestionReviewItems :many
SELECT section_table, id
FROM (
    SELECT 1 AS section_order, 'suggestion_company_profiles'::text AS section_table, scp.id, scp.created_at
    FROM suggestion_company_profiles scp
    WHERE scp.suggestion_id = sqlc.arg('suggestion_id')::uuid AND scp.status = 'pending'
    UNION ALL
    SELECT 2, 'suggestion_company_domains'::text, scd.id, scd.created_at
    FROM suggestion_company_domains scd
    WHERE scd.suggestion_id = sqlc.arg('suggestion_id')::uuid AND scd.status = 'pending'
    UNION ALL
    SELECT 3, 'suggestion_company_locations'::text, scl.id, scl.created_at
    FROM suggestion_company_locations scl
    WHERE scl.suggestion_id = sqlc.arg('suggestion_id')::uuid AND scl.status = 'pending'
    UNION ALL
    SELECT 4, 'suggestion_company_emails'::text, sce.id, sce.created_at
    FROM suggestion_company_emails sce
    WHERE sce.suggestion_id = sqlc.arg('suggestion_id')::uuid AND sce.status = 'pending'
    UNION ALL
    SELECT 5, 'suggestion_company_phones'::text, scp2.id, scp2.created_at
    FROM suggestion_company_phones scp2
    WHERE scp2.suggestion_id = sqlc.arg('suggestion_id')::uuid AND scp2.status = 'pending'
    UNION ALL
    SELECT 6, 'suggestion_company_financials'::text, scf.id, scf.created_at
    FROM suggestion_company_financials scf
    WHERE scf.suggestion_id = sqlc.arg('suggestion_id')::uuid AND scf.status = 'pending'
    UNION ALL
    SELECT 7, 'suggestion_company_industries'::text, sci.id, sci.created_at
    FROM suggestion_company_industries sci
    WHERE sci.suggestion_id = sqlc.arg('suggestion_id')::uuid AND sci.status = 'pending'
    UNION ALL
    SELECT 8, 'suggestion_company_markets'::text, scm.id, scm.created_at
    FROM suggestion_company_markets scm
    WHERE scm.suggestion_id = sqlc.arg('suggestion_id')::uuid AND scm.status = 'pending'
    UNION ALL
    SELECT 9, 'suggestion_company_services'::text, scs.id, scs.created_at
    FROM suggestion_company_services scs
    WHERE scs.suggestion_id = sqlc.arg('suggestion_id')::uuid AND scs.status = 'pending'
    UNION ALL
    SELECT 10, 'suggestion_company_relationships'::text, scr.id, scr.created_at
    FROM suggestion_company_relationships scr
    WHERE scr.suggestion_id = sqlc.arg('suggestion_id')::uuid AND scr.status = 'pending'
) pending_items
ORDER BY section_order, created_at, id;

-- name: GetSuggestionCompanyProfileByID :one
SELECT * FROM suggestion_company_profiles WHERE id = $1;

-- name: MarkSuggestionCompanyProfileApplied :exec
UPDATE suggestion_company_profiles
SET status = 'applied',
    reviewed_by = $2,
    reviewed_at = now(),
    review_note = $3,
    updated_at = now()
WHERE id = $1 AND status = 'pending';

-- name: MarkSuggestionCompanyProfileRejected :exec
UPDATE suggestion_company_profiles
SET status = 'rejected',
    reviewed_by = $2,
    reviewed_at = now(),
    review_note = $3,
    updated_at = now()
WHERE id = $1 AND status = 'pending';

-- name: GetSuggestionCompanyDomainByID :one
SELECT * FROM suggestion_company_domains WHERE id = $1;

-- name: MarkSuggestionCompanyDomainApplied :exec
UPDATE suggestion_company_domains SET status = 'applied', reviewed_by = $2, reviewed_at = now(), review_note = $3, updated_at = now()
WHERE id = $1 AND status = 'pending';

-- name: MarkSuggestionCompanyDomainRejected :exec
UPDATE suggestion_company_domains SET status = 'rejected', reviewed_by = $2, reviewed_at = now(), review_note = $3, updated_at = now()
WHERE id = $1 AND status = 'pending';

-- name: GetSuggestionCompanyEmailByID :one
SELECT * FROM suggestion_company_emails WHERE id = $1;

-- name: MarkSuggestionCompanyEmailApplied :exec
UPDATE suggestion_company_emails SET status = 'applied', reviewed_by = $2, reviewed_at = now(), review_note = $3, updated_at = now()
WHERE id = $1 AND status = 'pending';

-- name: MarkSuggestionCompanyEmailRejected :exec
UPDATE suggestion_company_emails SET status = 'rejected', reviewed_by = $2, reviewed_at = now(), review_note = $3, updated_at = now()
WHERE id = $1 AND status = 'pending';

-- name: GetSuggestionCompanyPhoneByID :one
SELECT * FROM suggestion_company_phones WHERE id = $1;

-- name: MarkSuggestionCompanyPhoneApplied :exec
UPDATE suggestion_company_phones SET status = 'applied', reviewed_by = $2, reviewed_at = now(), review_note = $3, updated_at = now()
WHERE id = $1 AND status = 'pending';

-- name: MarkSuggestionCompanyPhoneRejected :exec
UPDATE suggestion_company_phones SET status = 'rejected', reviewed_by = $2, reviewed_at = now(), review_note = $3, updated_at = now()
WHERE id = $1 AND status = 'pending';

-- name: GetSuggestionCompanyLocationByID :one
SELECT * FROM suggestion_company_locations WHERE id = $1;

-- name: MarkSuggestionCompanyLocationApplied :exec
UPDATE suggestion_company_locations SET status = 'applied', reviewed_by = $2, reviewed_at = now(), review_note = $3, updated_at = now()
WHERE id = $1 AND status = 'pending';

-- name: MarkSuggestionCompanyLocationRejected :exec
UPDATE suggestion_company_locations SET status = 'rejected', reviewed_by = $2, reviewed_at = now(), review_note = $3, updated_at = now()
WHERE id = $1 AND status = 'pending';

-- name: InsertSuggestionCompanyFinancial :one
INSERT INTO suggestion_company_financials (
    suggestion_id, target_row_id, operation, confidence,
    year, source_name, employee_count, revenue_amount, revenue_currency, revenue_usd,
    profit_amount, profit_usd, evidence
)
VALUES (
    sqlc.arg('suggestion_id')::uuid,
    sqlc.narg('target_row_id')::uuid,
    sqlc.arg('operation')::text,
    sqlc.narg('confidence')::real,
    sqlc.arg('year')::int,
    sqlc.arg('source_name')::text,
    sqlc.narg('employee_count')::int,
    sqlc.narg('revenue_amount')::bigint,
    sqlc.narg('revenue_currency')::text,
    sqlc.narg('revenue_usd')::bigint,
    sqlc.narg('profit_amount')::bigint,
    sqlc.narg('profit_usd')::bigint,
    COALESCE(sqlc.narg('evidence')::jsonb, '{}'::jsonb)
)
RETURNING *;

-- name: GetSuggestionCompanyFinancialByID :one
SELECT * FROM suggestion_company_financials WHERE id = $1;

-- name: MarkSuggestionCompanyFinancialApplied :exec
UPDATE suggestion_company_financials SET status = 'applied', reviewed_by = $2, reviewed_at = now(), review_note = $3, updated_at = now()
WHERE id = $1 AND status = 'pending';

-- name: MarkSuggestionCompanyFinancialRejected :exec
UPDATE suggestion_company_financials SET status = 'rejected', reviewed_by = $2, reviewed_at = now(), review_note = $3, updated_at = now()
WHERE id = $1 AND status = 'pending';

-- name: GetSuggestionCompanyIndustryByID :one
SELECT * FROM suggestion_company_industries WHERE id = $1;

-- name: MarkSuggestionCompanyIndustryApplied :exec
UPDATE suggestion_company_industries SET status = 'applied', reviewed_by = $2, reviewed_at = now(), review_note = $3, updated_at = now()
WHERE id = $1 AND status = 'pending';

-- name: MarkSuggestionCompanyIndustryRejected :exec
UPDATE suggestion_company_industries SET status = 'rejected', reviewed_by = $2, reviewed_at = now(), review_note = $3, updated_at = now()
WHERE id = $1 AND status = 'pending';

-- name: GetSuggestionCompanyMarketByID :one
SELECT * FROM suggestion_company_markets WHERE id = $1;

-- name: MarkSuggestionCompanyMarketApplied :exec
UPDATE suggestion_company_markets SET status = 'applied', reviewed_by = $2, reviewed_at = now(), review_note = $3, updated_at = now()
WHERE id = $1 AND status = 'pending';

-- name: MarkSuggestionCompanyMarketRejected :exec
UPDATE suggestion_company_markets SET status = 'rejected', reviewed_by = $2, reviewed_at = now(), review_note = $3, updated_at = now()
WHERE id = $1 AND status = 'pending';

-- name: GetSuggestionCompanyServiceByID :one
SELECT * FROM suggestion_company_services WHERE id = $1;

-- name: MarkSuggestionCompanyServiceApplied :exec
UPDATE suggestion_company_services SET status = 'applied', reviewed_by = $2, reviewed_at = now(), review_note = $3, updated_at = now()
WHERE id = $1 AND status = 'pending';

-- name: MarkSuggestionCompanyServiceRejected :exec
UPDATE suggestion_company_services SET status = 'rejected', reviewed_by = $2, reviewed_at = now(), review_note = $3, updated_at = now()
WHERE id = $1 AND status = 'pending';

-- name: GetSuggestionCompanyRelationshipByID :one
SELECT * FROM suggestion_company_relationships WHERE id = $1;

-- name: MarkSuggestionCompanyRelationshipApplied :exec
UPDATE suggestion_company_relationships SET status = 'applied', reviewed_by = $2, reviewed_at = now(), review_note = $3, updated_at = now()
WHERE id = $1 AND status = 'pending';

-- name: MarkSuggestionCompanyRelationshipRejected :exec
UPDATE suggestion_company_relationships SET status = 'rejected', reviewed_by = $2, reviewed_at = now(), review_note = $3, updated_at = now()
WHERE id = $1 AND status = 'pending';

-- name: UpdateSuggestionCreatedCompany :exec
UPDATE suggestions
SET created_company_id = $2,
    updated_at = now()
WHERE id = $1;

-- name: CountSuggestionReviewItemStatuses :one
SELECT
    COUNT(*) FILTER (WHERE status = 'pending')::bigint AS pending_count,
    COUNT(*) FILTER (WHERE status = 'applied')::bigint AS applied_count,
    COUNT(*) FILTER (WHERE status = 'rejected')::bigint AS rejected_count
FROM (
    SELECT scp.status FROM suggestion_company_profiles scp WHERE scp.suggestion_id = sqlc.arg('suggestion_id')::uuid
    UNION ALL SELECT scd.status FROM suggestion_company_domains scd WHERE scd.suggestion_id = sqlc.arg('suggestion_id')::uuid
    UNION ALL SELECT scl.status FROM suggestion_company_locations scl WHERE scl.suggestion_id = sqlc.arg('suggestion_id')::uuid
    UNION ALL SELECT sce.status FROM suggestion_company_emails sce WHERE sce.suggestion_id = sqlc.arg('suggestion_id')::uuid
    UNION ALL SELECT scp2.status FROM suggestion_company_phones scp2 WHERE scp2.suggestion_id = sqlc.arg('suggestion_id')::uuid
    UNION ALL SELECT scf.status FROM suggestion_company_financials scf WHERE scf.suggestion_id = sqlc.arg('suggestion_id')::uuid
    UNION ALL SELECT sci.status FROM suggestion_company_industries sci WHERE sci.suggestion_id = sqlc.arg('suggestion_id')::uuid
    UNION ALL SELECT scm.status FROM suggestion_company_markets scm WHERE scm.suggestion_id = sqlc.arg('suggestion_id')::uuid
    UNION ALL SELECT scs.status FROM suggestion_company_services scs WHERE scs.suggestion_id = sqlc.arg('suggestion_id')::uuid
    UNION ALL SELECT scr.status FROM suggestion_company_relationships scr WHERE scr.suggestion_id = sqlc.arg('suggestion_id')::uuid
) children;

-- name: UpdateSuggestionAggregateStatus :exec
UPDATE suggestions
SET status = $2,
    reviewed_by = $3,
    reviewed_at = CASE WHEN $2 IN ('applied', 'rejected') THEN now() ELSE reviewed_at END,
    review_note = $4,
    updated_at = now()
WHERE id = $1;
