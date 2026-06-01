-- name: CreateCompanyFinancial :one
INSERT INTO company_financials (
    company_id, year, source_name,
    employee_count, revenue_amount, revenue_currency, revenue_usd,
    profit_amount, profit_usd, evidence
) VALUES (
    sqlc.arg('company_id')::uuid,
    sqlc.arg('year')::int,
    sqlc.arg('source_name')::text,
    sqlc.narg('employee_count')::int,
    sqlc.narg('revenue_amount')::bigint,
    sqlc.narg('revenue_currency')::text,
    sqlc.narg('revenue_usd')::bigint,
    sqlc.narg('profit_amount')::bigint,
    sqlc.narg('profit_usd')::bigint,
    sqlc.arg('evidence')::jsonb
)
ON CONFLICT (company_id, year, source_name)
DO UPDATE SET
    employee_count   = EXCLUDED.employee_count,
    revenue_amount   = EXCLUDED.revenue_amount,
    revenue_currency = EXCLUDED.revenue_currency,
    revenue_usd      = EXCLUDED.revenue_usd,
    profit_amount    = EXCLUDED.profit_amount,
    profit_usd       = EXCLUDED.profit_usd,
    evidence          = EXCLUDED.evidence,
    updated_at       = now()
RETURNING *;

-- name: ListCompanyFinancials :many
SELECT * FROM company_financials
WHERE company_id = $1
ORDER BY year DESC, created_at DESC;

-- name: ListPendingCompanyFinancials :many
SELECT
    cf.id,
    cf.company_id,
    cf.year,
    cf.source_name,
    cf.employee_count,
    cf.revenue_amount,
    cf.revenue_currency,
    cf.revenue_usd,
    cf.profit_amount,
    cf.profit_usd,
    cf.evidence,
    cf.status,
    cf.reviewed_by,
    cf.reviewed_at,
    cf.created_at,
    cf.updated_at,
    c.name AS company_name
FROM company_financials cf
JOIN companies c ON c.id = cf.company_id
WHERE cf.status = 'suggested'
ORDER BY cf.created_at DESC
LIMIT sqlc.arg('limit')::int OFFSET sqlc.arg('offset')::int;

-- name: CountPendingCompanyFinancials :one
SELECT COUNT(*)::int FROM company_financials WHERE status = 'suggested';

-- name: ListPendingCompanyFinancialIDs :many
SELECT id FROM company_financials WHERE status = 'suggested' ORDER BY created_at DESC;

-- name: ApproveCompanyFinancial :exec
WITH updated AS (
    UPDATE company_financials SET
        status      = 'approved',
        reviewed_by = sqlc.narg('reviewed_by')::text,
        reviewed_at = now(),
        updated_at  = now()
    WHERE id = sqlc.arg('id')::uuid AND status = 'suggested'
    RETURNING company_id, employee_count, revenue_usd
)
UPDATE companies SET
    employee_count = COALESCE(updated.employee_count, companies.employee_count),
    revenue_usd    = COALESCE(updated.revenue_usd, companies.revenue_usd),
    updated_at     = now()
FROM updated
WHERE companies.id = updated.company_id;

-- name: RejectCompanyFinancial :exec
UPDATE company_financials SET
    status      = 'rejected',
    reviewed_by = sqlc.narg('reviewed_by')::text,
    reviewed_at = now(),
    updated_at  = now()
WHERE id = sqlc.arg('id')::uuid AND status = 'suggested';

-- name: BulkUpdateCompanyFinancialStatus :exec
UPDATE company_financials SET
    status      = sqlc.arg('status')::text,
    reviewed_by = sqlc.narg('reviewed_by')::text,
    reviewed_at = now(),
    updated_at  = now()
WHERE id = ANY(sqlc.arg('ids')::uuid[]) AND status = 'suggested';
