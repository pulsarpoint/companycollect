# NBRP Bulk Database (Paid MOU) Field Catalog

> **PLANNING-ONLY / PAID.** The national business-registration database in bulk,
> available via a **fee-based MOU** with the Business Registration Support Centre.
> Cataloged from public descriptions; no data retrieved.

## Source Summary

- Country: Vietnam
- Source type: official_registry
- Organization: Business Registration Support Centre, MPI
- URL: https://dangkykinhdoanh.gov.vn/
- License: paid / contract (MOU)
- Access: paid
- Freshness: periodic
- Record shape: planning-only
- Primary keys: `ma_so_doanh_nghiep`
- Join keys: `ma_so_doanh_nghiep`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| company_master | company master | Full register master | object | identifier | planning-only; whole population |
| changes_feed | registration changes | New/amended registrations | array | filing | planning-only |

## Interpretation Notes

- The **only route to full-coverage** Vietnamese company data — a **paid MOU** for
  the bulk register (same fields as the per-company search). Keep **planning-only**
  until a contract is in place; redact legal-representative PII. Join on the
  enterprise code (= tax code).
