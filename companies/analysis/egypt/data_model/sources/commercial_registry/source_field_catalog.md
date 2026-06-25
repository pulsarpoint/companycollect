# Commercial Registry (السجل التجاري) Field Catalog

## Source Summary

- Country: Egypt
- Source type: official_registry
- Organization: GOEIC / Ministry of Supply
- URL: https://www.gov.eg/
- License: restricted
- Access: **not openly searchable** online
- Freshness: live register
- Record shape: per-company commercial registration (not openly searchable)
- Primary keys: commercial_registry_number
- Join keys: commercial_registry_number, tax_id

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| commercial_registry_number | رقم السجل التجاري | Commercial registration number | string | identifier |  | same key as GAFI |
| company_name | الاسم التجاري | Trade name | string | legal_name |  | |
| activity | النشاط | Activity | string | activity |  | |
| address | العنوان | Address | string | address |  | |
| capital | رأس المال | Capital | decimal | financial |  | EGP |
| owner | صاحب المنشأة | Owner | string | person |  | PERSONAL DATA — redact |

## Interpretation Notes

- The **Commercial Registry** (السجل التجاري), under **GOEIC / the Ministry of
  Supply**, holds the **commercial registration number** for companies and traders —
  the same identifier used by GAFI. It is **not openly searchable online** (in-person
  / restricted portal) and has **no open bulk/API**.
- The field model is documented from **public knowledge**; **no real values copied**.
- **Join**: keyed on the **Commercial Registry number** (and Tax ID) — the shared key
  with GAFI. For traders/sole proprietors the **owner** name is personal data (PDP Law
  151/2020) — redact.
- Implementation is **blocked** (not openly searchable); planning-only. Prefer GAFI as
  the canonical registry surface; this entry documents the underlying registration.
