# Central Supplier Database (CSD) Field Catalog

> **PLANNING-ONLY / LOGIN-GATED.** The mandatory database of suppliers transacting
> with government (links CIPC + SARS). Access is login-gated (registered suppliers
> / government users); no open bulk. Cataloged from public documentation only — no
> records fetched.

## Source Summary

- Country: South Africa
- Source type: supplier_registry
- Organization: National Treasury / OCPO
- URL: https://secure.csd.gov.za/
- License: restricted
- Access: authenticated (login-gated)
- Freshness: live
- Record shape: per-supplier record
- Primary keys: `csd_number`
- Join keys: `csd_number`, `registration_number`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| supplier.csd_number | CSD number (MAAA…) | CSD id | string | identifier | complements CIPC reg number |
| supplier.supplier_name | Legal name | Supplier name | string | legal_name | |
| supplier.registration_number | CIPC reg number | Registration number | string | identifier | **join to CIPC** |
| supplier.tax_status | Tax compliance | SARS status | string | license_or_terms | |
| supplier.bee_status | B-BBEE status | BEE level | string | license_or_terms | SA-specific |
| supplier.bank_status | Bank verification | Bank status | string | metadata | |

## Interpretation Notes

- The CSD is the bridge that links a supplier's **CIPC registration number**,
  **SARS tax status**, and **B-BBEE level** — it carries the registration number
  the open OCDS data lacks. But it is **login-gated** (not open bulk).
- **B-BBEE status** is a South-Africa-specific procurement attribute (empowerment
  level) relevant to government contracting.
- The **open** downstream view of CSD/procurement activity is the eTenders **OCDS**
  data (awards), which is public domain but name-keyed.
- No raw sample record (gated source).
