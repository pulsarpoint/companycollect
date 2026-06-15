# MCA21 Portal — Company/LLP Master Data & Public Documents Field Catalog

> **PLANNING-ONLY.** The live MCA register (mca.gov.in) offers a **free per-CIN
> master-data lookup** plus **pay-per-document** access to filings. It is
> **WAF-protected** (HTTP 403 to automated clients here). Cataloged from public
> documentation only — nothing fetched, no values copied.

## Source Summary

- Country: India
- Source type: official_registry
- Organization: Ministry of Corporate Affairs
- URL: https://www.mca.gov.in/.../company-master-data.html
- License: restricted
- Access: free per-CIN lookup + paid documents (WAF-blocked to bots)
- Freshness: live register (fresher than the data.gov.in snapshots)
- Record shape: per-company lookup view
- Primary keys: `cin`
- Join keys: `cin`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| view.cin | CIN | Corporate Identification Number | string | identifier | join key |
| view.company_status_master | master-data fields | Live status/capital/dates/activity | object | metadata | free lookup |
| view.directors_din | Directors (DIN) | Directors + DIN | array | person | **PERSONAL DATA (DPDP) — redact** |
| view.charges | Index of Charges | Registered charges (secured debt) | array | relationship | |
| view.public_documents | View Public Documents | Filed documents incl. AOC-4/XBRL | array | document | pay-per-document |

## Interpretation Notes

- This is the **live** equivalent of the open master data, and additionally the
  only source of **directors/DIN** and **charges**. Directors are **personal data**
  (DPDP Act) and must be redacted in any committed output.
- The portal is **WAF-protected** (403) and document access is **paid**, so it is
  not an open bulk route. For open data use `mca_company_master_data`
  (data.gov.in); for financials see `mca_xbrl_financials` (paid) and
  `bse_nse_listed_financials` (listed).
- No raw sample record (gated source).
