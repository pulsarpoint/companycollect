# New Zealand Companies Register Field Catalog

> **PLANNING-ONLY.** Public per-company **search** site; no free bulk download or
> API found (help centre documents none). Cataloged from public documentation —
> no records fetched. Directors/shareholders are personal data (Privacy Act 2020).

## Source Summary

- Country: New Zealand
- Source type: official_registry
- Organization: Companies Office / MBIE
- URL: https://companies-register.companiesoffice.govt.nz/
- License: public register (Crown copyright)
- Access: public search (no bulk/API)
- Freshness: live register
- Record shape: per-company search view
- Primary keys: `company_number`
- Join keys: `nzbn`, `company_number`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| company.company_number | Company number | Register id | string | identifier | join to NZBN sourceRegisterUniqueIdentifier |
| company.nzbn | NZBN | 13-digit NZBN | string | identifier | universal join key |
| company.company_name | Company name | Name | string | legal_name | |
| company.company_status | Company status | Status | string | status | Registered/Removed/In liquidation |
| company.incorporation_date | Incorporation date | Incorp date | date | date | |
| company.registered_office | Registered office | Address | string | address | |
| company.directors[] | Directors | Directors | array | person | **PERSONAL DATA — redact** |
| company.shareholders[] | Shareholders | Ownership | array | ownership | **PERSONAL DATA where individuals** |
| company.documents[] | Filed documents | Filings | array | document | financial statements only for those required to file |

## Interpretation Notes

- Searchable by company name, **company number**, or **NZBN**. The company number
  joins to the NZBN API (`sourceRegisterUniqueIdentifier`); use the NZBN API for
  structured identity and this register for **directors/shareholders** and **filed
  documents**.
- **Financial statements** appear in filed documents **only for entities required
  to file** (FMC reporting entities, large/overseas-owned companies). Most NZ
  companies do not file public financials.
- **Personal data**: directors and individual shareholders are personal data under
  the **Privacy Act 2020** — redact in committed/shared outputs.
- No free bulk/API; do not scrape aggressively. No raw sample record.
