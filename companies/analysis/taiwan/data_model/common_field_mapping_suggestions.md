# Common Field Mapping Suggestions — Taiwan

This file is a **suggestion** for a future cross-country mapper. It does **not**
constrain the country-specific Taiwan profile, which is the source of truth.

Taiwan's sources are **fully open** (GCIS + TWSE + TPEx), so these mappings are directly
implementable (no access gating).

| Common field | Taiwan mapping | Source | Notes |
|---|---|---|---|
| company_id | registration.unified_business_number | gcis_company_basic | 8-digit 統一編號 |
| registration_number | registration.unified_business_number | gcis_company_basic | 統一編號 |
| tax_id | registration.unified_business_number | gcis_company_basic | 統一編號 is the tax id |
| vat_id | registration.unified_business_number | gcis_company_basic | no separate VAT id |
| legal_name | legal_identity.legal_name | gcis_company_basic | Chinese; English from TWSE |
| status | status.status_text | gcis_company_basic | 核准設立 etc. |
| legal_form | not_available_in_open_sources | — | not a discrete field; inferable from name (股份有限公司 = Co. Ltd by shares) |
| incorporation_date | status.incorporation_date | gcis_company_basic | ROC→Gregorian (TWSE Gregorian) |
| dissolution_date | status (Revoke_App_Date) | gcis_company_basic | revocation date when present |
| registered_address | registered_location.address | gcis_company_basic | English from TWSE |
| activity_code | activity.industry_code | twse_listed / tpex_listed | listed only; GCIS活業項目 needs another dataset |
| financials | capital + listing | gcis_company_basic / twse_listed | capital (TWD); TWSE OpenAPI has more financial endpoints |
| officers | officers | gcis_company_basic / twse_listed | **REDACT — personal data** |
| owners | not_available_in_open_sources | — | shareholders not in these basic datasets; 董監事 dataset separate |
| source_provenance | source_provenance | all | per-section |

Concepts **not directly available** from the modeled datasets (available elsewhere openly):

- `legal_form` — not a discrete field in GCIS basic data (inferable from the company-name
  suffix, e.g. 股份有限公司).
- `owners` / shareholders / directors-supervisors (董監事) — a **separate GCIS dataset**,
  not modeled here.
- Detailed `financials` beyond capital — additional **TWSE/TPEx OpenAPI** endpoints
  (financial statements, dividends) exist for later enrichment.
- Business activity items (營業項目) — a separate GCIS dataset.
