# South Korea Company Profile — Source Mapping

> Two registration ids: **법인등록번호** (corporate registration number, 13-digit,
> court) and **사업자등록번호** (business registration number, 10-digit, NTS = tax
> id = VAT number; no separate VAT id). Identity + financials come from the
> **OpenDART API** (free key — key-gated/planning-only here; covers listed +
> external-audit companies). **NTS status API** (free key) adds operating status.
> Exact legal form, capital, directors are **paid** (court registry IROS).

## Field mapping

| Profile path | Source | Source path | Join key | Freshness | License/Access | Precedence / Notes |
|---|---|---|---|---|---|---|
| registration.corp_registration_number | opendart_api | company.jurir_no | corp_reg_no | live | free key | Court id; join to IROS. |
| registration.business_registration_number | opendart_api | company.bizr_no | biz_reg_no | live | free key | Tax id; join to NTS. |
| registration.dart_corp_code | opendart_api | corpCode.corp_code | dart_corp_code | live | free key | OpenDART join key. |
| registration.stock_code | opendart_api | company.stock_code | — | live | free key | Listed only. |
| tax_identifiers.tax_id / vat_id | opendart_api | company.bizr_no | — | live | free key | Same value; no separate VAT id. |
| legal_identity.legal_name(_en) | opendart_api | company.corp_name(_eng) | — | live | free key | Primary name. |
| legal_identity.market_class | opendart_api | company.corp_cls | — | live | free key | Y/K/N/E. |
| legal_identity.legal_form | iros_court_register | registry.company_type | corp_reg_no | live | paid | Exact form (주식회사/…). |
| status.business_status / tax_type / closure_date | nts_business_status | data[].b_stt / tax_type / end_dt | biz_reg_no | live | free key | Operating status. |
| incorporation.establishment_date | opendart_api | company.est_dt | — | live | free key | YYYYMMDD. |
| activity.industry_code_ksic | opendart_api | company.induty_code | — | live | free key | KSIC. |
| registered_location.* | opendart_api | company.adres / hm_url | — | live | free key | |
| capital.registered_capital_krw | iros_court_register | registry.capital | corp_reg_no | live | paid | KRW; planning-only. |
| financial_statements[] | opendart_api | fnlttSinglAcntAll rows | dart_corp_code | live | free key | KRW; BS/IS/CIS/CF/SCE. |
| officers[] | opendart_api / iros_court_register | company.ceo_nm / registry.directors | corp_reg_no | live | gated | PLANNING-ONLY; personal data (PIPA) — redact. |

## Source precedence

1. **opendart_api** — authoritative for identity (both registration numbers,
   names, market class, industry, establishment date, address) and **financials**
   (XBRL, KRW). Free key. Covers listed + external-audit companies.
2. **nts_business_status** — authoritative for **operating status** (active/
   suspended/closed) by business registration number. Free key.
3. **iros_court_register** — exact **legal form, capital, directors**, and the
   **unlisted long tail**. Paid; planning-only.

Conflict rules:
- **Status:** NTS business-status (active/closed) supersedes the DART listing class
  for operating status.
- **Identity/financials:** OpenDART is authoritative for DART-registered companies.
- **Legal form / capital / directors:** court registry (paid) is authoritative.

## Join keys

- **dart_corp_code** within OpenDART (corpCode ↔ company ↔ financials).
- **business registration number (10-digit)** links OpenDART ↔ NTS.
- **corporate registration number (13-digit)** links OpenDART ↔ court registry.
- Tax id = business registration number = VAT number (no separate VAT id).

## Missing / restricted data

- **Unlisted micro/SME companies** — not in DART; court registry (paid) only.
- **Financials of non-DART companies** — not available openly.
- **Exact legal form, capital, full director list** — court registry (paid).
- **Directors/CEO** — personal data (PIPA), redact.
- **No separate VAT id** — it is the business registration number.
