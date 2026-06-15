# India Company Profile — Source Mapping

> Keyed on the **21-char CIN** (Corporate Identification Number). Identity +
> capital are **open** (MCA Company Master Data via data.gov.in OGD API,
> GODL-India). India has **GST, not VAT**; **PAN** (tax id) and **GSTIN** are
> **not** in the open data. Financials are **paid** (MCA AOC-4/XBRL) or
> **listed-only** (BSE/NSE) — planning-only. Officers (DIN) are personal-data
> gated (MCA portal).

## Field mapping

| Profile path | Source | Source path | Join key | Freshness | License/Access | Precedence / Notes |
|---|---|---|---|---|---|---|
| registration.cin | mca_company_master_data | corporate_identification_number | cin | snapshot 2015-2021 | GODL/open | Authoritative id. |
| registration.cin_decoded.* | (derived) | parse CIN | — | — | — | listing/industry/state/year/type/RoC. |
| tax_identifiers.pan / gstin / vat_id | — | — | — | — | not available | PAN/GSTIN not open; no VAT. |
| legal_identity.legal_name | mca_company_master_data | company_name | — | snapshot | GODL/open | Primary name. |
| legal_identity.company_class / category / sub_category | mca_company_master_data | company_class / company_category / company_sub_category | — | snapshot | GODL/open | Legal form. |
| status.status_raw / status | mca_company_master_data | company_status | — | snapshot | GODL/open | Normalize casing. |
| incorporation.date_of_registration | mca_company_master_data | date_of_registration | — | snapshot | GODL/open | Two date formats. |
| capital.authorized/paidup | mca_company_master_data | authorized_capital / paidup_capital | — | snapshot | GODL/open | INR; capital only. |
| activity.principal_business_activity / industrial_class | mca_company_master_data | principal_business_activity / industrial_class | — | snapshot | GODL/open | industrial_class 2021 only. |
| registered_location.* | mca_company_master_data | registered_office_address / registered_state / registrar_of_companies | — | snapshot | GODL/open | Free-text address. |
| compliance_markers.latest_year_* | mca_company_master_data | latest_year_ar / latest_year_bs | — | snapshot 2021 | GODL/open | Filing-year markers, not figures. |
| financial_statements[] | mca_xbrl_financials / bse_nse_listed_financials | filing.* / results.* | cin / isin | annual/quarterly | paid / exchange terms | PLANNING-ONLY; INR. |
| officers[] | mca_portal_master_data | view.directors_din | cin | live | gated | PLANNING-ONLY; DPDP personal data — redact. |

## Source precedence

1. **mca_company_master_data** (data.gov.in OGD) — authoritative open identity +
   capital. Use the **newest snapshot per state** (2021 > 2018 > 2015). Note it is
   a point-in-time snapshot, not live.
2. **mca_portal_master_data** — the **live** register (fresher) and the only source
   of directors/charges; WAF-gated, documents paid. Use to refresh status or for
   officers (personal data) only when lawful.
3. **mca_xbrl_financials** — authoritative all-company financials; paid.
4. **bse_nse_listed_financials** — open financials for **listed** companies only.

Conflict rules:
- **Status/capital freshness:** the live portal supersedes the snapshot when
  available; otherwise use the newest snapshot.
- **Financials:** prefer MCA AOC-4/XBRL (authoritative) where paid access exists;
  otherwise BSE/NSE for listed companies. The open layer has none.

## Join keys

- **CIN (21-char)** is the universal join key. Listed financials join via
  **ISIN ↔ CIN**. PAN/GSTIN are not available to join on.

## Missing / restricted data

- **Financial statements** (P&L, balance sheet) — paid (MCA) or listed-only (BSE/
  NSE). Not in the open data.
- **Directors / officers (DIN)** — MCA portal only; personal data (DPDP), redact.
- **PAN / GSTIN** — not in the open data. **No VAT** (GST regime).
- **Beneficial ownership (SBO)** — filed with MCA but not openly published.
- **Live feed** — open data is point-in-time snapshots (latest 2021).
- **Contact email** in the dataset is personal data — redact.
