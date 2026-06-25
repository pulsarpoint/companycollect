# SECP eServices — Company / LLP Registry Field Catalog

## Source Summary

- Country: Pakistan
- Source type: official_registry
- Organization: Securities and Exchange Commission of Pakistan (SECP)
- URL: https://eservices.secp.gov.pk/eServices/
- License: restricted
- Access: **firewalled / WAF-blocked from this environment** (403 / timeout)
- Freshness: live
- Record shape: per-company lookup (planning-only)
- Primary keys: cuin
- Join keys: cuin, company_name

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| cuin | CUIN | SECP company id | string | identifier |  | registry key |
| company_name | Company Name | Registered name | string | legal_name |  | |
| company_kind | Company Kind | Entity kind | string | legal_form |  | private/public ltd, SMC, LLP |
| status | Status | Status | string | status |  | active/dormant/dissolved |
| incorporation_date | Date of Incorporation | Inc. date | date | date |  | |
| registered_address | Registered Office Address | Registered office | string | address |  | |
| directors | Directors / Officers | Directors | array | person |  | **PERSONAL DATA — redact** |

## Interpretation Notes

- **SECP eServices** is the **authoritative** Pakistani company/LLP registrar, keyed on the
  **CUIN (Company Universal Identification Number)**. From this environment the SECP website
  returned **HTTP 403 (WAF)** and `eservices.secp.gov.pk` **timed out** — **firewalled**. The
  eServices portal hosts company name search and filings (filings require login).
- All fields here are **planning-only**, documented from public knowledge — **no values
  captured** (SECP not reachable; WAF not bypassed).
- **Join**: the **CUIN** is the registry key; PSX listed companies join here by **name** (PSX
  does not publish CUIN). The **NTN** (FBR) is a separate tax identifier.
- **Personal data**: directors/officers are natural persons — redact.
- No `sample_record.json`: restricted/firewalled source, nothing captured.
