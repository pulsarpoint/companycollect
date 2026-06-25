# Saudi Arabia Company Profile — Mapping

This maps the country-specific combined profile to its sources. Both sources are
**gated**: the Ministry of Commerce Commercial Register (CR) is **Nafath login-gated**
(and its inquiry hosts were firewalled from the investigation environment), and the
Saudi Exchange (Tadawul) issuer directory is **public via the browser but WAF-gated**
("Access Denied", 403) for automation. No registry per-company values were captured;
listed identity is from public-knowledge Tadawul symbols.

## Identifiers

- **CR number** (10-digit, region prefix) — primary company id; from the MoC CR (gated).
- **Unified National Number** (`700…`) — cross-agency company id; from the MoC CR (gated).
- **VAT number** (15-digit, ZATCA; starts/ends with `3`) — tax/VAT key; from the MoC CR (gated).
- **Tadawul symbol** (4-digit) / **ISIN** (`SA…`) — listed-entity key; from Tadawul (WAF-gated).

## Mapping table

| Profile path | Source | Source path | Join key | Precedence | Notes |
|---|---|---|---|---|---|
| registration.cr_number | moc_commercial_register | cr_number | yes | CR only | 10-digit; Nafath-gated |
| registration.unified_number_700 | moc_commercial_register | unified_number_700 | yes | CR only | `700…` cross-agency id |
| tax_identifiers.vat_number | moc_commercial_register | vat_number | yes | CR/ZATCA | 15-digit |
| tax_identifiers.tax_id | moc_commercial_register | vat_number | yes | CR/ZATCA | = VAT/Unified Number |
| legal_identity.legal_name | moc_commercial_register | company_name | yes | CR > Tadawul | Tadawul name for listed |
| legal_identity.company_type | moc_commercial_register | company_type | no | CR only | JSC/LLC/SJSC/… |
| status.status_text | moc_commercial_register | status | no | CR only | Active/Expired/Cancelled |
| status.issue_date | moc_commercial_register | issue_date | no | CR only | Hijri→Gregorian |
| status.expiry_date | moc_commercial_register | expiry_date | no | CR only | Hijri→Gregorian |
| activity.activities | moc_commercial_register | activities | no | CR only | ISIC |
| activity.tadawul_sector | tadawul_listed | sector | no | Tadawul | listed only |
| registered_location.head_office | moc_commercial_register | head_office | no | CR only | |
| capital.capital_amount | moc_commercial_register | capital | no | CR only | SAR; gated |
| officers[] | moc_commercial_register | managers | no | CR only | **PERSONAL DATA — REDACT (PDPL)** |
| listing.symbol | tadawul_listed | symbol | yes | Tadawul | 4-digit |
| listing.isin | tadawul_listed | isin | yes | Tadawul | `SA…` |
| listing.sector | tadawul_listed | sector | no | Tadawul | |
| financial_statements[] | tadawul_listed | financial_statements | no | Tadawul | SAR; listed only |
| source_provenance[] | both | n/a | n/a | n/a | per-section provenance |

## Precedence and joins

- **Identity / registration / tax**: the **MoC Commercial Register** is authoritative
  (CR number, Unified Number, VAT, type, status, capital). All Nafath-gated.
- **Legal name**: prefer the CR name; use the Tadawul name for listed companies when the
  CR is not accessible.
- **Listing + financials**: **Tadawul** only, for the listed subset; join to the CR by
  company name / Unified Number (no shared numeric key across the two).
- **Currency** SAR throughout. **Dates** primarily **Hijri** (convert to Gregorian).

## Missing / restricted

- Both sources gated → no open per-company values were captured. CR is **Nafath
  login-gated** and inquiry hosts were **firewalled/NXDOMAIN**; Tadawul is **WAF-gated**
  for automation. `open.data.gov.sa` was **firewalled**.
- **Managers / owners / partners** are personal data under the **PDPL (Royal Decree
  M/19 of 1443H)** — redact in any stored profile.
- Private-company financials are **not public**; only Tadawul-listed financials are.
