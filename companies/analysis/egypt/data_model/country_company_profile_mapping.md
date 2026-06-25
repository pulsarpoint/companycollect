# Egypt — combined profile mapping

## Join keys & precedence

- **Primary join key: Commercial Registry number** (رقم السجل التجاري) — shared by
  GAFI and the Commercial Registry. **Tax ID** (الرقم الضريبي) links tax; for listed
  companies the **EGX symbol / ISIN** is an additional key (join to the registry by
  company name).
- **Precedence**: **GAFI / Commercial Registry** are authoritative for corporate
  identity, status, capital, board, shareholders — but **gated** (GAFI login;
  Commercial Registry not openly searchable). **EGX** is authoritative for the
  **listed** subset (browser-public, WAF-gated for automation).

| Profile path | Source | Source path | Join key | Precedence | Notes |
|---|---|---|---|---|---|
| registration.commercial_registry_number | gafi_registry | commercial_registry_number | CR no | authoritative | gated; same in Commercial Registry |
| tax_identifiers.tax_id | gafi_registry | tax_id | CR no | authoritative | 9-digit |
| tax_identifiers.vat_id | n/a | — | — | n/a | VAT under the Tax ID |
| legal_identity.legal_name | gafi_registry | company_name | CR no | authoritative | EGX name as alt (listed) |
| legal_identity.company_type | gafi_registry | company_type | CR no | authoritative | S.A.E./LLC/branch |
| status.status_text | gafi_registry | status | CR no | authoritative | Active/Under liquidation/Struck off |
| activity.activity_purpose | gafi_registry | activity | CR no | authoritative | |
| activity.egx_sector | egx_listed | sector | symbol | authoritative (listed) | WAF-gated |
| registered_location.registered_address | gafi_registry | registered_address | CR no | authoritative | gated |
| capital.capital_amount | gafi_registry | capital | CR no | authoritative (gated) | EGP |
| owners[] | gafi_registry | shareholders | CR no | authoritative (gated) | REDACT (PDP 151/2020) |
| officers[] | gafi_registry | directors | CR no | authoritative (gated) | REDACT |
| listing.* | egx_listed | egx_symbol/isin/sector | symbol | authoritative (listed) | browser-public, WAF-gated |
| financial_statements[] | egx_listed | financial statements | symbol | authoritative (listed) | EGP; WAF-gated; private not open |

## Freshness

- GAFI / Commercial Registry: **live** (gated). EGX: **event-driven/quarterly**
  (browser-public, WAF-gated).

## Missing-data notes

- **No open company register; no open programmatic financials** — GAFI login-gated,
  Commercial Registry not openly searchable, EGX WAF-gated.
- **data.gov.eg / egypt.gov.eg unreachable**; CAPMAS is statistics only.
- **No separate VAT number** (VAT under the Tax ID).
- **Board/shareholders** redacted as personal data (PDP Law 151/2020).
- **No registry per-company values captured** (gates not bypassed); listed identity
  from EGX.
