# United Arab Emirates — combined profile mapping

## Join keys & precedence

- **Primary join key: trade/commercial license number** (per emirate DED) **or
  free-zone registration number** — the per-authority company id. The **NER economic
  register number** is the **national unified** id; the **TRN** (15-digit, FTA) is the
  cross-emirate tax key (and the VAT id).
- **There is no single national company id** — `license_authority` routes to the
  underlying registry (emirate DED vs free zone). For listed companies the **DFM/ADX
  symbol** is an additional key.
- **Precedence**: the **issuing authority** (emirate DED or free-zone registrar) is
  authoritative for that company's identity; the **NER** unifies across authorities;
  **DFM/ADX** is authoritative for the **listed** subset. All registry layers are
  gated (login/WAF); listed financials are browser-public (WAF-gated for automation).

| Profile path | Source | Source path | Join key | Precedence | Notes |
|---|---|---|---|---|---|
| registration.trade_license_number | emirate_deds | trade_license_number | license_no | authoritative (mainland) | per-emirate; gated |
| registration.economic_register_number | national_economic_register | economic_register_number | unified | authoritative (unified) | login-gated |
| registration.license_authority | national_economic_register | license_authority | unified | routing | DED / free zone |
| tax_identifiers.trn / vat_id | national_economic_register | trn | unified | authoritative | 15-digit; FTA; VAT = TRN |
| legal_identity.legal_name | emirate_deds | trade_name | license_no | authoritative | DFM/ADX name as alt (listed) |
| legal_identity.company_type | national_economic_register | company_type | unified | authoritative | LLC/PJSC/FZE/... |
| status.status_text | emirate_deds | status | license_no | authoritative | Active/Expired/Cancelled |
| status.license_expiry_date | emirate_deds | expiry_date | license_no | authoritative | annual renewal |
| activity.activities | emirate_deds | activities | license_no | authoritative | DED/ISIC |
| activity.exchange_sector | dfm_adx_listed | sector | symbol | authoritative (listed) | WAF-gated |
| registered_location.emirate | national_economic_register | emirate | unified | authoritative |  |
| registered_location.registered_address | freezone_public_registers | registered_address | reg_no | authoritative (free zone) | WAF-gated |
| owners[] | emirate_deds | owners | license_no | authoritative (gated) | REDACT (PDPL) |
| listing.* | dfm_adx_listed | symbol/exchange/isin | symbol | authoritative (listed) | browser-public, WAF-gated |
| financial_statements[] | dfm_adx_listed | financial statements | symbol | authoritative (listed) | AED; WAF-gated; private not open |

## Freshness

- Emirate DEDs / NER / free-zone registers: **live** (gated). DFM/ADX:
  **event-driven/quarterly** (browser-public, WAF-gated).

## Missing-data notes

- **No single national register; no open bulk; no open programmatic financials** —
  every registry layer is login/WAF/rate-limited.
- **data.gov.ae / bayanat.ae unreachable** — no company dataset.
- **No separate VAT number** (VAT = the TRN).
- **Owners/managers/directors** redacted as personal data (PDPL; DIFC/ADGM DP laws).
- **No registry per-company values captured** (gates not bypassed); listed identity
  from DFM/ADX.
