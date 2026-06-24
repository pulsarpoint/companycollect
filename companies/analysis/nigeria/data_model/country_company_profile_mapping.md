# Nigeria — combined profile mapping

## Join keys & precedence

- **Primary join key: RC number** (companies; BN for business names, IT for
  incorporated trustees). **TIN** (FIRS) links tax; for listed companies the **NGX
  symbol** is an additional key (join to CAC by company name).
- **Precedence**: **CAC** (search Cloudflare-gated; documents paid) is authoritative
  for corporate identity, status, capital, directors, shareholders, and AFS — but not
  open. **NGX** is authoritative for the **listed** subset (open). The **CAC BO
  register (PSC)** adds beneficial owners (token-gated; personal data).

| Profile path | Source | Source path | Join key | Precedence | Notes |
|---|---|---|---|---|---|
| registration.rc_number | cac_registry | rc_number | rc_number | authoritative | Cloudflare/paid |
| tax_identifiers.tin | firs | TIN | rc_number | authoritative | FIRS |
| tax_identifiers.vat_id | firs | VAT reg | rc_number | authoritative | FIRS |
| legal_identity.legal_name | cac_registry | company_name | rc_number | authoritative | NGX name as alt (listed) |
| legal_identity.company_type | cac_registry | company_type | rc_number | authoritative | Plc/Ltd/Ltd-Gte/BN/IT |
| status.status_text | cac_registry | status | rc_number | authoritative | Active/Inactive/... |
| status.registration_date | cac_registry | registration_date | rc_number | authoritative |  |
| activity.nature_of_business | cac_registry | nature_of_business | rc_number | authoritative | CAC |
| activity.ngx_sector | ngx_equities | [].Sector | symbol | authoritative (listed) | open |
| registered_location.registered_address | cac_registry | registered_address | rc_number | authoritative | paid |
| capital.share_capital | cac_registry | share_capital | rc_number | authoritative (paid) | NGN |
| owners[] | cac_registry / cac_bor_psc | shareholders / pscName | rc_number | authoritative | REDACT (NDPA 2023) |
| officers[] | cac_registry | directors | rc_number | authoritative (paid) | REDACT |
| listing.* | ngx_equities | [].Symbol/Market/ClosePrice | symbol | authoritative (listed) | OPEN |
| financial_statements[] | ngx_equities / cac_registry | issuer financials / AFS | symbol/rc_number | authoritative | NGX open (listed); CAC AFS paid |

## Freshness

- NGX: **daily** (open). CAC: **live** (Cloudflare/paid). CAC BO: **live** (token).

## Missing-data notes

- **No open bulk corporate register; no open private financials** — CAC is
  gated/paid; only NGX (listed) is open.
- **CAC BO register** is token-gated; a misconfigured endpoint leaked PII (not used).
- **data.gov.ng** unreachable.
- **Directors/shareholders/PSC** redacted as personal data (NDPA 2023).
- **No CAC per-company values captured** (Cloudflare/paywall not bypassed); listed
  data from NGX.
