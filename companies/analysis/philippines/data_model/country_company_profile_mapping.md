# Philippines — combined profile mapping

## Join keys & precedence

- **Primary join key: SEC Registration Number** (corporations/partnerships). **TIN**
  (BIR) links tax; the **DTI BN number** keys sole proprietorships; for listed
  companies the **PSE stock symbol** is an additional key (join to SEC by name).
- **Precedence**: **SEC** (via SEC Express / eFAST) is authoritative for corporate
  identity, status, capital, officers, stockholders, and financials (AFS) — but
  **paid**. **PSE EDGE** is authoritative for the **listed** subset (open). **DTI
  BNRS** for sole proprietors.

| Profile path | Source | Source path | Join key | Precedence | Notes |
|---|---|---|---|---|---|
| registration.sec_registration_number | sec_express | sec_registration_number | sec_reg | authoritative | paid |
| registration.dti_bn_number | dti_bnrs | bn_number | bn_number | authoritative | sole props |
| tax_identifiers.tin | sec_express | tin | sec_reg | authoritative | BIR |
| tax_identifiers.vat_id | n/a | — | — | n/a | TIN-based; no separate VAT |
| legal_identity.legal_name | sec_express | company_name | sec_reg | authoritative | PSE name as alt |
| legal_identity.company_type | sec_express | company_type | sec_reg | authoritative | Stock/Non-stock/OPC |
| status.status_text | sec_express | status | sec_reg | authoritative | Active/Revoked/... |
| status.incorporation_date | sec_express | incorporation_date | sec_reg | authoritative |  |
| activity.primary_purpose | sec_express | primary_purpose | sec_reg | authoritative | PSIC |
| activity.pse_sector | pse_edge | sector/subsector | symbol | authoritative (listed) | open |
| registered_location.registered_address | sec_express | registered_address | sec_reg | authoritative | paid |
| capital.* | sec_express | authorized/paid_up | sec_reg | authoritative (paid) | PHP |
| owners[] | sec_express | stockholders | sec_reg | authoritative (paid) | REDACT natural persons |
| officers[] | sec_express | directors/officers | sec_reg | authoritative (paid) | REDACT |
| listing.* | pse_edge | stock_symbol/sector/listing_date | symbol | authoritative (listed) | OPEN |
| financial_statements[] | sec_express | AFS | sec_reg | authoritative (paid) | PHP; PSE EDGE for listed |

## Freshness

- SEC (Express/eFAST): **live** (paid). PSE EDGE: **event-driven/quarterly** (open).
  DTI BNRS: **live** (free, sole props).

## Missing-data notes

- **No open bulk corporate register; no open private financials** — SEC is paid; only
  PSE EDGE (listed) is open.
- **data.gov.ph has no accessible company dataset** (JS SPA).
- **No separate VAT number** (TIN-based).
- **Directors/officers/stockholders** redacted as personal data (Data Privacy Act 2012).
- **No SEC per-company values captured** (paywall not bypassed); listed data from PSE.
