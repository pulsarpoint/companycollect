# Kenya — combined profile mapping

## Join keys & precedence

- **Primary join key: company registration number** (BRS). **KRA PIN** links tax; for
  listed companies the **NSE ticker** is an additional key (join to BRS by company
  name).
- **Precedence**: **BRS** (eCitizen, paid) is authoritative for corporate identity,
  status, capital, directors, shareholders, and annual returns — but not open. **NSE**
  is authoritative for the **listed** subset (open).

| Profile path | Source | Source path | Join key | Precedence | Notes |
|---|---|---|---|---|---|
| registration.registration_number | brs_ecitizen | registration_number | reg_no | authoritative | eCitizen/paid |
| tax_identifiers.kra_pin | brs_ecitizen | kra_pin | reg_no | authoritative | tax id |
| tax_identifiers.vat_id | n/a | — | — | n/a | VAT under the PIN |
| legal_identity.legal_name | brs_ecitizen | company_name | reg_no | authoritative | NSE name as alt (listed) |
| legal_identity.company_type | brs_ecitizen | company_type | reg_no | authoritative | Ltd/PLC/CLG/BN/LLP |
| status.status_text | brs_ecitizen | status | reg_no | authoritative | Active/Dormant/Dissolved |
| status.registration_date | brs_ecitizen | registration_date | reg_no | authoritative |  |
| activity.nse_sector | nse_listed | sector_segment | ticker | authoritative (listed) | open |
| registered_location.registered_address | brs_ecitizen | registered_address | reg_no | authoritative | paid |
| capital.nominal_capital | brs_ecitizen | nominal_capital | reg_no | authoritative (paid) | KES |
| owners[] | brs_ecitizen | shareholders (CR12) | reg_no | authoritative (paid) | REDACT (DPA 2019) |
| officers[] | brs_ecitizen | directors (CR12) | reg_no | authoritative (paid) | REDACT |
| listing.* | nse_listed | ticker/sector_segment | ticker | authoritative (listed) | OPEN |
| financial_statements[] | nse_listed / brs_ecitizen | financial results / annual returns | ticker/reg_no | authoritative | NSE open (listed); BRS paid |

## Freshness

- NSE: **event-driven/quarterly** (open). BRS: **live** (eCitizen/paid).

## Missing-data notes

- **No open bulk corporate register; no open private financials** — BRS is paid/gated;
  only NSE (listed) is open.
- **opendata.go.ke has no accessible company dataset**.
- **No separate VAT number** (VAT under the KRA PIN).
- **Directors/shareholders (CR12)** redacted as personal data (Data Protection Act 2019).
- **No BRS per-company values captured** (login/paywall not bypassed); listed data from NSE.
