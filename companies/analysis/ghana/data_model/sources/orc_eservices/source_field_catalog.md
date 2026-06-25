# ORC / RGD — Office of the Registrar of Companies (eServices) Field Catalog

## Source Summary

- Country: Ghana
- Source type: official_registry
- Organization: Office of the Registrar of Companies (formerly Registrar General's Department)
- URL: https://eservices.rgd.gov.gh/
- License: paid per transaction
- Access: **eServices, paid**; **firewalled** from this environment
- Freshness: live register
- Record shape: per-company search + documents (eServices, paid)
- Primary keys: registration_number
- Join keys: registration_number, tin, company_name

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| registration_number | Registration Number | ORC reg number | string | identifier |  | primary key |
| company_name | Company Name | Registered name | string | legal_name |  | |
| company_type | Company Type | Type | string | legal_form |  | Ltd by shares/by guarantee/unlimited/external |
| status | Status | Status | string | status |  | Active/Dissolved/Struck off |
| incorporation_date | Date of Incorporation | Incorporation date | date | date |  | |
| registered_address | Registered Office Address | Registered office | string | address |  | |
| nature_of_business | Nature of Business | Activity | string | activity |  | |
| stated_capital | Stated Capital | Stated capital | decimal | financial |  | GHS |
| tin | TIN | Tax id | string | identifier |  | tax join |
| directors | Directors | Directors | array | person |  | PERSONAL DATA — redact |
| shareholders | Shareholders / Subscribers | Shareholders | array | ownership |  | PERSONAL DATA — redact |

## Interpretation Notes

- The **ORC** (Office of the Registrar of Companies, formerly the **Registrar
  General's Department, RGD**) is the official registrar for companies, business
  names, and partnerships. Company **search** and **documents** (incorporation, status
  report, annual returns) are delivered via the **eServices** portal
  (`eservices.rgd.gov.gh`) — **paid per transaction (GHS)**.
- **Access (verified):** `orc.gov.gh`, `rgd.gov.gh`, `eservices.rgd.gov.gh` resolved
  via DNS (`197.253.x.x`) but **timed out** from this environment — a network block.
  **Not bypassed.** The field model is documented from **public knowledge**; **no real
  values copied**.
- **Identifiers**: the **registration number** is the company id; the **TIN** (GRA)
  links tax; directors may carry the **Ghana Card PIN**.
- **Capital / financials** (stated capital, annual returns) come from paid documents;
  currency **GHS**.
- **Personal data**: directors and shareholders are personal data under Ghana's **Data
  Protection Act, 2012 (Act 843)** — redact.
- Implementation is **blocked on payment** (eServices) and constrained by the network
  block; planning-only.
