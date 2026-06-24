# CAC — Corporate Affairs Commission Field Catalog

## Source Summary

- Country: Nigeria
- Source type: official_registry
- Organization: Corporate Affairs Commission (CAC)
- URL: https://search.cac.gov.ng/
- License: restricted / paid documents
- Access: **Cloudflare-gated** search; **paid** documents
- Freshness: live register
- Record shape: per-company search (gated) + paid documents
- Primary keys: rc_number
- Join keys: rc_number, tin, company_name

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| rc_number | RC Number | Company registration number | string | identifier |  | primary key (BN/IT for others) |
| company_name | Company Name | Registered name | string | legal_name |  | |
| company_type | Company Type | Type | string | legal_form |  | Plc/Ltd/Ltd-Gte/BN/IT |
| status | Status | Status | string | status |  | Active/Inactive/Dissolved |
| registration_date | Registration Date | Registration date | date | date |  | |
| registered_address | Registered Address | Registered office | string | address |  | |
| nature_of_business | Nature of Business | Activity | string | activity |  | |
| share_capital | Share Capital | Share capital | decimal | financial |  | NGN; paid |
| directors | Directors | Directors | array | person |  | PERSONAL DATA — redact |
| shareholders | Shareholders | Shareholders | array | ownership |  | PERSONAL DATA — redact |
| annual_returns_afs | Annual Returns / AFS | Financials | array | financial |  | NGN; paid |

## Interpretation Notes

- **CAC** is the official registrar for **companies (RC)**, **business names (BN)**,
  and **incorporated trustees (IT)**. Access is **not open**:
  - The public search (`search.cac.gov.ng`) is **Cloudflare-gated** ("Just a
    moment…") — bot-blocked; **not bypassed**.
  - Company **documents** (status report, certified true copies, **annual returns**,
    **AFS**) are obtained via the CAC portal — **paid per document**.
- The field model above is from **public knowledge**; **no real company values are
  copied** (the search is gated and documents are paid).
- **Identifiers**: the **RC number** is the company id for limited companies; **TIN**
  (FIRS) links tax.
- **Capital / financials** (share capital, annual returns, AFS) come from paid
  documents; currency **NGN**.
- **Personal data**: directors and shareholders are personal data under the **NDPA
  2023** — redact.
- Implementation is **blocked on payment** (and Cloudflare for search); planning-only.
