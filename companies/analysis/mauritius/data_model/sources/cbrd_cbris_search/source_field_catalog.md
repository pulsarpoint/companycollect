# CBRD — CBRIS Online Company Search Field Catalog

## Source Summary

- Country: Mauritius
- Source type: official_registry
- Organization: Corporate and Business Registration Department (CBRD), via Mauritius Network Services (MNS)
- URL: https://onlinesearch.mns.global/
- License: restricted
- Access: **public search, Cloudflare Turnstile-gated; documents paid**
- Freshness: live
- Record shape: per-company search result (planning-only)
- Primary keys: brn
- Join keys: brn, company_name

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| brn | Business Registration Number | CBRD company id | string | identifier |  | registry key |
| company_name | Company / Business Name | Registered name | string | legal_name |  | |
| company_type | Type | Entity type | string | legal_form |  | ltd by shares / sole trader / société |
| company_status | Status | Status | string | status |  | live / removed / defunct |
| incorporation_date | Date of Incorporation/Registration | Inc./reg. date | date | date |  | |
| registered_office_address | Registered Office Address | Registered office | string | address |  | |
| directors | Directors | Directors | array | person |  | **PERSONAL DATA — redact** |
| shareholders | Shareholders | Shareholders | array | ownership |  | **PERSONAL DATA — redact** |

## Interpretation Notes

- The **CBRD CBRIS** online search (`onlinesearch.mns.global`) is the **authoritative**
  Mauritius company/business register lookup, keyed on the **BRN (Business Registration
  Number)**. The front-end is an **Angular SPA** that loads **Cloudflare Turnstile**, so the
  search is **CAPTCHA-gated**; full documents (constitution, annual return, financials) are
  **paid**. **No open bulk or free API** (guessed API endpoints 404). All fields here are
  **planning-only**, documented from public knowledge — **no values captured** (Turnstile not
  bypassed).
- **Identifier**: the **BRN** is the registry key and the basis of the entity's **MRA** tax
  identity. Join the open ICT directory to this register by **name** (the directory has no BRN).
- **Personal data**: directors and shareholders are natural persons under the **Data
  Protection Act 2017** — redact.
- No `sample_record.json`: restricted/paid/Turnstile-gated source, nothing captured.
