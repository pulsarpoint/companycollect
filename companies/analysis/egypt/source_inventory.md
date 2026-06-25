# Source inventory — Egypt

| Source | Type | Org | Access | Formats | Financials | Status |
|---|---|---|---|---|---|---|
| EGX listed (`egx.com.eg`) | Listed companies + financials | EGX | Browser; WAF-gated | html, json, pdf | yes (listed) | blocked_by_authentication |
| GAFI (`gafi.gov.eg`) | Company establishment / eServices | GAFI | Login-gated | html | no (capital only) | blocked_by_authentication |
| Commercial Registry (السجل التجاري) | Commercial registration | GOEIC / Min. Supply | Not openly searchable | html | no | blocked_by_authentication |
| egypt.gov.eg / data.gov.eg | Open-data portal | MCIT | Unreachable | — | no | unavailable |

## Identifiers

- **Commercial Registry number (رقم السجل التجاري)** — commercial registration id.
- **Tax ID (الرقم الضريبي)** — Egyptian Tax Authority (9-digit).
- **Unified company number** — links registry + tax.
- **EGX symbol / ISIN** (`EG…`) — listed companies.

## Key facts

- **No open company register** — GAFI eServices are login-gated; the Commercial
  Registry is not openly searchable; no open bulk/API.
- **EGX** (listed companies + financials) is **public via the browser but WAF-gated**
  for automation.
- **data.gov.eg / egypt.gov.eg** unreachable; CAPMAS is statistics only.
- Currency **EGP**; Arabic + English. Directors/shareholders are personal data (PDP
  Law 151/2020) → redact.
