# Source inventory — Saudi Arabia

| Source | Type | Org | Access | Formats | Financials | Status |
|---|---|---|---|---|---|---|
| MoC Commercial Register (`mc.gov.sa`) | Official registry | Ministry of Commerce | Nafath login-gated; hosts firewalled | html | no (capital only) | blocked_by_authentication |
| Saudi Exchange / Tadawul (`saudiexchange.sa`) | Listed financials | Saudi Exchange | Browser; WAF (Access Denied) | html, pdf | yes (listed) | blocked_by_authentication |
| open.data.gov.sa | Open-data portal | SDAIA | Firewalled here | csv/xlsx/json | no | unavailable |

## Identifiers

- **CR number (رقم السجل التجاري)** — 10-digit, region prefix (1010 Riyadh, 2050/2051
  Eastern, 4030 Jeddah, …).
- **Unified National Number / "700 number" (الرقم الموحد)** — `700…` unified company id.
- **VAT number** — 15-digit (ZATCA), starts and ends with `3`.

## Key facts

- **MoC Commercial Register** is the official register but its CR inquiry is **Nafath
  login-gated** and the inquiry sub-hosts were **NXDOMAIN/firewalled**; the **Saudi
  Business Center** was not reachable. No open bulk/API.
- **Tadawul** (listed financials) is **public via the browser but WAF-gated**
  ("Access Denied").
- **open.data.gov.sa** is **firewalled** here. Currency **SAR**; Arabic + English.
- Managers/owners are personal data (PDPL, Royal Decree M/19 of 1443H) → redact.
