# Source inventory — Nigeria

| Source | Type | Org | Access | Formats | Financials | Status |
|---|---|---|---|---|---|---|
| NGX equities API (`doclib.ngxgroup.com`) | Listed market data + financials | NGX | **Open JSON** | json, html, pdf | yes (listed) | **recommended** |
| CAC registry (`search.cac.gov.ng`) | Official registry + documents | CAC | Cloudflare-gated; paid docs | html, pdf | yes (AFS, paid) | blocked_by_payment |
| CAC BO Register (`bor.cac.gov.ng`) | Beneficial ownership (PSC) | CAC | Browser; API token-gated | json, html | no | blocked_by_authentication |
| data.gov.ng | Open-data portal | NITDA | Unreachable | — | no | unavailable |

## Identifiers

- **RC number** — Registration of Company (limited companies).
- **BN number** — Business Name (sole proprietors / partnerships).
- **IT number** — Incorporated Trustees (NGOs / associations).
- **TIN** — Tax Identification Number (FIRS); **VAT** registration.

## Key facts

- **CAC** is the official register but **Cloudflare-gated** (search) and **paid**
  (documents); no open bulk/API.
- The **CAC BO register** (PSC) is public via browser but **token-gated** for
  automation — and a token-less endpoint leaked an individual's PII (a
  misconfiguration that was **not used/stored**).
- **NGX** is the one **open** source — listed companies + financials (verified live:
  DANGCEM, MTNN, GTCO…). Listed only (~150).
- **data.gov.ng** unreachable. Currency **NGN**; English. Directors/beneficial owners
  are personal data (NDPA 2023) → redact.
