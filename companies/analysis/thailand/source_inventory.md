# Source inventory — Thailand

| Source | Type | Org | Access | Formats | Financials | Status |
|---|---|---|---|---|---|---|
| DBD OpenAPI (`openapi.dbd.go.th`) | Official registry API | DBD / MoC | **Open, no key** | json | capital only | **recommended** |
| DBD DataWarehouse (`datawarehouse.dbd.go.th`) | Financial statements | DBD | Login-gated | html, json | yes (full) | blocked_by_authentication |
| data.go.th (CKAN) | Open-data portal | DGA | WAF-blocked here | csv/xlsx/json | no | unavailable |
| SET (`set.or.th`) | Listed financials | SET | Public (browser) | html, pdf | yes (listed) | useful_secondary_source |

## Identifiers

- **Juristic Person ID (เลขทะเบียนนิติบุคคล)** — **13-digit** = company id = **Tax ID**.
- **VAT** — same 13-digit Tax ID (no separate VAT number).
- **TSIC** — Thailand Standard Industrial Classification (activity).

## Key facts

- The **DBD OpenAPI** is a genuine **open** official company API (no token):
  per-company by 13-digit ID, returning identity, type, status, register date, TSIC
  activity, **register & paid-up capital (THB)**, and a structured address. Verified
  live (PTT, Bangkok Bank, CP All, INET).
- **Full financial statements** are in **DBD DataWarehouse** (login) and **SET**
  (listed). No open bulk register; no separate VAT number.
- `data.go.th` was WAF-blocked for automation here.
- Currency **THB**; Thai + English names. Directors/shareholders (PDPA) not in the
  open API.
