# Bulgaria — Source Inventory

| Source | Type | Access | Format | License | Status |
|---|---|---|---|---|---|
| **Търговски регистър** (Commercial Register) | Official registry | Free search / registered API / agreement bulk | HTML/JSON/XML | Free search; CC-BY publications | **recommended** (open-ish) |
| **ГФО** — Annual Financial Statements | Official financials | Free per-company documents | **PDF** | Public (register) | useful secondary (**document-based**) |
| **data.egov.bg** (Registry Agency publications) | Open data portal | Free | CSV/XML/JSON | **CC-BY** | **recommended** (CC-BY daily publications) |
| CompanyBook.BG | Third-party API | Free non-financial / paid financials | JSON | Per terms | useful secondary |
| Регистър БУЛСТАТ | Official registry | Free search | HTML | Public | useful secondary (non-traders) |
| Регистър на действителните собственици | Beneficial ownership | Restricted | PDF | Restricted | blocked by authentication |
| Commercial aggregators (APIS, ...) | Commercial API | Paid | JSON | Commercial | useful secondary (structured financials) |

## Access points

- Commercial Register search: https://portal.registryagency.bg/CR/en (free; single lookups). Web service: registration/contract. Full bulk: data-sharing agreement.
- Open data (CC-BY publications): https://data.egov.bg/ ("Търговски регистър" dataset; api_key for resource data)
- ГФО (financials): filed PDFs in the Commercial Register (public by 30 June)
- BULSTAT: https://www.bulstat.bg/ ; aggregator: https://apis.bg/

## Key facts

- **Registry open-ish**: free public search + **CC-BY daily publications** (data.egov.bg) + official web
  service (registration); **full bulk by agreement**.
- **Financials (ГФО) = public PDFs**, not structured open (no XBRL) — parse or use a paid provider.
- **Single key**: ЕИК (EIK, 9-digit; 13 for branches) = VAT root (`BG` + EIK).
- data.egov.bg was WAF-blocked from automated access in this environment (403).

See `source_inventory.json` for the machine-readable version.
