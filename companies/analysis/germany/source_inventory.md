# Germany — Source Inventory

| Source | Type | Access | Format | License | Status |
|---|---|---|---|---|---|
| **OffeneRegister.de** company bulk (JSONL) | Open data bulk | Free public | jsonl.bz2 | CC-BY 4.0 (NC per mirror — confirm) | **recommended** ✅ downloaded |
| OffeneRegister.de SQLite `handelsregister.db` | Open data bulk | Free public | sqlite | CC-BY 4.0 (confirm) | useful secondary (2022, not downloaded) |
| OpenSanctions `de_offeneregister` | Open data mirror | Free public | json/FTM | CC-BY-NC 4.0 | useful secondary (graph, not downloaded) |
| Handelsregister (handelsregister.de) | Official registry | Free view | html/pdf/xml per-doc | Free view; no bulk; terms restrict scraping | blocked (no bulk/API) |
| Unternehmensregister | Official registry | Free + paid docs | html/pdf/xml per-doc | Basic free; docs ~€1 | useful secondary (enrichment) |
| BRIS / EU e-Justice | Official EU aggregator | Free public | html | EU service; lookup only | useful secondary (verify EUID) |
| bundesAPI/handelsregister | Community scraper | Free, ≤60/hr | python cli | Subject to portal terms | useful secondary (targeted lookups) |
| Commercial APIs (handelsregister.ai, OpenRegister, Viaductus, Kausate, Implisense) | Commercial API | Paid | json + docs | Commercial | blocked by payment (fresh data option) |
| GovData.de | Open data portal | Free public | various (CKAN API) | Per dataset | useful secondary (regional/statistical) |

## Financial data sources (annual financial statements / Jahresabschluss)

| Source | Type | Access | Format | License | Status |
|---|---|---|---|---|---|
| **Unternehmensregister / Bundesanzeiger** (statements) | Official financial disclosure | Free **view** (no fee/registration since 2022) | XBRL / iXBRL / HTML / PDF | Free view; **no bulk/API**; reuse not granted | useful secondary (per-company) |
| **OpenRegister.de** Bundesanzeiger financial API | Commercial API | Paid | JSON | Commercial | blocked by payment (**structured financials at scale**) |
| `bundesanzeiger` (bundesAPI `deutschland`) | Community Python tool | Free, per-company, captcha-limited | dict / HTML | Apache-2.0 tool; portal terms apply | useful secondary (targeted enrichment) |

**Financial-data bottom line:** no open/bulk dataset and no free official retrieval API.
Structured financials at scale → **commercial API** (OpenRegister / North Data / …).
Free → per-company `bundesanzeiger` scraping (captcha/rate-limited) + XBRL/HTML parsing.

## Direct download URLs (open data)

- `https://daten.offeneregister.de/de_companies_ocdata.jsonl.bz2` — 260 MB, 2019 (**downloaded**)
- `https://daten.offeneregister.de/handelsregister.db` — 3.7 GB SQLite, 2022
- `https://daten.offeneregister.de/openregister.db.gz` — 773 MB, 2019
- `https://daten.offeneregister.de/de_companies_ocdata.jsonl.bz2.torrent`
- `https://daten.offeneregister.de/openregister.db.gz.torrent`
- `https://data.opensanctions.org/datasets/latest/de_offeneregister/entities.ftm.json` — ~6.4 GB

## Key portals (no bulk)

- Official register: https://www.handelsregister.de/
- Unternehmensregister: https://www.unternehmensregister.de/
- BRIS: https://e-justice.europa.eu/topics/registers-business-insolvency-land/business-registers-search-company-eu_en
- GovData: https://www.govdata.de/

See `source_inventory.json` for the machine-readable version with full fields.
