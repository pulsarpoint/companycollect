# Finland — Source Inventory

| Source | Type | Access | Format | License | Freshness | Status |
|---|---|---|---|---|---|---|
| **PRH Open Data — YTJ API v3** | Official registry API | Public, no auth | JSON | CC-BY-4.0 | Daily | **recommended** |
| avoindata.suomi.fi dataset `yritykset` | Open data catalog | Public (CKAN API) | JSON | CC-BY-4.0 | Daily | useful_secondary |
| avoindata.suomi.fi dataset `prh-avoin-data` | Open data catalog | Public (CKAN API) | JSON | CC-BY-4.0 | Daily | useful_secondary |
| PRH digital financial statements API | Official registry API | Public, no auth | JSON | CC-BY-4.0 | Daily | useful_secondary |
| Virre financial statements | Official register document service | Paid; user or contract client | PDF/document | PRH terms | On demand | paid_fallback |
| Legacy `full_prh_data.csv` dump | Portal resource | — | CSV | CC-BY-4.0 | was monthly | **unavailable (404)** |

## Primary endpoint

```
GET https://avoindata.prh.fi/opendata-ytj-api/v3/companies
```

| Parameter | Effect | Verified |
|---|---|---|
| `totalResults=true` | adds `totalResults` count to response | yes → 819,096 |
| `page=N` | page N of results, 100 records/page | yes |
| `businessId=NNNNNNN-N` | single company lookup by Y-tunnus | yes |
| `maxResults` | **ignored** — page size fixed at 100 | yes |

Full backfill ≈ `page=1 … 8191`.

## Key facts

- **Publisher:** Finnish Patent and Registration Office (PRH), jointly with the Tax
  Administration via the Business Information System (YTJ).
- **Auth:** none. **Cost:** free.
- **License:** Creative Commons Attribution 4.0 — attribution required, redistribution OK.
- **Excluded:** sole traders (*toiminimi*), email, phone, municipalities, wellbeing
  services counties, tax partnerships.
- **Portal note:** old `avoindata.fi` redirects to `avoindata.suomi.fi`; human pages
  403 to bots — use the CKAN action API for metadata.

## Financial statements

The YTJ company endpoint does not include financial statement figures. Financials
should be modeled as separate Finland sources:

- `finland/prhxbrl` — free PRH digital financial statement API. Public, CC-BY-4.0,
  structured IXBRL/XML. PRH states this covers only about 5% of all financial
  statements.
- `finland/virre_financial_statements` — paid Virre fallback for statement documents
  outside the digital API subset.

Current Virre prices checked on 2026-06-10:

| Product | Selling price |
|---|---:|
| Financial statements search | free |
| Electronic financial statement | EUR 4.02 / document |
| Electronic financial statement, contract client | EUR 2.01 / document |
| Contract client start-up fee | EUR 125.50 |
| Contract client annual fee per user name | EUR 27.61 |
| Contract client invoicing charge | EUR 8.16 |

See `financials_and_virre_pricing.md` for endpoints, pricing details, break-even
math, and implementation notes.

## Added 2026-07-19 — financial data gap follow-up

| Source | Type | Access | Format | License | Status |
|---|---|---|---|---|---|
| Verohallinto public corporate income tax data (2020–2024) | official tax open data | bulk CSV on vero.fi | CSV (latin-1, `;`) | CC-BY-4.0 | recommended |
| filings.xbrl.org ESEF index (country=FI) | official ESEF repository | free JSON:API + iXBRL zips | JSON / iXBRL | public regulated info (verify redistribution) | recommended |
| Nasdaq Helsinki OAM | official OAM | web only | xHTML/zip | public regulated info | useful_secondary_source |

Details and verified counts in `financial_data_gap_analysis.md`.
