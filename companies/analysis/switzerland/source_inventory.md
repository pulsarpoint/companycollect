# Switzerland — Source Inventory

| Source | Slug | Type | Access | License | Format | Status |
|---|---|---|---|---|---|---|
| Zefix via LINDAS (SPARQL) | zefix_lindas | official_registry | public | OGD / Open use | sparql-json/rdf | recommended |
| Zefix Public REST API | zefix_rest_api | official_registry | restricted (free creds) | OGD / Open use | json | blocked_by_authentication |
| SOGC / SHAB gazette | sogc_shab | official_gazette | restricted (via REST) | OGD / Open use | json/html | useful_secondary_source |
| SIX listed-company financials | six_listed_financials | stock_exchange | public | issuer publications | xbrl/pdf | useful_secondary_source |
| Handelsregister extracts | handelsregister_extract | official_registry | paid | paid | pdf | blocked_by_payment |

## Best source

**Zefix via LINDAS SPARQL** (open, no auth) — the full register of **788,989**
entities keyed on **UID**. The REST API (free Basic-auth credentials) adds SOGC
events + status. **Financials are not openly available for private companies** —
only listed issuers (SIX) and regulated entities publish.

## Downloaded (real)

- `raw/api/lindas_props.json` — property exploration of `admin:ZefixOrganisation`
- `raw/api/lindas_sample.json` — structured SELECT (name, UID, legal form, address)
- `raw/api/zefix_openapi.json` — Zefix REST OpenAPI (confirms Basic-auth gate)
- `raw/api/zefix_ckan.json` — opendata.swiss CKAN package (LINDAS resources)
- `raw/samples/lindas_company_sample.json` — one real company binding
- `normalized/companies.sample.jsonl` — 5 real normalized records (UID + identity)
