# Sweden — source inventory (summary)

| # | Source | Type | Access | Formats | License | Financials | Status |
|---|--------|------|--------|---------|---------|-----------|--------|
| 1 | **Bolagsverket — Värdefulla datamängder API v1** | Official registry API | OAuth2 client_credentials (free creds via Kundanmälan) | JSON, iXBRL, ZIP | Free / EU high-value datasets | **Yes — annual reports (iXBRL)** | **recommended (primary)** |
| 2 | **SCB — Företagsregistret / FDB free API** | Statistical business register API | Client cert (→ API key Sept 2026), free | JSON, XML | **CC0** | No | **recommended (secondary/seed)** |
| 3 | dataportal.se | National DCAT catalog | Open | DCAT, JSON | metadata | — | useful_secondary_source |
| 4 | Bolagsverket paid XML packet / legacy API | Official registry (paid) | Agreement + fee | XML, CSV | commercial | partial | blocked_by_payment |
| 5 | Verklig huvudman (UBO) | Official registry | Controlled | — | restricted | No | blocked_by_license_uncertainty |
| 6 | Aggregators (allabolag, bolagsapi.se, apiverket.se, foretagsapi.se, OpenCorporates, Apify) | Third-party | Paid/keyed | JSON, PDF, iXBRL | vendor | resold | useful_secondary_source |

## Key endpoints (primary source)

```
Base:  https://gw.api.bolagsverket.se/vardefulla-datamangder/v1
Auth:  OAuth2 client_credentials, scope vardefulla-datamangder:read  (WSO2 gateway)
GET  /isalive
POST /organisationer        -> company base data by organisationsnummer (JSON)
POST /dokumentlista         -> list annual-report documents for an org
GET  /dokument/{id}         -> ZIP containing iXBRL annual report (financial data)
```

## One-line recommendation

Use **Bolagsverket Värdefulla datamängder** as the primary company + **financial** source (free OAuth2,
iXBRL annual reports), and **SCB FDB free API** (CC0) to seed/complete the company + workplace universe.
Both became free on **26 June 2025** under the EU Open Data Directive high-value-datasets rule.
