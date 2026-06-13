# Sweden — license & terms notes

## Big picture

Sweden's company data became free under the **EU Open Data Directive (2019/1024)** and its
**high-value-datasets** implementing regulation (EU 2023/138), category *"Companies and company
ownership"*. Sweden implemented this so that **Bolagsverket** and **SCB** must provide the core company
datasets **free of charge, via API and bulk**. Launch: **26 June 2025** ("Värdefulla datamängder").

## SCB — Företagsregistret / FDB free API

- **License: CC0 1.0** (public-domain dedication). SCB states data may be used, distributed and made
  available **without any requirement to cite the source**. (Attribution still courteous.)
- Fee removal followed a **government decision amending the relevant ordinance**, effective
  **26 June 2025**.
- Operational terms: you must **accept the API terms of use** and obtain a **certificate + password**
  (today) — migrating to an **API-key model in September 2026**. Limits: 2,000 rows/request, 10 req/10 s.
- **Status: clear and permissive.** Safe for ingestion and redistribution.

## Bolagsverket — Värdefulla datamängder API

- **Free of charge, no contract.** Provided under the EU high-value-datasets obligation
  ("it should be free for everyone to use" per the EU Commission directive, as stated by Bolagsverket).
- Access is **OAuth2-gated** (free `client_id`/`client_secret` via self-service Kundanmälan) — the
  authentication is for **rate-management/identification**, not payment.
- **Reuse terms: confirm per dataset.** The directive mandates open reuse, and the national catalog
  (dataportal.se) carries the formal license string. Treat as **open/free-reuse**, but before
  large-scale **redistribution** of the raw payloads (esp. annual-report documents), record the exact
  license shown on `dataportal.se/datasets/612_5428` and the API terms page.
- Annual reports (iXBRL) are **public filings**; their data is reusable, but the **document files**
  may carry specific reuse wording — verify.

## Paid / restricted (NOT for open ingestion)

- **Bolagsverket XML bulk packet & legacy paid API** — commercial agreement + fee (~SEK 6,250
  onboarding + usage). Superseded for open use by the free VDM API. Do not use unless a paid contract
  is explicitly authorised.
- **Verklig huvudman (UBO) register** — not part of the free open-API set; restricted. Out of scope.
- **Commercial aggregators** (allabolag, bolagsapi.se, apiverket.se, foretagsapi.se, OpenCorporates,
  Apify) — governed by **each vendor's terms**; many repackage the same official data. Use the official
  free APIs instead for primary ingestion.

## Uncertainty / to confirm

- [ ] Exact formal license string for the Bolagsverket VDM datasets on dataportal.se (CC0 vs custom open).
- [ ] Any attribution/redistribution clause on the **annual-report document** payloads specifically.
- [ ] SCB API-key terms once the Sept 2026 model is published (expected same CC0 data license).
