# Switzerland — Search Attempts

## Attempt 1
- Date/time: 2026-06-15
- Source: WebSearch
- Query: `Zefix REST API Switzerland commercial register company data opendata.swiss bulk download license`
- Result: Zefix = federal commercial-register index; REST API + opendata.swiss + LINDAS linked data; OGD/open-use license, attribution.
- Decision: test the REST API and find the bulk/open route.

## Attempt 2
- Date/time: 2026-06-15
- Source: WebSearch
- Query: `Switzerland company financial statements public filing obligation private companies SIX listed annual report`
- Result: **no public filing obligation** for private companies (Art. 958 CO — prepared, not disclosed). Public only for listed (SIX) + banks/insurers.
- Decision: financials = unavailable for private companies; SIX for listed.

## Attempt 3
- Date/time: 2026-06-15
- Source: curl (Zefix REST)
- Query: `GET /api/v1/company/uid/CHE-105.909.036`; `POST /api/v1/company/search`
- Result: **HTTP 401** on all endpoints.
- Decision: inspect the OpenAPI spec for the auth scheme.

## Attempt 4
- Date/time: 2026-06-15
- Source: curl (OpenAPI) — `GET /ZefixPublicREST/v3/api-docs`
- Result: OpenAPI 2.7; **global HTTP Basic security** (`Zefix-Credentials`). Endpoints: company/search, company/uid|ehraid|chid, sogc, legalForm, registryOfCommerce, community. So the REST API needs (free) credentials.
- Decision: find the open route → opendata.swiss / LINDAS.

## Attempt 5
- Date/time: 2026-06-15
- Source: curl (opendata.swiss CKAN package_show, followed redirect)
- Result: Zefix dataset resources are **LINDAS linked data (SPARQL)** + REST + web app — no flat-file bulk. SPARQL endpoint `https://lindas.admin.ch/query`, no auth.
- Decision: query LINDAS directly.

## Attempt 6
- Date/time: 2026-06-15
- Source: curl (LINDAS SPARQL)
- Query: property exploration of `admin:ZefixOrganisation`
- Result: name, legalName, **UID/CHID/EHRAID** (schema:identifier nodes), additionalType (eCH-0097 legal form), address, municipality, **url** (website), **description** (purpose), Wikidata seeAlso. Open, no auth.
- Decision: Zefix LINDAS = recommended open identity source.

## Attempt 7
- Date/time: 2026-06-15
- Source: curl (LINDAS SPARQL)
- Query: structured SELECT (name, UID, legalForm, street, locality, postal) + COUNT
- Result: **788,989** ZefixOrganisations; verified records (Zazuko GmbH CHE-242.294.601; Kaufmännischer Verband Schweiz CHE-105.840.918). Built normalized sample.
- Decision: page SELECT with OFFSET/LIMIT for bulk identity.
