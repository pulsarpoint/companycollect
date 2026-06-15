# Switzerland Company Data — Investigation

## Conclusion

Switzerland has **excellent open identity data** but **almost no public financial data**:

- **Identity**: the federal commercial-register index **Zefix** is published as **LINDAS Linked Data**
  and queryable via an **open SPARQL endpoint** (`https://lindas.admin.ch/query`, **no auth**) — the
  full register of **788,989** legal entities with UID, legal form, address, municipality, website,
  and purpose. The Zefix **REST API** (richer: SOGC publications, status) requires free **HTTP Basic**
  credentials.
- **Financials**: Switzerland has **no public filing obligation** for private companies. Under Art. 958
  of the Code of Obligations companies prepare annual accounts but do **not** disclose them publicly.
  Financials are public **only** for **listed companies** (SIX Swiss Exchange) and **regulated**
  entities (banks/insurers, FINMA). For the private majority, financials are **not obtainable** openly.

## Identifiers

- **UID** — *Unternehmens-Identifikationsnummer*, format `CHE-xxx.xxx.xxx` (digits `CHExxxxxxxxx` in
  linked data). The company id and universal key.
- **VAT number** = UID + ` MWST` (DE) / ` TVA` (FR) / ` IVA` (IT) when VAT-registered.
- **CHID** — cantonal commercial-register id (`CHxxxxxxxxxxx`).
- **EHRA-id** — Zefix internal entity id (the `…/zefix/company/{id}` URI).
- **eCH-0097** — the legal-form code list (e.g. 0106 = AG, 0107 = GmbH, "Verein" = association).

## Sources found

### 1. Zefix via LINDAS SPARQL (open) — RECOMMENDED
- Endpoint `https://lindas.admin.ch/query` (POST/GET SPARQL; `Accept:
  application/sparql-results+json`). **No authentication.** OGD / Open-use license (attribution).
- `admin:ZefixOrganisation` objects expose: `schema:legalName`/`schema:name`, `schema:identifier`
  (structured nodes for **UID** `CompanyUID`, **CHID**, **EHRAID**), `schema:additionalType` →
  eCH-0097 **legal form**, `schema:address` (streetAddress / postalCode / addressLocality),
  `admin:municipality`, `schema:url` (**website**), `schema:description` (**business purpose**),
  `rdfs:seeAlso` (Wikidata). Verified live: 788,989 entities; sample Zazuko GmbH (CHE-242.294.601),
  Kaufmännischer Verband Schweiz (CHE-105.840.918).
- Bulk via paged `SELECT … OFFSET/LIMIT`. (opendata.swiss lists this dataset as **LINDAS linked data +
  REST + web app**, not a flat CSV.)

### 2. Zefix Public REST API — free, BUT credentialed
- Base `https://www.zefix.admin.ch/ZefixPublicREST/api/v1/`. OpenAPI 2.7 confirms **global HTTP Basic
  security** (`Zefix-Credentials`) — every endpoint returns **HTTP 401** without credentials (verified).
- Endpoints: `POST /company/search`, `GET /company/uid/{id}`, `/company/ehraid/{id}`,
  `/company/chid/{id}`, `GET /sogc/{id}`, `/sogc/bydate/{date}` (gazette), `/legalForm`,
  `/registryOfCommerce`, `/community`. Adds SOGC (SHAB) publications and status/detail not in LINDAS.
- → **blocked_by_authentication** (credentials are free on request; not bypassed).

### 3. SOGC / SHAB — Swiss Official Gazette of Commerce
- Commercial-register events (incorporations, mutations, officers, dissolutions) are published in the
  SOGC, accessible via the Zefix REST `/sogc` endpoints (so also credentialed) and shab.ch. Useful as
  an event stream; same auth gate as the REST API.

### 4. SIX Swiss Exchange — listed-company financials (open, small population)
- Listed issuers publish annual/interim reports (often XBRL/PDF) via SIX and company IR pages. Covers
  only the few hundred listed companies. The only broadly-open route to Swiss financials.

### 5. Handelsregisterauszug / cantonal extracts — paid
- Certified register extracts and underlying documents are sold per company by the cantonal commercial
  registers (and via registry portals). → **blocked_by_payment**.

### 6. Commercial aggregators (Moneyhouse, Bisnode/Dun & Bradstreet, etc.) — paid
- Repackage Zefix/SOGC plus estimated financials. Restricted/paid; cross-check only.

## What was NOT bypassed

- The Zefix REST Basic-auth gate (401) was not circumvented; the open LINDAS SPARQL endpoint was used
  instead. Paid extracts and aggregators were not accessed. SPARQL queried politely (small LIMITs).

## Recommended ingestion

Page the **Zefix LINDAS SPARQL** endpoint to build the open identity master keyed on **UID**; add the
**Zefix REST** SOGC stream once free credentials are obtained. Treat **private-company financials as
unavailable**; obtain listed-company financials from **SIX**.
