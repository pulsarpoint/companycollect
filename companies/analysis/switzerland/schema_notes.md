# Switzerland — Schema Notes

## Identifiers

- **UID** — *Unternehmens-Identifikationsnummer*, `CHE-xxx.xxx.xxx` (stored as
  `CHExxxxxxxxx` in linked data). Company id and universal key.
- **VAT** = UID + ` MWST` (DE) / ` TVA` (FR) / ` IVA` (IT) for VAT-registered firms.
- **CHID** — cantonal commercial-register id (`CHxxxxxxxxxxx`).
- **EHRA-id** — Zefix internal id (the `…/zefix/company/{id}` URI tail).
- **eCH-0097** — legal-form code list (`https://ld.admin.ch/ech/97/legalforms/…`):
  e.g. `0106` = Aktiengesellschaft (AG), `0107` = GmbH; "Verein" = association.

## Zefix LINDAS — `admin:ZefixOrganisation` (SPARQL)

| Predicate | Meaning |
|---|---|
| schema:legalName / schema:name | Registered name |
| schema:identifier → {schema:name, schema:value} | Identifier nodes: `CompanyUID` (UID), `CHID`, `EHRAID` |
| schema:additionalType → schema:name | Legal form (eCH-0097; label per language) |
| schema:address → schema:streetAddress / schema:postalCode / schema:addressLocality | Registered address |
| admin:municipality | Municipality (BFS id → name) |
| schema:url | Website |
| schema:description | Business purpose (Zweck) |
| rdfs:seeAlso | Wikidata link (where present) |

- Endpoint: `https://lindas.admin.ch/query` (SPARQL; `Accept:
  application/sparql-results+json`). No auth. **788,989** entities.
- Multilingual literals (de/fr/it) — filter by language when selecting labels.
- Bulk via paged `SELECT … ORDER BY ?company OFFSET n LIMIT m`.

## Zefix REST API (credentialed) — adds

- `GET /company/uid|ehraid|chid/{id}` — full company object incl. **status**
  (active/deleted), register seat, capital, purpose, SOGC refs.
- `GET /sogc/{id}` & `/sogc/bydate/{YYYY-MM-DD}` — gazette events (incorporations,
  mutations, **officers**, dissolutions).
- `GET /legalForm`, `/registryOfCommerce`, `/community` — classifiers.

## Mapping to internal model

| Internal | Switzerland source |
|---|---|
| company_id | UID |
| registration_number | UID (or CHID) |
| tax_id | UID |
| vat_id | UID + " MWST"/"TVA"/"IVA" (if VAT-registered) |
| legal_name | Zefix legalName |
| legal_form | Zefix additionalType (eCH-0097) |
| status | Zefix REST status (LINDAS = active set); credentialed |
| incorporation_date | SOGC / REST (credentialed) — not in LINDAS core |
| dissolution_date | SOGC / REST (credentialed) |
| registered_address | Zefix address (street/postal/locality) |
| municipality | Zefix municipality |
| activity_code | **not available** (Switzerland's register has no NOGA code per company in the open set) |
| financials | **not available** for private companies (no public filing); listed via SIX |
| officers | SOGC / paid extract — not in LINDAS |
| owners | **not available** (no public beneficial-ownership register) |

## Gotchas

- The Zefix **REST** API needs free Basic-auth credentials (401 otherwise); use
  **LINDAS SPARQL** for open bulk.
- **No per-company activity code** and **no financials** in the open data; both are
  structural gaps for Switzerland.
- Person data (officers) appears only in SOGC/paid extracts — handle per FADP/GDPR.
