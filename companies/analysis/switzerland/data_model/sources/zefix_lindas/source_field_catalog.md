# Zefix via LINDAS (SPARQL) Field Catalog

## Source Summary

- Country: Switzerland
- Source type: official_registry
- Organization: EHRA (Federal Commercial Registry Office) / FOJ; published on LINDAS
- URL: https://lindas.admin.ch/query (SPARQL; `Accept: application/sparql-results+json`)
- License: OGD / Open use (attribution required)
- Access: public (no authentication)
- Freshness: daily
- Record shape: RDF graph; `admin:ZefixOrganisation` objects queried via SPARQL
- Primary keys: `uid`
- Join keys: `uid`, `chid`, `ehraid`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| ?company URI | company URI | EHRA-id URI | string | identifier | …/zefix/company/240345 | ehraid = tail |
| schema:legalName | legalName | Legal name | string | legal_name | Zazuko GmbH | multilingual |
| schema:identifier[CompanyUID].value | CompanyUID | UID | string | identifier | CHE105840918 | company id; VAT=UID+MWST |
| schema:identifier[CHID].value | CHID | Cantonal id | string | identifier | CH03640617915 | |
| schema:identifier[EHRAID] | EHRAID | Zefix id | string | identifier | 240345 | |
| schema:additionalType→name | additionalType | Legal form | string | legal_form | GmbH, AG, Verein | eCH-0097 |
| schema:address→streetAddress/postalCode/addressLocality | address | Registered address | string | address | Reitergasse 9, 8004 Zürich | |
| admin:municipality | municipality | Municipality | string | geography | …/municipality/371 | BFS id |
| schema:url / schema:description | url, description | Website, purpose | string | metadata | https://zazuko.com/ | |

## Interpretation Notes

- **The open backbone for Switzerland**: a single SPARQL endpoint (no auth)
  exposing the full Zefix register — **788,989** `ZefixOrganisation` entities.
  OGD/Open-use (attribute EHRA/Zefix).
- **Identifiers**: `CompanyUID` (CHE…) is the company id; **VAT = UID + ' MWST'
  (de) / ' TVA' (fr) / ' IVA' (it)**. `CHID` and `EHRAID` are alternate keys.
- **Legal form** is an eCH-0097 code (URI `…/legalforms/0106` = AG, `0107` = GmbH)
  with a multilingual label.
- **Multilingual literals** (de/fr/it) — filter by language when selecting labels;
  resolve `municipality` URIs to names.
- **What's NOT here**: per-company **activity code** (no NOGA in the open set),
  **status/dates** (LINDAS is essentially the active set; status/history via the
  credentialed REST/SOGC), **officers**, **financials**.
- `sample_record.json` is a real binding (Kaufmännischer Verband Schweiz,
  CHE-105.840.918), company-level only.
