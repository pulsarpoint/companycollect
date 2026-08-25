# Serbia — Search Attempts Log

All attempts on 2026-06-06.

## Attempt 1
- Source: WebSearch
- Query: `Serbia business register APR open data company bulk download API`
- Language: English
- Why: Identify the official registry and any open data / API.
- Top URLs: opencorporates.com/registers/224, org-id.guide/list/RS-APR,
  apr.gov.rs, serbia-business.eu (APR portal announcement).
- Result: Confirmed APR is the official registry. One result claimed bulk
  CSV/XML at `data.apr.gov.rs` — flagged for verification.
- Decision: Verify APR open data location directly.

## Attempt 2
- Source: WebSearch
- Query: `Agencija za privredne registre Srbija otvoreni podaci preuzimanje API privredni subjekti`
- Language: Serbian (Latin)
- Why: Local-language terms for the registry and data delivery.
- Top URLs: apr.gov.rs search pages, the "Automatizovano izdavanje podataka
  (veb-servis)" service page.
- Result: Found APR's web-service for automated data delivery — free for state
  bodies, paid for banks/businesses; covers all status registers + beneficial
  owners. Contact apr-podaci@apr.gov.rs.
- Decision: Note web-service as secondary (paid) source; keep hunting for open data.

## Attempt 3
- Source: WebSearch
- Query: `data.gov.rs privredni subjekti companies dataset CSV`
- Language: mixed
- Why: Locate company datasets on the national open data portal.
- Top URLs: data.gov.rs, opendata.stat.gov.rs, data.stat.gov.rs.
- Result: Confirmed `data.gov.rs` exists (udata/etalab platform) but no direct
  dataset link surfaced.
- Decision: Query the portal API directly.

## Attempt 4
- Source: WebFetch `https://data.gov.rs/sr/datasets/?q=privredni+subjekti` and
  `https://data.apr.gov.rs/` and `https://opencorporates.com/registers/224`
- Result: data.gov.rs returned only homepage shell; `data.apr.gov.rs` →
  **ECONNREFUSED**; OpenCorporates confirmed register 224, search at
  pretraga2.apr.gov.rs, "0/20 for freely available data" (i.e. no open bulk
  *historically*).
- Decision: Use the portal's machine API; verify data.apr.gov.rs via DNS.

## Attempt 5
- Source: curl — `data.gov.rs` udata API `/api/1/datasets/?q=...` (Latin terms),
  DNS lookups for `data.apr.gov.rs` and `opendata.apr.gov.rs`.
- Result: Latin queries returned 0 (portal content is Cyrillic). **DNS: both APR
  data subdomains return NXDOMAIN** — the earlier "data.apr.gov.rs CSV bulk"
  claim was false.
- Decision: Re-query the portal API in Cyrillic.

## Attempt 6
- Source: curl udata API, Cyrillic queries: `привредни`, `предузетници`,
  `привредна друштва`, `компаније`, `APR`, `регистар`, `пословни`.
- Result: Hits — **«АПИ за Регистар привредних друштава»**, «АПИ за Регистар
  финансијских извештаја», «АПИ за Регистар задужбина и фондација…», all
  published by Агенција за привредне регистре.
- Decision: Fetch full dataset records to extract real API endpoints.

## Attempt 7
- Source: curl udata API `/api/1/datasets/{id}/` for the 3 APR datasets.
- Result: Real OpenAPI endpoints discovered:
  `https://openapi.apr.gov.rs/api/opendata/companies`,
  `.../companies/financial-statements`, `.../ngo`. License `public_domain`.
- Decision: Test and download the endpoints.

## Attempt 8
- Source: curl `https://openapi.apr.gov.rs/api/opendata/companies`
- Result: HTTP 200, 57.5 MB JSON, 133,357 companies, snapshot 2026-05-31, keyed
  by matični broj. HEAD allows GET, OPTIONS.
- Decision: Download all three endpoints in full with metadata + SHA-256.

## Attempt 9
- Source: curl — downloaded financial-statements (123,455) and ngo (40,547);
  listed all datasets for APR org id 678e217c0aae3fe3ad3e361b.
- Result: APR publishes exactly 4 datasets (3 = the open APIs above + one NGO
  thematic list). **No entrepreneurs (preduzetnici) open dataset.**
- Decision: Conclude — companies API is the primary source; preduzetnici only via
  paid web-service.

## Attempt 10
- Source: WebSearch `APR Serbia preduzetnici register open data API ...` +
  WebFetch of the companies dataset catalog page.
- Result: Confirmed APR registers preduzetniks but open data is companies-only;
  catalog page confirms **monthly** update frequency and **public data** license.
- Decision: Investigation complete.

## Attempt 11
- Date/time: 2026-08-24
- Search engine or source: Direct GET of the user-supplied APR endpoint
- Query: `https://openapi.apr.gov.rs/api/opendata/companies`
- Language: Serbian source data
- Why this query was tried: Verify that the endpoint is live and determine the
  current payload shape and snapshot.
- Top relevant URLs:
  `https://openapi.apr.gov.rs/api/opendata/companies`
- Result: HTTP 200; snapshot `2026-07-31`; 133,634 records; 57,673,691 bytes.
- Decision: Keep as the recommended company-core source.

## Attempt 12
- Date/time: 2026-08-24
- Search engine or source: Local `jq` schema enumeration on the saved full JSON
- Query: Unique keys across every object under `Podaci`
- Language: Serbian field names
- Why this query was tried: Test specifically for representatives/directors
  rather than relying on a sample record.
- Top relevant URLs:
  `https://openapi.apr.gov.rs/api/opendata/companies`
- Result: Exactly seven fields were present: `PoslovnoIme`, `SifraOpstine`,
  `NazivOpstine`, `NazivStatus`, `DatumOsnivanja`, `NazivPravneForme`,
  and `SifraDelatnosti`. No representative fields.
- Decision: The open feed cannot meet the representative requirement.

## Attempt 13
- Date/time: 2026-08-24
- Search engine or source: Official data.gov.rs dataset page and catalog API
- Query: APR company dataset id `68000c424d29e8a004f93e04`
- Language: Serbian
- Why this query was tried: Verify publisher, license, update cadence and the
  official link to the supplied endpoint.
- Top relevant URLs:
  `https://data.gov.rs/sr/datasets/api-za-registar-privrednikh-drushtava/`
- Result: Publisher APR; resource is the supplied JSON endpoint; current license
  id is `sodl` (Serbian Open Data License), not `public_domain`; description
  says monthly updates.
- Decision: Correct the stale license classification in the workspace.

## Attempt 14
- Date/time: 2026-08-24
- Search engine or source: Official APR status-data page and 2026 data-set PDF
- Query: `zakonski zastupnici`, `ostali zastupnici`, `prokuristi`,
  `setovi podataka`
- Language: Serbian (Latin and Cyrillic)
- Why this query was tried: Identify an authorized way to obtain company
  representatives and its cost structure.
- Top relevant URLs:
  `https://www.apr.gov.rs/usluge/epodaci-na-zahtev-korisnika/statusni-i-drugi-poslovni-podaci.2410.html`;
  `https://www.apr.gov.rs/upload/Portals/0/OIP/2026/Razvrstavanje_grupa_podataka_u_setove_podataka.pdf`
- Result: Legal representatives are SP3; other representatives, boards and
  procurists are SP4; SP2 is mandatory. Published prices are 25 RSD/entity for
  SP2 and 5 RSD/entity for each add-on.
- Decision: Recommend SP2+SP3 for backfill, with SP4 when broader officer
  coverage is needed.

## Attempt 15
- Date/time: 2026-08-24
- Search engine or source: Official APR automated-data-delivery page
- Query: `Automatizovano izdavanje podataka veb-servis`
- Language: Serbian
- Why this query was tried: Find an authorized incremental update path.
- Top relevant URLs:
  `https://www.apr.gov.rs/usluge/epodaci-na-zahtev-korisnika/automatizovano-izdavanje-podataka-%28veb-servis%29.2413.html`
- Result: Contracted service covers public status-register groups and supports
  time-period changes plus selected groups by `matični broj`; business users
  pay, while public technical schema/auth details are absent.
- Decision: Recommend this service for daily representative changes after APR
  supplies technical documentation and a contract.

## Attempt 16
- Date/time: 2026-08-24
- Search engine or source: Manual inspection of APR public search and APR terms
- Query: `https://pretraga.apr.gov.rs/search`
- Language: Serbian
- Why this query was tried: Determine whether the public UI can be an automated
  representative source.
- Top relevant URLs:
  `https://pretraga.apr.gov.rs/search`;
  `https://apr.gov.rs/dokumenti.1432.html`
- Result: The UI uses reCAPTCHA. APR says unauthorized scripts/applications and
  automated tools against search results are prohibited and may be blocked.
- Decision: Do not scrape. Use the public search only for human spot checks.
