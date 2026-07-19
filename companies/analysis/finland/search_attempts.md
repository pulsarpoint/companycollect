# Finland — Search Attempts Log

## Attempt 1

- Date/time: 2026-06-06T13:41Z
- Search engine or source: WebSearch
- Query: `Finland company register API PRH Business Information System open data`
- Language: English
- Why: locate the official national company registry and its API.
- Top relevant URLs:
  - https://avoindata.prh.fi/en  (PRH Open Data)
  - https://www.prh.fi/en/companiesandorganisations/tietopalvelut/prhopendata.html
  - https://avoindata.suomi.fi/data/en_GB/dataset/prh-avoin-data
  - https://www.ytj.fi/en/index/opendata.html
- Result: Identified PRH as the official registry; open data via avoindata.prh.fi; daily updates; no email/phone.
- Decision: Treat PRH Open Data as primary candidate; dig into the API.

## Attempt 2

- Date/time: 2026-06-06T13:41Z
- Search engine or source: WebSearch
- Query: `avoindata.fi yritykset open data company register bulk download`
- Language: English
- Why: find a bulk (CSV/JSON) download of the full register.
- Top relevant URLs:
  - https://www.avoindata.fi/data/en_GB/dataset/yritykset
  - https://www.avoindata.fi/data/en_GB/dataset/yritykset/resource/ac409ad1-3183-4174-9317-a13c16511ab1 (full_prh_data.csv)
  - https://avoindata.prh.fi/en/ytj/swagger-ui
- Result: Found references to a monthly `full_prh_data.csv` and to fetching all companies as JSON via the API/Swagger.
- Decision: Verify both the CSV resource and the API directly.

## Attempt 3

- Date/time: 2026-06-06T13:41Z
- Search engine or source: WebSearch
- Query: `Finland kaupparekisteri avoin data API yritystietojärjestelmä YTJ` (local language)
- Language: Finnish
- Why: confirm local terminology and find Finnish-language portal pages.
- Top relevant URLs:
  - https://avoindata.prh.fi/fi , https://avoindata.prh.fi/ytj.html
  - https://avoindata.prh.fi/en/ytj/swagger-ui
  - https://avoindata.suomi.fi/data/fi/dataset/yritykset
- Result: Confirmed YTJ = joint PRH + Tax Administration system; API returns all registered + pending companies as JSON; daily; sole traders excluded.
- Decision: Test the v3 API endpoints directly with curl.

## Attempt 4 (direct API verification)

- Date/time: 2026-06-06T13:41Z
- Source: curl against `https://avoindata.prh.fi/opendata-ytj-api/v3/companies`
- Why: confirm the API is live, key-less, and inspect schema + pagination.
- Result:
  - `?totalResults=true&maxResults=1` → HTTP 200, `totalResults: 819096`, returned 100 companies (page size fixed at 100).
  - `?businessId=0100002-9` → HTTP 200, full single record (2 KB).
  - `?page=2` → HTTP 200, next 100 companies.
  - `/v3/api-docs`, `/v3/swagger-config` → HTTP 404 (raw OpenAPI not exposed at those paths).
- Decision: API is the canonical bulk + lookup path. Saved samples to `raw/api/`.

## Attempt 5 (portal metadata + bulk CSV check)

- Date/time: 2026-06-06T13:41Z
- Source: avoindata.suomi.fi CKAN action API
- Why: read machine-readable dataset/resource metadata; confirm CSV bulk existence and license.
- Result:
  - `package_show?id=yritykset` → HTTP 200; license **CC-BY-4.0**; describes daily JSON via API.
  - `package_show?id=prh-avoin-data` → HTTP 200; license **CC-BY-4.0**; resource = PRH API.
  - `resource_show?id=ac409ad1-...` (legacy full_prh_data.csv) → **HTTP 404** (removed).
  - Human portal pages (`/data/en_GB/...`) return HTTP 403 to fetchers; use CKAN API instead.
- Decision: Drop the legacy CSV; standardize on the PRH JSON API. License confirmed CC-BY-4.0.

## Attempt 6 (financial coverage gap follow-up: tax administration open data)

- Date/time: 2026-07-19T15:00Z
- Search engine or source: web search + avoindata.suomi.fi CKAN API + vero.fi
- Query: `Verohallinto yhteisöjen tuloverotuksen julkiset tiedot CSV avoindata corporate income tax public data Finland download`
- Language: Finnish + English
- Why this query was tried: PRH digital XBRL covers only ~5% of statements; looking for a universal-coverage financial signal for all Finnish corporate entities.
- Top relevant URLs:
  - https://www.vero.fi/tietoa-verohallinnosta/tilastot/avoin_dat/
  - https://avoindata.suomi.fi/data/fi/dataset/yhteisojen-tuloverotuksen-julkiset-tiedot
- Result: Confirmed annual CSV bulk files (2020-2024 on vero.fi, 2011-2014 on CKAN; CKAN dataset is stale for recent years). License CC-BY-4.0. Downloaded 2024 file: 384,627 rows, 8 columns (tax year, business ID, name, municipality, taxable income, taxes assessed, refund, residual tax). Latin-1, semicolon-delimited, decimal comma. Also found a tax-amendments CSV (2022-2024 corrections).
- Decision: Recommend as new source `finland/verotax`. Overlap with `fi_companies`: 274,355 of 460,988 (60%); 124,650 rows with taxable income > 0.

## Attempt 7 (listed-company financial statements: ESEF)

- Date/time: 2026-07-19T15:05Z
- Search engine or source: web search + filings.xbrl.org JSON:API
- Query: `Finland ESEF financial statements listed companies XBRL filings download Finanssivalvonta`
- Language: English
- Why this query was tried: ClickHouse showed 0 of 298 public limited companies have financials via PRH XBRL; listed issuers file ESEF annual reports to the Nasdaq Helsinki OAM instead.
- Top relevant URLs:
  - https://www.finanssivalvonta.fi/en/capital-markets/issuers-and-investors/esef-xbrl/
  - https://filings.xbrl.org/api/filings?filter=[{"name":"country","op":"eq","val":"FI"}]
- Result: filings.xbrl.org has 1,168 Finnish ESEF filings (FY2020 onward) with free API and direct zip package URLs. Entities identified by LEI (join to Y-tunnus via GLEIF). Nasdaq OAM itself has no obvious bulk interface; filings.xbrl.org is the practical machine-readable path.
- Decision: Recommend as new source `finland/esef` for listed-company IFRS consolidated financials; reuse the existing `gleif` module for LEI -> business ID mapping.

## Attempt 8 (future coverage: mandatory structured filing)

- Date/time: 2026-07-19T15:10Z
- Search engine or source: web search + xbrl.org news
- Query: `Finland mandatory XBRL reporting company accounts trade register 2027`
- Language: English
- Why this query was tried: assess whether the 5% digital-filing limitation of the current `finland_xbrl` source is permanent.
- Top relevant URLs:
  - https://www.xbrl.org/news/finland-moves-to-mandatory-xbrl-reporting-for-company-accounts/
- Result: iXBRL filing to PRH becomes mandatory in 2027 for companies required to appoint an auditor, expanding 2028 to most limited companies and partnerships; PRH targets all trade-register accounts in structured form by 2028.
- Decision: No new pipeline needed for this; the existing `finland_xbrl` source will organically approach full coverage in 2027-2028. Keep Virre as the only paid backfill option for pre-2027 non-digital filers.

## Attempt 9 (public procurement: Hilma AVP API)

- Date/time: 2026-07-19T18:30Z
- Search engine or source: web search + Hilma developer portal + GitHub Hankintailmoitukset/hilma-api
- Query: `Hilma hankintailmoitukset.fi open data API public procurement Finland winners suppliers`
- Language: English/Finnish
- Why this query was tried: connect Finnish companies to public-contract awards (revenue signal, government-customer graph).
- Top relevant URLs:
  - https://hns-hilma-prod-apim.developer.azure-api.net/
  - https://github.com/Hankintailmoitukset/hilma-api (endpoints/avpendpoints.md)
- Result: AVP (read) API is free, commercial use allowed, ~18-20k notices/year incl. contract award notices. BUT requires an `Ocp-Apim-Subscription-Key` obtained by self-registering on the developer portal (immediate for the avp-read product). Notices delivered as Base64 eForms XML; `/eform-search` (Azure Search syntax) + batch read endpoints. Rate-limited per key.
- Decision: classify `blocked_by_authentication` (free registration) until a key exists. Registration is an account action for the data owner, not the agent.

## Attempt 10 (public procurement: TED keyless alternative)

- Date/time: 2026-07-19T18:40Z
- Search engine or source: direct probe of TED v3 API
- Query: POST https://api.ted.europa.eu/v3/notices/search with `place-of-performance IN (FIN) AND notice-type IN (can-standard)`
- Language: English
- Why this query was tried: EU-threshold Finnish notices are mirrored to TED, whose API might not need a key.
- Result: Works with NO authentication. 37,946 Finnish contract-award notices, winner names in the search response, structured winner org identifiers (incl. national registration numbers) in the per-notice eForms XML. Cross-country by construction (any `place-of-performance`).
- Decision: `recommended` as the first procurement source (EU-threshold, all countries); Hilma remains the add-on for Finnish national below-threshold notices once a key exists. Sample saved to `raw/api/ted_fi_award_notices_sample.json`.
