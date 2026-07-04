# Search attempts

## Attempt 1

- Date/time: 2026-07-03T22:42:43Z
- Source: Web search
- Query: `site:datacvr.virk.dk API CVR search Virk`
- Language: English/Danish mixed
- Why: Find official public endpoints and API docs.
- Relevant URLs:
  - `https://datacvr.virk.dk/`
  - `https://datacvr.virk.dk/data/cvr-help/cvr-api`
- Result: Confirmed public site and help pages, but HTML is a JavaScript app.
- Decision: Inspect endpoint references and official documentation.

## Attempt 2

- Date/time: 2026-07-03T22:42:43Z
- Source: Direct HTTP
- Query: `https://datacvr.virk.dk/robots.txt`
- Language: n/a
- Why: Determine crawling rules.
- Relevant URLs:
  - `https://datacvr.virk.dk/robots.txt`
- Result: `Crawl-delay: 10`; multiple disallowed paths including `/search/`.
- Decision: Any no-auth crawl must be single-host throttled and avoid disallowed paths.

## Attempt 3

- Date/time: 2026-07-03T22:42:43Z
- Source: Direct HTTP
- Query: `https://datacvr.virk.dk/data/visninger?soeg=30714024&type=Alle&language=da`
- Language: Danish
- Why: Test public search endpoint.
- Relevant URLs:
  - `https://datacvr.virk.dk/data/visninger?soeg=30714024&type=Alle&language=da`
- Result: HTTP 403 Cloudflare managed challenge from this environment.
- Decision: Do not attempt to bypass. Treat public endpoint as browser-oriented and fragile for automation.

## Attempt 4

- Date/time: 2026-07-03T22:42:43Z
- Source: Direct HTTP
- Query: `https://datacvr.virk.dk/data/visenhed?enhedstype=virksomhed&id=30714024&language=da`
- Language: Danish
- Why: Test public detail endpoint.
- Relevant URLs:
  - `https://datacvr.virk.dk/data/visenhed?enhedstype=virksomhed&id=30714024&language=da`
- Result: HTTP 403 Cloudflare managed challenge from this environment.
- Decision: Detail lookups can be part of a browser-based manual/sparse flow, but not a reliable bulk crawler.

## Attempt 5

- Date/time: 2026-07-03T22:42:43Z
- Source: Official Erhvervsstyrelsen documentation
- Query: `Kom godt igang med Elasticsearch CVR`
- Language: Danish
- Why: Verify official API, pagination, and bulk-like access route.
- Relevant URLs:
  - `https://erhvervsstyrelsen.dk/kom-godt-igang-med-elasticSearch`
- Result: Official API supports Query DSL, `_source`, and scroll. Requires issued username/password.
- Decision: Recommended for systematic extraction only if credentials become available.

## Attempt 6

- Date/time: 2026-07-03T22:42:43Z
- Source: Wikidata property metadata
- Query: `CVR number P1059 formatter URL`
- Language: English
- Why: Cross-check public formatter/search URL shapes.
- Relevant URLs:
  - `https://www.wikidata.org/wiki/Property:P1059`
- Result: Confirms public formatter and search formatter URL shapes for DataCVR.
- Decision: Use endpoint shapes for lookup-only recommendation, not broad crawling.

## Attempt 7

- Date/time: 2026-07-03T22:55:09Z
- Source: CVR API documentation
- Query: `https://cvrapi.dk/documentation`
- Language: Danish
- Why: Check third-party no-auth API mentioned by user.
- Relevant URLs:
  - `https://cvrapi.dk/`
  - `https://cvrapi.dk/documentation`
  - `https://cvrapi.dk/terms`
  - `https://cvrapi.dk/help`
- Result: Found simple GET/POST API at `https://cvrapi.dk/api` with required `search` and `country`, JSON/XML output, 50 free lookups/day, descriptive User-Agent requirement, quota and ban errors, and published terms.
- Decision: Add as preferred no-auth low-volume lookup fallback, below official credentialed API.

## Attempt 8

- Date/time: 2026-07-03T22:55:09Z
- Source: Direct HTTP sample
- Query: `https://cvrapi.dk/api?search=30714024&country=dk`
- Language: n/a
- Why: Test whether a bounded sample lookup works from this environment.
- Relevant URLs:
  - `https://cvrapi.dk/api?search=30714024&country=dk`
- Result: HTTP 403 with empty body, using a descriptive User-Agent.
- Decision: Record sample failure and require collector to stop on 403 rather than retry aggressively.
