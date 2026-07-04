# Denmark CVR crawl investigation

Investigation date: 2026-07-03

## Source

Denmark's official company register is the Central Business Register (CVR), published through CVR.dk/DataCVR and maintained by Erhvervsstyrelsen, the Danish Business Authority.

The public web application at `https://datacvr.virk.dk/` is a JavaScript application. Direct non-browser requests from this environment returned a Cloudflare challenge page with HTTP 403 and `cf-mitigated: challenge`, including for the public search/detail endpoints.

## Public UI endpoints observed

Public references and indexed metadata identify these URL shapes:

- Search: `https://datacvr.virk.dk/data/visninger?soeg={query}&type=Alle`
- Detail: `https://datacvr.virk.dk/data/visenhed?enhedstype=virksomhed&id={cvr}`
- Human detail route: `https://datacvr.virk.dk/enhed/virksomhed/{cvr}`

These are suitable only for bounded lookup flows where we already have a company name or CVR number. They are not suitable for full-registry discovery.

## Robots and anti-abuse constraints

`https://datacvr.virk.dk/robots.txt` declares `Crawl-delay: 10` for all user agents and disallows several paths, including `/search/`, Drupal internals, and several product/cart/document publication paths. The observed Cloudflare managed challenge is a strong signal that automated access should be very conservative and should not attempt to bypass access controls.

## Official API

Erhvervsstyrelsen documents an Elasticsearch-style API at `distribution.virk.dk/cvr-permanent`. It supports `_search`, Query DSL, scroll search, and source-field filtering. The documentation explicitly assumes issued credentials. It is the correct technical route for bulk-like extraction and incremental updates, but it is unavailable under the user's stated no-auth constraint.

Important official guidance from the Elasticsearch documentation:

- Prefer Query DSL and restrict `_source` fields.
- Use `size` to limit responses.
- For local copies, use scroll with `scroll=1m` and small batches.
- Reduce batch size if processing cannot keep up.
- Clear scroll contexts when done.
- The authority reserves the right to block access if scroll guidance is not followed.

## Third-party API: cvrapi.dk

`https://cvrapi.dk/` is a third-party API for lookup in the Danish and Norwegian company registers. It exposes:

- Base URL: `https://cvrapi.dk/api`
- Methods: GET or POST
- Required parameters: `search`, `country`
- Countries: `dk`, `no`
- Optional output format: `json`, `xml`
- Optional token parameter for issued tokens
- Search options: CVR/VAT number, company name, production unit, phone

The documentation states that callers must use a descriptive User-Agent containing company/project/contact information. It also documents a free limit of 50 lookups per day and error responses including `QUOTA_EXCEEDED`, `BANNED`, `INVALID_VAT`, `NOT_FOUND`, `INTERNAL_ERROR`, and `INVALID_UA`.

Terms published from 2019 allow copying, distribution, publication, modification, combination with other material, and commercial/non-commercial use, but they also prohibit charging for separate features such as name search or for showing information received from CVR API. They prohibit bypassing the daily limit by rotating IPs or obscuring the User-Agent. They also include specific restrictions for advertising-protected companies.

A single sample lookup from this environment to `https://cvrapi.dk/api?search=30714024&country=dk` returned HTTP 403 with an empty body. This may be environment/IP related, but the collector should treat it as a hard stop, not a retryable transient failure.

## Recommended no-auth crawl strategy

Use a seeded lookup collector, not a broad crawler:

1. Input must be a known CVR number list, or a user-supplied company-name list.
2. First try `cvrapi.dk` for exact CVR numbers if the expected volume is within quota and its terms fit the use case.
3. Use DataCVR public detail lookup only as a fallback for exact CVR numbers.
4. Cache by normalized query and by CVR number.
5. For `cvrapi.dk`, keep well under 50 free lookups/day unless a token or commercial arrangement is obtained.
6. For DataCVR, enforce a global per-host delay of at least 10 seconds, preferably 15-30 seconds with jitter.
7. Run single concurrency per host.
8. Stop automatically on `QUOTA_EXCEEDED`, `BANNED`, `INVALID_UA`, HTTP 403, 429, 503, Cloudflare challenge HTML, or unexpected consent/login/challenge pages.
9. Persist raw HTML/JSON response metadata, status code, content hash, and retrieval time.
10. Do not scrape beneficial ownership data or login-gated data.
11. Do not attempt ID-space enumeration of 8-digit CVR numbers.

## Why not exhaustive crawling

Exhaustive crawling would require either enumerating search terms or enumerating CVR numbers. Both are inefficient and server-heavy, and CVR.dk is protected against automation. The official API exists specifically for systematic extraction, but it requires credentials. Without credentials, the only defensible strategy is sparse, demand-driven lookup.

## Data fields likely available

Public CVR pages and the official API schema indicate availability of company identifier, current and historical names, legal form, status/life-cycle periods, registered address, municipality, industry codes, production units, contact details where public, employment intervals, and update timestamps. Beneficial ownership access changed in 2025 and should be treated as restricted unless the requester has a legitimate access basis.
