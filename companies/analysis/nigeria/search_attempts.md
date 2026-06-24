# Search attempts — Nigeria

## Attempt 1
- Date/time: 2026-06-25
- Source: direct probe of candidate official hosts
- Query: GET `cac.gov.ng`, `search.cac.gov.ng`, `pre.cac.gov.ng`, `ngxgroup.com`,
  `data.gov.ng`
- Language: English
- Result: cac.gov.ng 301→403; search.cac.gov.ng 403; ngxgroup 200; data.gov.ng 000
- Decision: check the CAC gate; pursue NGX

## Attempt 2
- Date/time: 2026-06-25
- Source: CAC main + search
- Query: follow redirects; inspect 403 body
- Result: **Cloudflare** "Just a moment…" challenge on cac.gov.ng + search.cac.gov.ng
  — bot-gated. Not bypassed.
- Decision: CAC search = Cloudflare-gated; look for the BO register + paid docs

## Attempt 3
- Date/time: 2026-06-25
- Source: CAC BO register + NGX
- Query: `bor.cac.gov.ng`; NGX equities API
- Result: bor.cac.gov.ng 200 ("Persons With Significant Control"); NGX equities API
  returns real JSON
- Decision: probe BOR API + extract NGX listed data

## Attempt 4
- Date/time: 2026-06-25
- Source: BOR API (`borapp.cac.gov.ng/api`)
- Query: endpoints from the SPA JS (`/bor-search/get_psc`, `/auth/access-token`)
- Result: `/api` and search return **401**; `get_psc` is POST (405 on GET) and needs
  a token. A token-less POST to `/auth/access-token` returned **an individual user's
  PII** (misconfiguration). **Stopped — not used/stored; no bypass.**
- Decision: BOR = public-via-browser but token-gated for automation; flag the PII leak

## Attempt 5
- Date/time: 2026-06-25
- Source: NGX equities API (`doclib.ngxgroup.com/REST/api/statistics/equities/`)
- Query: GET pageSize=500
- Result: **OPEN JSON** — 146 listed equities with symbol/sector/market/prices/volume.
  Verified DANGCEM, MTNN, GTCO, ZENITHBANK, SEPLAT, NESTLE, BUACEMENT, etc.
- Decision: NGX = recommended (open, listed)

## Attempt 6
- Date/time: 2026-06-25
- Source: identifiers / tax
- Query: RC / BN / IT numbers; TIN
- Result: RC (companies), BN (business names), IT (incorporated trustees); TIN (FIRS)
- Decision: document identifier model
