# Denmark — search attempts log

## Attempt 1 — locate official CVR system-to-system access

- Date/time: 2026-06-13
- Search engine or source: WebSearch
- Query: `CVR Erhvervsstyrelsen system-til-system adgang distribution.virk.dk Elasticsearch API company register`
- Language: Danish/English
- Why this query was tried: CVR (Det Centrale Virksomhedsregister), run by Erhvervsstyrelsen,
  is the known official Danish business register; find the technical access route.
- Top relevant URLs:
  - https://erhvervsstyrelsen.dk/kom-godt-igang-med-elasticSearch
  - http://datahub.virk.dk/dataset/system-til-system-adgang-til-cvr-data
  - https://brokk-sindre.github.io/cvr-documentation/api-reference/overview/
- Result: Official access is an Elasticsearch distribution at `http://distribution.virk.dk/cvr-permanent`.
  Free credentials via email `cvrselvbetjening@erst.dk` after signing a protected-data declaration.
  Indexes: `virksomhed`, `produktionsenhed`, `deltager`; also `registreringstekster`.
- Decision: CVR-permanent = primary base-data source (auth required, free).

## Attempt 2 — locate financial statements (regnskaber) API

- Date/time: 2026-06-13
- Search engine or source: WebSearch
- Query: `Erhvervsstyrelsen regnskaber XBRL annual reports API offentliggoerelser distribution.virk.dk financial statements`
- Language: Danish/English
- Why: User explicitly needs financial data; locate the digital annual-report distribution.
- Top relevant URLs:
  - https://datacvr.virk.dk/data/
  - https://erhvervsstyrelsen.dk/vejledning-xbrl-og-inline-xbrl-rest-klient-eksempel-i-java
  - https://sprogteknologi.dk/dataset/regnskabsdata
- Result: All Danish companies must file annual reports to Erhvervsstyrelsen; published via the
  Offentliggørelser distribution. Documents available as XBRL/iXBRL/ESEF/PDF. Endpoint
  `http://distribution.virk.dk/offentliggoerelser/_search`. Since Jan 2025 non-financial
  companies must file iXBRL.
- Decision: Offentliggørelser = financial-data source (open, document URLs + XBRL figures).

## Attempt 3 — license / reuse terms

- Date/time: 2026-06-13
- Search engine or source: WebSearch
- Query: `CVR-data gratis genbruge erhvervsstyrelsen vilkår reklamebeskyttelse license commercial reuse terms`
- Result: CVR base data is free to reuse including commercially under CVR-loven (Lov om Det
  Centrale Virksomhedsregister). Caveat: *reklamebeskyttelse* (advertising protection) —
  protected entities may not be used for direct marketing and must be flagged when redistributed.
- Decision: Recorded in `license_notes.md`.

## Attempt 4 — live API verification (curl)

- Date/time: 2026-06-13
- Source: direct HTTP to distribution.virk.dk
- Calls + results:
  - `POST /offentliggoerelser/_search {term cvrNummer:25313763}` → **200**, 29 filings, doc URL returned
  - `POST /offentliggoerelser/_search {term cvrNummer:22756214 (Maersk), sort desc}` → **200**, 85 filings,
    latest = Q1 2026 interim with DELAARSRAPPORT iXBRL + XBRL + ESEF + ESEF_EXTENSION zip
  - `POST /offentliggoerelser/_search {match_all, size:0}` → **200**, total = **6,295,759**
  - `POST /cvr-permanent/virksomhed/_search {match_all}` → **401 Authorization Required** (auth needed, as documented)
- Decision: Financial API is fully open and key-less; CVR base needs free credentials. Saved raw samples.

## Attempt 5 — XBRL document download

- Date/time: 2026-06-13
- Source: regnskaber.virk.dk document URL from Attempt 4
- Result: `GET .../<token>.xml` → **200**, gzip-compressed Inline XBRL. Decompressed → valid iXBRL
  using Danish DCCA taxonomy (`xbrl.dcca.dk/fsa`, `/gsd`, `/cmn`) + IFRS/ESEF namespaces.
  Confirms machine-readable figures are openly downloadable.
- Decision: Financial figures extractable end-to-end. Saved to `data/denmark/raw/samples/`.

## Attempt 6 — index/field documentation

- Date/time: 2026-06-13
- Source: WebFetch community CVR API reference (brokk-sindre.github.io/cvr-documentation)
- Result: Confirmed record counts (virksomhed 2,194,982 / deltager 1,772,344 / produktionsenhed
  2,787,126), HTTP Basic auth, 3,000-doc query cap with scroll API for bulk, core field list.
- Decision: Recorded counts and fields in `source_inventory.json` and `schema_notes.md`.
