# Company data sources for Serbia (RS)

## Status

- Official company bulk/API: **found** — APR Companies Open Data API
- Official representatives open data: **not found**
- Official representatives paid delivery: **found** — one-off data sets and contracted web service
- Official beneficial owners: **separate APR CEV register** — eID/contract restricted; not SP3/SP4
- Open-data license: **Serbian Open Data License (SODL / `sodl`)**
- Recommended ingestion path: **hybrid**

## Best source

Use the official Serbian Business Registers Agency (APR) open endpoint for the
company backbone:

`https://openapi.apr.gov.rs/api/opendata/companies`

A live check on 2026-08-24 returned a 2026-07-31 snapshot with 133,634
companies. The payload is a single JSON document whose `Podaci` object is keyed
by the eight-digit `matični broj`.

The open feed exposes only:

- `PoslovnoIme`
- `SifraOpstine`
- `NazivOpstine`
- `NazivStatus`
- `DatumOsnivanja`
- `NazivPravneForme`
- `SifraDelatnosti`

It does **not** expose legal representatives, other representatives, directors,
procurists, boards, members/founders, PIB, or a street address.

## Representatives

APR's 2026 paid data-set schedule places:

- legal representatives in add-on set **SP3**;
- other representatives, directors' boards, supervisory/executive boards,
  procurists and management boards in **SP4**;
- members/founders in **SP5**;
- branch representatives and branch procurists in **SP6**.

All add-on sets require the expanded base set **SP2**. One-off delivery is
available as XLS/XLSX, or MDB by special request. For continuous operation,
APR's contracted web service supports change retrieval by time period and
selected data groups by `matični broj`.

## Recommendation

1. Ingest the open company JSON monthly and key records by `matični broj`.
2. Obtain an initial representative backfill from APR using SP2 + SP3, adding
   SP4 if procurists and non-statutory representatives are required.
3. Use APR's contracted web service for daily representative changes.
4. Do not scrape APR's public search. APR explicitly prohibits automated tools
   against the search site, and the current UI uses reCAPTCHA.
5. Contract CEV separately only after privacy/security review if beneficial
   ownership is required. Never infer it from the public `Чланови` section.

See `representatives_analysis.md` for integration/cost analysis and
`data_model/apr_company_people_model.md` for the manually observed
representative shape, CEV field contract and ClickHouse table proposal.
