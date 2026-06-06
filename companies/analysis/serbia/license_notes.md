# Serbia — License Notes

## APR Open Data APIs (companies, financial-statements, ngo)

- **License declared on `data.gov.rs`:** `public_domain` (catalog metadata field).
- **Portal label:** "Јавни подаци" (Public Data).
- **Publisher:** Агенција за привредне регистре (APR), an official government agency.
- **Portal terms of use:** `https://data.gov.rs/terms` (referenced from each
  dataset page; not quoted in detail here — review before public redistribution).
- **Assessment:** Declared public domain by the official publisher on the national
  open data portal. Reuse and redistribution should be permissible; standard good
  practice is to **attribute APR / data.gov.rs** and to retain the `DatumPreseka`
  snapshot date so downstream users know the vintage.
- **Uncertainty:** The single-word `public_domain` tag is the strongest signal,
  but the portal's general Terms of Use page was not fully captured. Before
  republishing the data verbatim at scale, confirm the `data.gov.rs/terms`
  conditions (attribution, no-warranty, possible non-endorsement clauses).

## APR Web-Service (veb-servis) — paid

- Governed by a **contract / prescribed fee** (Pravilnik on data delivery).
- Free only for state bodies; banks and other businesses pay.
- Redistribution rights are governed by that contract — **not** open. Do not
  assume reuse rights; negotiate explicitly with APR (`apr-podaci@apr.gov.rs`).

## OpenCorporates

- Subject to **OpenCorporates' own terms**; bulk/API access requires an agreement
  and may carry share-alike / attribution obligations. Treat as restricted.

## Statistical Office (RZS)

- Open statistical data (aggregate only). Standard attribution to RZS advisable.

## Bottom line

The three **APR OpenAPI datasets are the only clearly reusable (public domain)
company sources** and are safe to ingest. Everything richer (entrepreneurs, PIB,
ownership, real-time) sits behind APR's paid contract and is **not** open.
