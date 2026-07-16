# Norway — license notes

## Summary

All recommended Norwegian company data (Enhetsregisteret entities + sub-entities, and
Regnskapsregisteret financial figures) is published by the **Brønnøysund Register Centre (Brreg)**
under the **Norwegian Licence for Open Government Data (NLOD) 2.0**.

- Official license name: **Norsk lisens for offentlige data (NLOD) 2.0** /
  *Norwegian Licence for Open Government Data (NLOD) 2.0*.
- License text: https://data.norge.no/nlod/en/2.0
- Confirmed on the data.norge.no catalog entry for Regnskapsregisteret and in the
  Enhetsregisteret API documentation.

## What NLOD 2.0 permits

- **Copy, use, and redistribute** the data, including for commercial purposes.
- **Combine** it with other data and build derivative products/services.
- Requires **attribution** to the source (Brønnøysundregistrene) and, where practical, a link
  to the license and an indication if the data has been modified.

## Required attribution

Use something like:

> Inneholder data under Norsk lisens for offentlige data (NLOD) tilgjengeliggjort av
> Brønnøysundregistrene.
>
> (English) Contains data under the Norwegian Licence for Open Government Data (NLOD) made
> available by the Brønnøysund Register Centre.

A short form for UI/footers: **"Kilde: Brønnøysundregistrene (NLOD 2.0)"**.

## Caveats / uncertainty

- **Regnskapsregisteret open key-figures API** is a preview and returns the latest filing only.
  Its key-figure data is NLOD 2.0, but the API channel is not guaranteed long-term.
- **Public annual-report PDF copies** are accessible without authentication for the latest
  15 years, but the checked API documentation does not establish that the documents themselves
  inherit NLOD. Obtain legal confirmation before retaining and redistributing report copies or
  a dataset derived from bulk OCR.
- **Paid annual-accounts subscription** data is governed by a framework agreement and a specific
  sub-agreement, not NLOD. The agreement describes daily XML delivery and restricts use of TIFF
  copies. Confirm historical backfill and downstream redistribution rights contractually.
- **Personal data**: entity records can contain personal names (e.g. roles — CEO, board,
  auditor). Role data with national identity numbers is only via the authenticated
  `autorisert-api` (Maskinporten) and is **not** open. Handle any personal data per GDPR even
  when the company data itself is open.
- **Beneficial Ownership Register** (reelle rettighetshavere) is **not** open bulk data — access
  is controlled. Excluded from open ingestion.
- Aggregator sites (proff.no, purehelp.no, regnskapstall.no, etc.) repackage this data under
  their own terms — prefer the Brreg source to stay clearly within NLOD 2.0.

## Bottom line

Safe to ingest and redistribute Enhetsregisteret + latest Regnskapsregisteret key figures in a
commercial product **with NLOD attribution**. Treat report PDFs and subscription XML as separate
legal products until their retention, derivation, and redistribution rights are confirmed.
