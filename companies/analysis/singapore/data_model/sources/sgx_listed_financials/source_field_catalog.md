# SGX Listed-Company Disclosures Field Catalog

> **PLANNING-ONLY / EXCHANGE TERMS.** Financial results / annual reports for
> **listed** companies (issuers) only, via SGX company announcements. Reuse
> governed by SGX website terms. Cataloged from public documentation — not fetched.

## Source Summary

- Country: Singapore
- Source type: financial_disclosure
- Organization: Singapore Exchange (SGX)
- URL: https://www.sgx.com/securities/company-announcements
- License: SGX terms of use (verify before redistribution)
- Access: public (exchange website)
- Freshness: quarterly / event-driven
- Record shape: per-issuer announcements + results
- Primary keys: `ticker`
- Join keys: `uen`, `ticker`, `issuer_name`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| issuer.ticker | Stock code | Ticker | string | identifier | listed only |
| issuer.name | Issuer name | Listed name | string | legal_name | join to ACRA |
| results.period | Reporting period | Period | string | date | quarterly/annual |
| results.revenue | Revenue | Revenue (SGD) | decimal | financial | |
| results.net_profit | Net profit | Net profit (SGD) | decimal | financial | |
| filing.annual_report | Annual report | Document | object | document | |

## Interpretation Notes

- Covers only **listed** companies (~600 issuers) — a small fraction of the ACRA
  universe. For private companies there is no open financial source (BizFile is
  paid).
- Currency **SGD**; statements follow Singapore FRS / IFRS.
- **Join**: by issuer name / UEN / ticker to the ACRA entities profile.
- The only **open** financial route for Singapore, but listed-only. No raw sample.
