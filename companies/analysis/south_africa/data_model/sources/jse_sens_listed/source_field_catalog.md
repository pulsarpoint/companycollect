# JSE / SENS Listed-Company Disclosures Field Catalog

> **PLANNING-ONLY / EXCHANGE TERMS.** Financial results / annual reports for
> **listed** companies (issuers) via JSE and SENS announcements. Reuse governed by
> JSE/SENS terms. Cataloged from public documentation — not fetched.

## Source Summary

- Country: South Africa
- Source type: financial_disclosure
- Organization: Johannesburg Stock Exchange (JSE)
- URL: https://www.jse.co.za/
- License: JSE / SENS terms of use (verify before redistribution)
- Access: public (exchange website)
- Freshness: event-driven / periodic
- Record shape: per-issuer announcements + results
- Primary keys: `share_code`
- Join keys: `registration_number`, `issuer_name`, `share_code`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| issuer.share_code | Share code | JSE code | string | identifier | listed only |
| issuer.name | Issuer name | Listed name | string | legal_name | join to CIPC |
| results.period | Reporting period | Period | string | date | |
| results.revenue | Revenue | Revenue (ZAR) | decimal | financial | IFRS |
| results.headline_earnings | Headline earnings | HEPS (ZAR) | decimal | financial | JSE-specific |
| filing.sens_announcement | SENS announcement | Document | object | document | results/AR |

## Interpretation Notes

- Covers only **listed** companies (~250 issuers) — a tiny fraction of the South
  African company universe. For private companies there is no open financial source
  (CIPC AFS is paid).
- Currency **ZAR**; statements follow IFRS. **Headline earnings (HEPS)** is a
  JSE-specific measure.
- **Join**: by issuer name / registration number / share code to the rest of the
  profile.
- The only **open** financial route for South Africa, but listed-only. No raw
  sample.
