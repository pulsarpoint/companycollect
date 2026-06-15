# United Kingdom — License Notes

## Companies House (all products)

- **License: Open Government Licence (OGL)** — Crown copyright; free reuse incl.
  commercial, with **attribution** ("Contains public sector information licensed
  under the Open Government Licence v3.0" / "Source: Companies House"). (Some
  community sources note CC-BY framing; OGL is the operative reuse licence.)
- Applies to the **Free Company Data Product**, **Accounts Bulk Data**, **PSC
  snapshot**, and **REST API** output. The UK is the only major economy where the
  register, bulk data, API, and accounts are all free.
- **REST API**: free **API key** required (HTTP Basic, key as username); rate
  limit 600 requests / 5 minutes. The key is free on registration — not a payment
  or a bypassable control.

## PERSONAL DATA (important)

- **Officers** (REST API) and **PSC / persons with significant control** carry
  person names, partial addresses, nationality, and **month/year of birth** — this
  is **personal data** (UK GDPR / Data Protection Act 2018).
  - Redact/minimise person-level fields in published profiles/samples.
  - Companies House suppresses full DOB/usual residential address by default; still
    treat what is published as personal data with a lawful basis.
- Company-level fields (number, name, address, status, SIC, financials) are not
  personal data for the legal entity.

## Accounts Bulk Data coverage

- Only **electronically-filed** accounts are included (~60–75% of filings);
  paper/scanned PDF filings are excluded. Daily files are retained for 60 days;
  monthly files cover a rolling year.

## Summary

- **Open & usable (attribute under OGL)**: basic data, accounts iXBRL, PSC.
- **Free but key-gated**: REST API (officers, filing history).
- **Personal-data caution**: officers + PSC — redact.
- **No tax id** in Companies House (VAT held separately by HMRC).
