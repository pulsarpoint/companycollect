# MBR API packages (Company Search API, etc.) Field Catalog

> **Paid / subscription.** The MBR's official API packages require registration + payment. Fields described from
> public documentation; no records/values copied. No `sample_record.json`.

## Source Summary

- Country: Malta
- Source type: official_registry
- Organization: Malta Business Registry (MBR)
- URL: https://mbr.mt/ (API packages)
- License: commercial / subscription (MBR API packages)
- Access: paid
- Freshness: real-time
- Record shape: JSON per query (subscription API)
- Primary keys: `registration_number`
- Join keys: `registration_number`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| registration_number | registrationNumber | Company id | string | identifier | (paid) | join key |
| company_record | (per package) | Structured company data | object | raw_extension | (paid) | see mbr_register |
| financial_information | financialInformation | Annual accounts/return data | object | financial | (paid) | structured-financials route |

## Interpretation Notes

- **The sanctioned automation path.** The MBR has launched official **API packages** (e.g. a Company Search API)
  on a **subscription/paid** basis — the lawful way to automate, since the free web search is **WAF-blocked** for
  bots. The API returns structured company data (name, registration, **officers**, **shareholders**, **financial
  information**); the exact field set depends on the package.
- For **structured financials at scale**, the paid API (where the package includes financial information) or a
  commercial provider is the practical alternative to OCR-ing the paid PDFs. Field semantics mirror the
  `mbr_register` catalog. Join on **registration_number**.
