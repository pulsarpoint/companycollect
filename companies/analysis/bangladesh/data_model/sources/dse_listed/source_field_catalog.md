# Dhaka Stock Exchange (DSE) — listed companies Field Catalog

## Source Summary

- Country: Bangladesh
- Source type: financial_disclosure
- Organization: Dhaka Stock Exchange PLC (dsebd.org)
- URL: https://www.dsebd.org/displayCompany.php (index: company_listing.php)
- License: public disclosure
- Access: **public via browser** (plain parseable HTML; no auth/payment)
- Freshness: daily
- Record shape: listing index + per-company HTML detail pages
- Primary keys: trading_code
- Join keys: trading_code, company_name

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| trading_code | Trading Code | DSE ticker | string | identifier | AAMRANET, ACMELAB | primary key |
| scrip_code | Scrip Code | Numeric security code | string | identifier |  | alt id |
| company_name | Company Name | Listed company name | string | legal_name | The ACME Laboratories Limited | |
| sector | Sector | DSE sector | string | activity | Pharmaceuticals & Chemicals | |
| authorized_capital_mn | Authorized Capital (mn) | Authorized capital | decimal | financial |  | BDT millions |
| paid_up_capital_mn | Paid-up Capital (mn) | Paid-up capital | decimal | financial |  | BDT millions |
| listing_year | Listing Year | Year listed | string | date |  | year only (≠ incorporation) |
| market_category | Market Category | A/B/N/Z | string | status | A | DSE category |
| type_of_instrument | Type of Instrument | Equity/MF/Bond | string | metadata | Equity | filter Equity for companies |

## Interpretation Notes

- The **Dhaka Stock Exchange** is the **cleanest open** Bangladeshi company source.
  `company_listing.php` is a **plain parseable HTML** index of **~640 listed instruments**
  (verified: **637** code+name pairs parsed; the listing markup is
  `<a href='displayCompany.php?name=<CODE>'>CODE</a> <span>(Full Name)</span>`). Each company
  has a browser-public **detail page** `displayCompany.php?name=<CODE>` with Trading Code,
  Scrip Code, Sector, Authorized Capital (mn), Paid-up Capital (mn), Listing Year, Market
  Category, and Type of Instrument. No auth/payment.
- **Scope**: **listed companies only** (~640 instruments include **mutual funds and bonds** —
  filter `type_of_instrument = Equity` for operating companies). **Key = DSE trading code**;
  **currency BDT** (capital fields in millions).
- **Join**: DSE does not publish the RJSC registration number, so join to RJSC by **name**.
  **Listing Year ≠ incorporation date** (incorporation is in the RJSC register).
- A real listing and a real detail page are saved under `raw/pages/`; `sample_record.json`
  included (public market data, no personal data).
