# Bangladesh Schema Notes

## Identifiers

- **RJSC registration number** — the authoritative company/firm registrar id (Registrar of
  Joint Stock Companies and Firms). The full-register key (RJSC; paid).
- **DSE trading code** — listed-company key (Dhaka Stock Exchange), e.g. `ACMELAB`,
  `AAMRANET`; plus a numeric **scrip code**. CSE has its own trading codes.
- **BIN** (Business Identification Number) / **e-TIN** — NBR tax identifiers.

## DSE — observed fields (per-company displayCompany.php)

| Field | Meaning |
|---|---|
| Trading Code | DSE ticker / trading code (key) |
| Scrip Code | Numeric security code |
| Company Name | Listed company name |
| Sector | DSE sector classification |
| Authorized Capital (mn) | Authorized capital (BDT millions) |
| Paid-up Capital (mn) | Paid-up capital (BDT millions) |
| Listing Year | Year listed on DSE |
| Market Category | A/B/N/Z category |
| Type of Instrument | Equity / Mutual Fund / Bond / Debenture |

~640 listed instruments (637 trading-code + name pairs parsed from company_listing.php).

## RJSC — fields (from public knowledge; gated/paid)

- RJSC registration number, entity name, entity type (private/public limited company,
  partnership firm, society, trade organization), status (active / struck-off), registration
  date, registered address, authorized & paid-up capital, directors. Directors are personal
  data — redact. No values captured.

## NBR — fields (verification)

- BIN, e-TIN, taxpayer name, VAT registration status. Per-BIN/TIN verification.

## Formats, language, encoding

- Languages: English (business/registry) + Bangla. UTF-8.
- Dates: Gregorian (format per source on capture).
- Currency: Bangladeshi Taka (BDT); DSE capital fields are in BDT millions (mn).

## Mapping to internal model

- company_id ← RJSC registration number (register) / DSE trading code (listed)
- registration_number ← RJSC registration number
- tax_id ← BIN / e-TIN (NBR)
- legal_name ← DSE company name / RJSC entity name
- status ← listed (DSE) / RJSC status (active/struck-off)
- incorporation_date ← RJSC registration date; listing_year ← DSE Listing Year
- registered_address ← RJSC registered address
- activity_code ← DSE Sector (listed); RJSC entity type
- financials/capital ← DSE Authorized/Paid-up Capital (mn, BDT)
- officers ← RJSC directors (**redact**)
- source_url, source_name, source_retrieved_at preserved per record
