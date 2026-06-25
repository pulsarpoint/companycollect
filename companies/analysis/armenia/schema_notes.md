# Armenia Schema Notes

## Identifiers

- **TIN / ՀՎՀՀ (HVHH)** — 8-digit taxpayer identification number; shared by the SRC and the
  State Register; the universal Armenian company key.
- **State registration number** — the State Register of Legal Entities identifier.
- **ISIN** — for AMX-listed securities (`AMxxxxxxxxxx`).

## State Register — fields (from public knowledge; bot-protected here)

- state_registration_number, TIN (ՀՎՀՀ), legal_name (Armenian; some English), legal_form
  (e.g. ՍՊԸ = LLC, ԲԲԸ/ՓԲԸ = OJSC/CJSC, individual entrepreneur), status
  (active/liquidated), registration_date, registered_address, director (executive),
  founders/participants. Director/founders are personal data — redact. No values captured.

## SRC taxpayer search — fields

- TIN (ՀՎՀՀ), taxpayer_name, taxpayer_status (active/inactive), VAT status. Per-TIN
  browser-public lookup (`/searchTaxpayerData`). No bulk/API.

## AMX — fields (SPA; not captured)

- instrument_name / issuer, ISIN (`AMxxxxxxxxxx`). Listed securities only; JS SPA.

## Formats, language, encoding

- Languages: Armenian (primary, Armenian script) + some English. UTF-8.
- Dates: Gregorian (formats to confirm per source on capture).
- Currency: Armenian Dram (AMD) for any financials.

## Mapping to internal model

- company_id ← TIN (ՀՎՀՀ) / state registration number
- registration_number ← state registration number
- tax_id / vat_id ← TIN (ՀՎՀՀ) (the TIN serves as the tax id; VAT status via SRC)
- legal_name ← State Register legal_name / SRC taxpayer_name
- legal_form ← State Register legal_form (ՍՊԸ/ԲԲԸ/ՓԲԸ)
- status ← State Register status / SRC taxpayer_status
- incorporation_date ← State Register registration_date
- registered_address ← State Register registered_address
- officers/owners ← State Register director/founders (**redact**)
- source_url, source_name, source_retrieved_at preserved per record
