# Georgia Schema Notes

## Identifiers

- **Identification code (საიდენტიფიკაციო კოდი)** — 9-digit; the company **registration
  number** and the **tax id** in one (used by both NAPR and the Revenue Service). The
  universal key for Georgian companies.
- **ISIN** — for listed securities (Georgian Stock Exchange); format `GExxxxxxxxxx`.

## NAPR e-registry — fields (from public knowledge; search CAPTCHA-gated)

- identification_code, legal_name (Georgian; English for some), legal_form (e.g. შპས = LLC,
  სს/ს.ს = JSC, ი.მ = individual entrepreneur), status (active/registered/liquidated),
  registration_date, registered_address, director, partners/shareholders, and a downloadable
  **extract (amonaweri)** PDF. Directors/partners are personal data. No values captured.

## SARAS Reporting Portal (reportal.ge) — fields

- identification_code, company_name, legal_form, NACE activity codes, reporting year, and
  filed **financial statements** + **management report** (PDF). Browser-public search by
  identification code or name; detailed search params: orgName, year, legalFormId,
  catgoryId, naceCodes. Automation requires the `__RequestVerificationToken`.

## GSE — fields

- security_name / issuer, ISIN (`GExxxxxxxxxx`). 32 distinct ISINs observed on the
  securities page (e.g. GE1100000029, GE2700604186). Listed companies only.

## Formats, language, encoding

- Languages: Georgian (primary) + some English. UTF-8.
- Dates: Gregorian (formats to be confirmed per source on capture).
- Currency: Georgian Lari (GEL) for financials (reportal).

## Mapping to internal model

- company_id ← identification_code (9-digit)
- registration_number ← identification_code
- tax_id / vat_id ← identification_code (the same code serves as the tax id)
- legal_name ← NAPR legal_name / reportal company_name
- legal_form ← legal_form (შპს/სს/ი.მ)
- status ← NAPR status
- incorporation_date ← NAPR registration_date
- registered_address ← NAPR registered_address
- activity_code ← reportal NACE codes
- financials ← reportal financial statements (GEL)
- officers/owners ← NAPR director/partners (**redact**)
- source_url, source_name, source_retrieved_at preserved per record
