# Uzbekistan Schema Notes

## Identifiers

- **STIR / INN** — 9-digit taxpayer identification number; the universal Uzbek company key
  (EGRPO + tax committee). (STIR = Uzbek "Soliq toʻlovchining identifikatsiya raqami";
  ИНН = Russian.)
- **EGRPO code** — statistical register code (Unified State Register of Enterprises and
  Organizations).
- **OKED** — economic-activity classifier (analogue of NACE/ISIC).
- **ISIN** — for UZSE-listed securities.

## EGRPO register — fields (from public knowledge; data.egov.uz firewalled here)

- STIR/INN, EGRPO code, legal_name (Uzbek Latin/Cyrillic; Russian), legal_form (e.g. MCHJ =
  LLC, AJ = JSC), status (active / liquidated), registration_date, registered_address,
  OKED activity, director (head). Director is personal data — redact. No values captured.

## State Tax Committee (soliq.uz) — fields (firewalled here)

- STIR/INN, taxpayer_name, VAT (QQS) registration status, taxpayer_status. Per-STIR search /
  VAT-payers registry. Covers individuals (personal data).

## UZSE — fields (SPA; not captured)

- issuer_name, ticker, ISIN. Listed securities only.

## Formats, language, encoding

- Languages: Uzbek (Latin and Cyrillic) + Russian. UTF-8.
- Dates: Gregorian (format per source on capture).
- Currency: Uzbekistani Som (UZS) for any financials.

## Mapping to internal model

- company_id ← STIR/INN (or EGRPO code)
- registration_number ← STIR/INN / EGRPO code
- tax_id ← STIR/INN (the STIR serves as the tax id; VAT status via soliq)
- legal_name ← EGRPO legal_name / soliq taxpayer_name
- legal_form ← EGRPO legal_form (MCHJ/AJ)
- status ← EGRPO status / soliq taxpayer_status
- incorporation_date ← EGRPO registration_date
- registered_address ← EGRPO registered_address
- activity_code ← EGRPO OKED
- officers ← EGRPO director (**redact**)
- source_url, source_name, source_retrieved_at preserved per record
