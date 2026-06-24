# QKB — National Business Center Field Catalog

## Source Summary

- Country: Albania
- Source type: official_registry
- Organization: Qendra Kombëtare e Biznesit (QKB)
- URL: https://qkb.gov.al/
- License: official register (public extracts)
- Access: free per-company extract (ekstrakt)
- Freshness: live register
- Record shape: per-company extract
- Primary keys: nipt
- Join keys: nipt

## Fields

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| ekstrakt.nipt | NIPT/NUIS | Unique id | string | identifier | = tax id = VAT id |
| ekstrakt.emri | Emri | Name | string | legal_name | |
| ekstrakt.forma_ligjore | Forma ligjore | Legal form | string | legal_form | |
| ekstrakt.data_regjistrimit | Data e regjistrimit | Registration date | date | date | |
| ekstrakt.administrator_ortake | Administrator/Ortakë | Officers/owners | array | person | **PERSONAL DATA — redact** |
| ekstrakt.bilanci | Pasqyrat financiare | Financial statements (ALL) | array | financial | filed with QKB |

## Interpretation Notes

- The official register: authoritative per-company extract (ekstrakt) by NIPT/name —
  identity, legal form, registration date, administrator/owners, capital, activity,
  address, status, and **financial statements** (bilanci/pasqyrat financiare, ALL).
- **Access**: free per-company extract; **no open bulk** (Open Data Albania mirrors
  it openly). **Administrator/owners** are personal data — redact.
- Currency **ALL (Lek)**. No raw sample record (official per-company source; the open
  mirror provides the sample).
