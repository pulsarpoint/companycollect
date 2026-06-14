# CompanyBook.BG — Field Catalog

> Third-party REST API over the Commercial Register: **non-financial data free**, **financials paid** (parsed
> balance sheets + income statements 2022+). Useful because it turns the **PDF ГФО into structured figures**.
> Cataloged from public docs; API key likely required → no sample.

## Source Summary

- Country: Bulgaria
- Source type: third_party_api
- Organization: CompanyBook.BG (private)
- URL: https://companybook.bg/?lang=en
- License: non-financial free; financials paid (underlying = CC-BY register)
- Access: public (non-financial) / paid (financials)
- Freshness: from the Commercial Register
- Record shape: JSON per company
- Primary keys: `eik`
- Join keys: `eik`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| eik | eik | Company EIK | string | identifier | join |
| name | name | Name | string | legal_name | |
| legal_form | legal_form | Legal form | string | legal_form | |
| status | status | Status | string | status | |
| address | address | Address | string | address | |
| contacts | contacts | Contacts | object | metadata | website |
| financials[] | balance/income (PAID) | Parsed financials | array | financial | **structured (paid)** |
| vat_id | vat | VAT | string | identifier | BG+EIK |

## Interpretation Notes

- **Why include it**: Bulgaria's official financials are **PDFs**; CompanyBook (and APIS) **pre-parse** them
  into structured balance sheets + income statements (paid) — the convenient route to structured financials
  at scale without building Cyrillic OCR.
- Underlying register data is **CC-BY** (attribute the Registry Agency). Free tier covers non-financial
  fields; financials are a paid subscription. Same **EIK** key. No `sample_record.json` (API key needed).
