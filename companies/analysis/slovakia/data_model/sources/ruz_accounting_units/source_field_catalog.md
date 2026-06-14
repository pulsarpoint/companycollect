# RÚZ Accounting Units (Open API) Field Catalog

## Source Summary

- Country: Slovakia
- Source type: official_registry
- Organization: Register účtovných závierok (Ministry of Finance SR / DataCentrum)
- URL: https://www.registeruz.sk/cruz-public/api/uctovna-jednotka?id={id}
- License: CC0
- Access: public
- Freshness: continuous (incremental via `zmenene-od`)
- Record shape: JSON object per accounting unit
- Primary keys: `ico`
- Join keys: `ico`, `dic`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| id | id | Accounting-unit id | integer | identifier | 154048 | links statements |
| ico | ico | IČO | string | identifier | 31333532 | join key |
| dic | dic | Tax id (DIČ) | string | identifier | 2020317068 | VAT=SK+dic |
| nazovUJ | nazovUJ | Name | string | legal_name | ESET, spol. s r.o. | |
| ulica/mesto/psc | — | Street/city/postal | string | address | Einsteinova 24, 85101 | |
| datumZalozenia | datumZalozenia | Founded | date | date | 1992-09-17 | |
| datumZrusenia | datumZrusenia | Dissolved | date | date | — | dissolution signal |
| pravnaForma | pravnaForma | Legal-form code | string | legal_form | 112 | → pravne-formy |
| skNace | skNace | SK NACE | string | activity | 62090 | → sk-nace |
| velkostOrganizacie | — | Org size code | string | metadata | 31 | → velkosti-organizacie |
| druhVlastnictva | — | Ownership type | string | ownership | 2 | → druhy-vlastnictva |
| kraj/okres | — | Region/district | string | geography | SK010/SK0105 | |
| konsolidovana | konsolidovana | Consolidated? | boolean | metadata | true | |
| idUctovnychZavierok[] | — | Statement ids | array | filing | [6500234,…] | → uctovna-zavierka |
| idVyrocnychSprav[] | — | Annual-report ids | array | filing | [2600812,…] | → vyrocna-sprava |

## Interpretation Notes

- The RÚZ master record: identity (IČO, DIČ), address, legal form, SK NACE, dates,
  region/district, and the **lists of statement/report ids** that link to the
  financial data. CC0.
- **Crawl**: `uctovne-jednotky?zmenene-od=YYYY-MM-DD&max-zaznamov=≤10000&pokracovat-za-id=…`
  → ids; then `uctovna-jednotka?id=…` per unit. Incremental by `zmenene-od`.
- `datumZrusenia` is the best open **dissolution** signal (RPO has no single
  status flag). Decode coded fields via the classifier endpoints.
- `sample_record.json` is the real ESET unit (id 154048), company-level only.
