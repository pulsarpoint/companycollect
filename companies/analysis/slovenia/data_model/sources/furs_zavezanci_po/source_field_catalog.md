# FURS — Seznam davčnih zavezancev (legal entities) Field Catalog

## Source Summary

- Country: Slovenia
- Source type: official_tax
- Organization: Finančna uprava RS (FURS) via OPSI
- URL: https://podatki.gov.si/dataset/seznam-davcnih-zavezancev (DURS_zavezanci_PO_csv.zip)
- License: CC-BY 4.0
- Access: public
- Freshness: daily
- Record shape: **UTF-8 BOM**, **semicolon**-delimited CSV; one row per legal entity
- Primary keys: `Matična številka`
- Join keys: `Matična številka`, `Davčna številka`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| Omejen obseg identifikacije | (same) | Limited VAT-id scope | string | status | (blank) | rare |
| Zavezanost za DDV | (same) | VAT liability | string | status | * | '*' = VAT payer |
| Davčna številka | (same) | Tax number | string | identifier | 10001310 | VAT=SI+this |
| Matična številka | (same) | Registration number | string | identifier | 3282490000 | join to PRS |
| Datum registracije za DDV | (same) | VAT reg. date | date | date | 14.03.2008 | DD.MM.YYYY |
| Šifra dejavnosti | (same) | SKD activity | string | activity | 49.410 | SKD 2008 |
| Ime zavezanca | (same) | Name | string | legal_name | ISTRA XLL …, D.O.O. | trim spaces |
| Naslov zavezanca | (same) | Address | string | address | FAZANSKA ULICA 4, 6320 … | trim spaces |
| Finančni urad | (same) | Tax office | string | metadata | 06 | |

## Interpretation Notes

- **144,537 legal entities** with the fields the PRS open feed lacks: **tax
  number**, **VAT status**, **VAT registration date**, and **SKD activity code**.
  CC-BY 4.0, updated daily.
- **VAT id = `SI` + Davčna številka** when `Zavezanost za DDV` = `*`.
- **Encoding/format**: UTF-8 BOM, **semicolon**-delimited; name/address are
  fixed-width with **trailing spaces** — trim.
- Join to AJPES PRS on **Matična številka** for the structured address + legal
  form. Companion FURS lists cover sole traders (DEJ) and VAT natural persons (FO)
  — those concern individuals (personal data) and are out of scope here.
- `sample_record.json` is a real legal entity (ISTRA XLL d.o.o.).
