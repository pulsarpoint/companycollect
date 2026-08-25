# APR Financial Statements (Open Data API) Field Catalog

## Source Summary

- Country: Serbia
- Source type: official_registry
- Organization: Agencija za privredne registre (APR) — Registar finansijskih izveštaja (RGFI)
- URL: https://openapi.apr.gov.rs/api/opendata/companies/financial-statements
- License: Serbian Open Data License (`sodl`)
- Access: public (plain GET)
- Freshness: monthly (DatumPreseka 2026-05-31)
- Record shape: JSON `{DatumPreseka, Podaci:{<maticni_broj>:{...}}}`
- Primary keys: `maticni_broj`
- Join keys: `maticni_broj`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| Podaci.<mb> (key) | maticni_broj | Registration number | string | identifier | 21141666 | join key |
| …GodinaFi | GodinaFi | Reporting year | integer | date | 2024 | latest only |
| …PoslovnoIme | PoslovnoIme | Business name | string | legal_name | ENEKS MONT PLUS DOO… | |
| …SifraOpstine | SifraOpstine | Municipality code | integer | geography | 70670 | int here |
| …NazivOpstine | NazivOpstine | Municipality name | string | geography | КРУШЕВАЦ | Cyrillic |
| …PoslovnaImovina | PoslovnaImovina | Business assets | integer | financial | 55995 | thousands RSD |
| …Kapital | Kapital | Capital/equity | integer | financial | 10076 | thousands RSD |
| …Gubitak | Gubitak | Accumulated loss | integer | financial | 0 | thousands RSD |
| …UkupniPrihodi | UkupniPrihodi | Total revenue | integer | financial | 123852 | thousands RSD |
| …NetoDobitak | NetoDobitak | Net profit | integer | financial | 3542 | thousands RSD |
| …NetoGubitak | NetoGubitak | Net loss | integer | financial | 0 | thousands RSD |
| …ProsecanBrojZaposlenih | ProsecanBrojZaposlenih | Avg. employees | integer | employment | 4 | headcount |

## Interpretation Notes

- **122,863 statements** (2026-05-31) — the **latest** annual financial statement
  (RGFI) per company, **one year only** (no multi-year history in the open feed).
- A compact summary set (assets, capital, accumulated loss, total revenue, net
  profit, net loss, employees) — not the full balance sheet / income statement.
- **Units**: monetary values are reported in **thousands of RSD** (RGFI
  convention); employees is a plain headcount. Net result =
  `NetoDobitak - NetoGubitak`; accumulated `Gubitak` is distinct from the period
  `NetoGubitak`.
- **Coverage gap**: 122,863 of 133,357 companies have a statement — newly formed
  or non-filing entities may be absent.
- `sample_record.json` is a real record (ENEKS MONT PLUS DOO, MB 21141666, 2024).
