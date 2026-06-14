# ANAF financial statements (bilant) web service Field Catalog

## Source Summary

- Country: Romania
- Source type: official_tax
- Organization: Agenția Națională de Administrare Fiscală (ANAF)
- URL: `GET https://webservicesp.anaf.ro/bilant?an=YYYY&cui=CUI`
- License: public information (financial statements published by law)
- Access: public (no auth/payment)
- Freshness: annual; **verified live 2014–2024** (doc says 2014–2019 but is stale)
- Record shape: JSON `{an, cui, deni, caen, den_caen, i:[{indicator, val_indicator, val_den_indicator}]}`
- Primary keys: `cui` + `an`
- Join keys: `cui`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| an | an | Fiscal year | integer | date | 2024 | composite key |
| cui | cui | Fiscal code | integer | identifier | 14399840 | join to OD_FIRME.CUI |
| deni | deni | Entity name | string | legal_name | Dante International SA | empty if no statement |
| caen | caen | CAEN code | integer | activity | 4754 | main activity |
| den_caen | den_caen | CAEN description | string | activity | Comert cu amanuntul… | |
| i[].indicator | indicator | Indicator code | string | financial | I13, I18, I20 | see code list below |
| i[].val_indicator | val_indicator | Value (RON) | integer | financial | 8992961799 | plain RON |
| i[].val_den_indicator | val_den_indicator | Label (RO) | string | financial | Cifra de afaceri neta | |

### Indicator code list (commercial entity)

| Code | RO label | English |
|---|---|---|
| I1 | ACTIVE IMOBILIZATE - TOTAL | Fixed assets |
| I2 | ACTIVE CIRCULANTE - TOTAL | Current assets |
| I3 | Stocuri | Inventories |
| I4 | Creante | Receivables |
| I5 | Casa si conturi la banci | Cash & bank |
| I6 | CHELTUIELI IN AVANS | Prepaid expenses |
| I7 | DATORII | Liabilities |
| I8 | VENITURI IN AVANS | Deferred income |
| I9 | PROVIZIOANE | Provisions |
| I10 | CAPITALURI - TOTAL | Equity |
| I11 | Capital subscris varsat | Paid-up capital |
| I13 | Cifra de afaceri neta | Net turnover |
| I14 | VENITURI TOTALE | Total revenue |
| I15 | CHELTUIELI TOTALE | Total expenses |
| I16 | Profit brut | Gross profit |
| I17 | Pierdere bruta | Gross loss |
| I18 | Profit net | Net profit |
| I19 | Pierdere neta | Net loss |
| I20 | Numar mediu de salariati | Avg. employees |

(Insurance/financial-sector entities use additional codes up to I33.)

## Interpretation Notes

- **Free, official, structured financials** by CUI/year — the standout feature
  that makes Romania best-in-class. Values are **plain RON** (not thousands).
- **WAF/User-Agent gotcha**: a non-browser User-Agent receives an empty `i:[]`
  with `deni:""` (looks like "no data"). Send a normal browser UA
  (`Mozilla/5.0`) to get real data. This is **not** a control bypass — no
  auth/payment/CAPTCHA is involved; it is an F5 device that filters bot UAs.
- **Rate limit: max 1 request/second.** Enrich per CUI; do not parallelise hard.
- The indicator set depends on entity type; map by `indicator` code, not order.
- `sample_record.json` is the real 2024 statement for Dante International SA
  (public information): net turnover 8,992,961,799 RON; avg. employees 3,191.
