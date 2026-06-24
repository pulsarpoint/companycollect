# FNS Open Data — Tax Information Field Catalog

## Source Summary

- Country: Russia
- Source type: tax_registry
- Organization: Federal Tax Service (ФНС / FNS)
- URL: https://www.nalog.gov.ru/opendata/ (bulk at file.nalog.ru)
- License: open data (FNS)
- Access: public bulk (no key)
- Freshness: annual
- Record shape: bulk XML/CSV per dataset, keyed on ИНН
- Primary keys: ИНН
- Join keys: ИНН

## Fields

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| set.ИНН | ИНН | Taxpayer id | string | identifier | join key |
| sshr | Среднесписочная численность | Average headcount | integer | employment | sshr set |
| revexp | Доходы/расходы | Income/expense | object | financial | revexp set, RUB |
| paidtax | Уплаченные налоги | Paid taxes | object | financial | paidtax set, RUB |
| taxregime | Спецрежимы | Special tax regimes | string | license_or_terms | |

## Interpretation Notes

- Since 2016 the FNS publishes formerly-tax-secret information about all
  organizations as **open datasets keyed on ИНН**: average headcount (sshr),
  income/expense from accounting statements (revexp), paid taxes (paidtax), special
  tax regimes, tax arrears, plus the register of disqualified persons
  (registerdisqualified). Bulk XML/CSV at file.nalog.ru (each with a passport + XSD).
- **Enrichment** layer joined on **ИНН** to GIR BO / RSMP / EGRUL. Currency RUB.
- Verified: the FNS opendata listing loads with all these dataset links. No raw
  sample record (bulk sets not downloaded here).
