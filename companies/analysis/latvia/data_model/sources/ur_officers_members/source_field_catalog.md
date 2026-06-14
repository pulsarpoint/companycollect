# UR officers / members / share capital / events Field Catalog

## Source Summary

- Country: Latvia
- Source type: official_registry
- Organization: Latvijas Republikas Uzņēmumu reģistrs (UR)
- URL: https://data.gov.lv/dati/lv/organization/ur (multiple datasets)
- License: CC0-1.0 (public domain)
- Access: public (free)
- Freshness: regular
- Record shape: multiple CSV datasets keyed by regcode
- Primary keys: `regcode`
- Join keys: `regcode`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| amatpersonas[].name | amatpersona | Officer | string | person | (not parsed) | **PII** |
| dalibnieki[].name | dalībnieks | Member/shareholder | string | ownership | (not parsed) | **PII** |
| pamatkapitals.amount | pamatkapitāls | Share capital | decimal | financial | (not parsed) | EUR |
| events | maksātnespēja/likvidācija/… | Lifecycle events | array | status | (not parsed) | insolvency/liquidation/etc. |

## Interpretation Notes

- **Governance, ownership and events — all open (CC0).** The UR org publishes ~35 datasets, all keyed on
  **regcode**: **officers** (amatpersonas), **members/shareholders** (dalībnieki), **share capital**
  (equity-capitals / pamatkapitāls), plus lifecycle events — **insolvency** (maksātnespējas procesi),
  **liquidations**, **reorganizations**, and **historical names** (vēsturiskie nosaukumi).
- **Three person/ownership layers kept distinct:** officers (amatpersonas) ≠ registered members (dalībnieki) ≠
  beneficial owners (patiesie labuma guvēji). **GDPR** applies to natural persons. Documented from the dataset
  list; confirm exact column names on first parse.
