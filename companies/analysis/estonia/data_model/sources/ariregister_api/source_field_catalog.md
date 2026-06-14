# e-Business Register XML/REST API (real-time) Field Catalog

## Source Summary

- Country: Estonia
- Source type: official_registry
- Organization: Registrite ja Infosüsteemide Keskus (RIK)
- URL: https://www.rik.ee/en/e-business-register/business-register-queries
- License: CC-BY 4.0 (open data services)
- Access: public (some advanced services may require a free account)
- Freshness: real-time
- Record shape: XML/JSON per query across ~16 services
- Primary keys: `registrikood`
- Join keys: `registrikood`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| (service-dependent) | (per service) | Real-time company record | object | raw_extension | — | see bulk catalogs |
| registrikood | registrikood | Query/join key | string | identifier | — | lookup key |

## Interpretation Notes

- **Real-time companion to the bulk downloads.** ~16 XML services expose register data (general data, annual
  reports, shareholders, beneficial owners) in real time for **point lookups**. Open data services are free
  since Oct 2022 (CC-BY 4.0); some advanced services may require a free user account.
- **Use case:** refresh a single company between daily bulk snapshots. For full-population loads, prefer the
  bulk files (`ariregister_company_data`, `ariregister_annual_reports`). Field semantics mirror the bulk
  catalogs.
