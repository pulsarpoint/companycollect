# Provincial Corporate Registries Field Catalog

> **DOCUMENTED-ONLY / MIXED ACCESS.** The 13 provincial/territorial registries
> (needed for provincially-incorporated companies, which the federal dataset
> excludes). Access varies: Québec REQ and BC OrgBook are open; others may be paid.
> Cataloged from public docs; no records retrieved.

## Source Summary

- Country: Canada
- Source type: official_registry
- Organization: Provincial/territorial registrars (13 jurisdictions)
- URL: https://www.orgbook.gov.bc.ca/ ; https://www.registreentreprises.gouv.qc.ca/
- License: varies by province
- Access: mixed (some open, some paid)
- Freshness: varies
- Record shape: per-province CSV/JSON/HTML
- Primary keys: `provincial_registry_number`
- Join keys: `business_number`, `name`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| registry_number | provincial reg. number | Provincial id | string | identifier | Québec NEQ / BC inc. no.; province-scoped |
| name | legal name | Name | string | legal_name | EN/FR |
| status_type | status / type | Status + form | string | status | |
| registered_office | registered office | Address | string | address | |

## Interpretation Notes

- **The coverage layer the federal dataset lacks.** Companies incorporated **in a
  province** (the majority) are not in Corporations Canada — each province's
  registry is a **separate source**, and access **varies**:
  - **Québec — REQ** (Registraire des entreprises): open data on Données Québec;
    id = **NEQ** (10-digit).
  - **BC — OrgBook BC**: open registry + API (orgbook.gov.bc.ca).
  - **Ontario, Alberta**, and others: often **paid** searches / restricted bulk.
- **Join** across federal/provincial via the **Business Number (BN)** where shared,
  or by name. Each province is its own implementation (~13 jurisdictions). Keep
  this entry as the umbrella; implement per-province on the same pattern.
