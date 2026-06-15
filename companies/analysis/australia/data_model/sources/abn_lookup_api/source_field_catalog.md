# ABN Lookup Web Services Field Catalog

> **FREE BUT GUID-GATED.** Free per-ABN/ACN/name lookup after registering for a
> free **GUID** (auth token). Same public ABN fields as the bulk extract.
> Cataloged from public docs; no records pulled (GUID not registered here).

## Source Summary

- Country: Australia
- Source type: official_registry
- Organization: ATO — Australian Business Register
- URL: https://abr.business.gov.au/Tools/WebServices (JSON: https://abr.business.gov.au/json/)
- License: CC-BY 3.0 Australia
- Access: public (free GUID registration)
- Freshness: real-time
- Record shape: JSON / SOAP per ABN/ACN/name
- Primary keys: `ABN`
- Join keys: `ABN`, `ACN`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| ABN / ACN | abn/acn | Identifiers | string | identifier | requires GUID |
| entityName | entityName/mainName | Name | string | legal_name | |
| entityType / GST / state / postcode | core fields | Core public fields | object | metadata | same as extract |
| businessNames[] | businessNames | Business/trading names | array | legal_name | |

## Interpretation Notes

- The **real-time per-ABN enrichment** counterpart to the bulk extract (same public
  data, same CC-BY 3.0 AU licence). Requires a **free GUID** (registration, not a
  payment); intended for lookups, **not bulk** (use the extract for bulk). JSON +
  SOAP. Redact individual names.
