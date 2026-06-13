# Registreringstekster (CVR registration/change texts) Field Catalog

## Source Summary

- Country: Denmark
- Source type: official_registry_api (Elasticsearch)
- Organization: Erhvervsstyrelsen (Danish Business Authority)
- URL: http://distribution.virk.dk/registreringstekster
- License: Free reuse incl. commercial under CVR-loven
- Access: public_with_free_credentials (same HTTP Basic credentials as cvr-permanent)
- Freshness: near real-time
- Record shape: Elasticsearch hits; registration/change texts keyed by CVR number
- Primary keys: unknown (registration event id)
- Join keys: `cvrNummer`

> **Planning-only.** No raw records were inspected and the source is behind the same
> free-credential gate as `cvr-permanent`. The inventory documents only that it provides
> "registration text / change events per CVR number". All field names below are
> **provisional / low-confidence** and must be confirmed against a real authenticated response.

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| `…_source.cvrNummer` | cvrNummer | CVR number (join key) — provisional | integer | identifier | — | Confirm name |
| `…_source.(registration text)` | registreringstekst | Registration/announcement/change event text — provisional | string | filing | — | May contain personal data |

## Interpretation Notes

- Same distribution host and credentials as `cvr-permanent`; implement after that source.
- Intended role: **audit / history** secondary source (who changed what, when) layered on top of
  the base register and financials. Not required for a core company profile.
- May contain personal data — apply GDPR / address-protection handling.
- Confirm record structure (event date, event type, body) before any ingestion.
