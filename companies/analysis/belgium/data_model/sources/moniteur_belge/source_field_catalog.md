# Moniteur Belge / Belgisch Staatsblad — Field Catalog

> Official gazette of company **acts/publications** (incorporations, amendments, appointments,
> dissolutions, mergers). Free public search (ejustice). An **event/lifecycle** source keyed on the
> EnterpriseNumber — not a bulk master. Documented; no sample pulled.

## Source Summary

- Country: Belgium
- Source type: official_gazette
- Organization: FOD Justitie / SPF Justice
- URL: https://www.ejustice.just.fgov.be/
- License: free public
- Access: public, no auth (web search)
- Freshness: daily
- Record shape: per-publication (HTML/PDF), searchable by enterprise number
- Primary keys: `publication_id`
- Join keys: `EnterpriseNumber`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| enterpriseNumber | Ondernemingsnummer | Enterprise | string | identifier | join when present |
| name | Naam/Dénomination | Company name | string | legal_name | |
| publicationType | Type publicatie | Act type | string | filing | event type |
| publicationDate | Datum/Date | Date | date | date | event date |
| reference | Referentie | Reference | string | identifier | |
| documentUrl | PDF link | Document | string | document | |

## Interpretation Notes

- **Lifecycle/events** — incorporations, statute amendments, appointments, dissolutions, mergers — keyed
  on the **EnterpriseNumber** (clean join). Complements KBO status with dated acts.
- Access is **free web search**; no documented clean bulk API (consider the daily gazette feed). Treat as a
  secondary event layer, not a master. No `sample_record.json` (web).
