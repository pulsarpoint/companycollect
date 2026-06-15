# SOGC / SHAB — Swiss Official Gazette of Commerce Field Catalog

> **DOCUMENTED-ONLY.** The commercial-register event stream. Accessed via Zefix
> REST `/sogc` (same free Basic-auth gate) or shab.ch; not retrieved here. Names
> officers → **personal data (FADP/GDPR)**, redact.

## Source Summary

- Country: Switzerland
- Source type: official_gazette
- Organization: SECO / EHRA
- URL: https://www.zefix.admin.ch/ZefixPublicREST/api/v1/sogc/ (and shab.ch)
- License: OGD / Open use
- Access: restricted (Zefix Basic auth) / public web (shab.ch)
- Freshness: daily
- Record shape: one publication per registry event
- Primary keys: `sogc_publication_id`
- Join keys: `uid`

## Fields (documented)

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| publicationDate | publicationDate | Publication date | date | date | |
| mutationType | mutationType | Event type | string | filing | incorporation/mutation/dissolution |
| uid | uid | Affected entity | string | identifier | join to Zefix |
| officers[] | persons | Officers/signatories | array | person | PII — redact |
| text | text | Publication text | string | document | free text |

## Interpretation Notes

- The **event/history** layer: incorporations, mutations, **officer changes**, and
  dissolutions — the source of **incorporation/dissolution dates** and **officers**
  not in LINDAS.
- Two access paths: Zefix REST `/sogc/{id}` & `/sogc/bydate/{date}` (free Basic
  auth) or the shab.ch web/search. Treat officer names per data-protection law.
