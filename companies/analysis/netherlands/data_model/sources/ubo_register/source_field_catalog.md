# UBO-register (beneficial owners) — KvK Field Catalog

> **Planning-only / restricted.** General public access suspended after the Nov 2022 CJEU ruling; access for
> AML-obliged entities (expanded via the KvK API from April 2026). Not open bulk. No records/values copied. No
> `sample_record.json`.

## Source Summary

- Country: Netherlands
- Source type: beneficial_ownership_register
- Organization: Kamer van Koophandel (KvK)
- URL: https://www.kvk.nl/ubo/
- License: restricted (AML-obliged entities; post-CJEU)
- Access: restricted
- Freshness: continuous
- Record shape: access-controlled register
- Primary keys: `kvkNummer`
- Join keys: `kvkNummer`, `rsin`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| ubo[].naam | uiteindelijk belanghebbende | UBO natural person | string | ownership | (restricted) | GDPR; restricted |
| ubo[].aardEnOmvang | aard en omvang | Nature/extent of interest | string | ownership | (restricted) | planning-only |

## Interpretation Notes

- **Restricted, not open.** After the Nov 2022 CJEU ruling, general public access was withdrawn; access is for
  **AML-obliged entities** (expanded via the KvK API from April 2026; API-UBO 2.0 JSON planned 2027). Treat the
  whole source as **planning-only**; do not attempt to bypass access controls. Beneficial owners are **personal
  data (GDPR)**. Join on `kvkNummer`/`rsin`.
- Distinct from **officers** (functionarissen, in the paid Basisprofiel) — keep them as separate sub-concepts.
