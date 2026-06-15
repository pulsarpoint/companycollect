# AJPES restPrsInfo (REST web service) Field Catalog

> **PLANNING-ONLY / CREDENTIALED.** Requires AJPES registration (FTP
> username/password) and is **explicitly not for mass download**. Cataloged from
> the public developer description; no records retrieved. Fields are the concepts
> the open feeds lack.

## Source Summary

- Country: Slovenia
- Source type: official_registry
- Organization: AJPES
- URL: https://www.ajpes.si/Doc/AJPES/Za_razvijalce/restPrsInfo_Opis_servisa_za_razvijalce.pdf
- License: AJPES terms (registered users)
- Access: restricted (credentials; not mass download)
- Freshness: real-time
- Record shape: planning-only (JSON/XML; minimal / ožja / širša tiers)
- Primary keys: `Matična številka`
- Join keys: `Matična številka`, `Davčna številka`

## Fields (from public docs)

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| status | status | Entity status | string | status | planning-only; fills status gap |
| skd | SKD dejavnost | Full activity (primary+secondary) | array | activity | planning-only |
| datumVpisa | datum vpisa | Registration date | date | date | planning-only; fills incorporation gap |
| spremembe[] | change list | Changed reg-numbers in a period | array | metadata | planning-only; incremental sync |

## Interpretation Notes

- The credentialed route to the fields the open CSVs lack: **status**, **full SKD
  activity**, **registration date**, and a **change-list** for incremental
  updates. Searchable by many identifiers; returns minimal/narrow/broad tiers.
- **Not for mass download** — intended for targeted lookups/sync, not bulk
  ingestion. Keep **planning-only** until AJPES access is obtained; do not scrape.
