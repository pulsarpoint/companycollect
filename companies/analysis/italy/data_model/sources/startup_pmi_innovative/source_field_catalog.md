# Startup innovative & PMI innovative (open data) — Field Catalog

> The **only free per-company open dataset** for Italy — but a **subset** (innovative startups + SMEs).
> Free weekly XLS lists (IODL 2.0 / CC-BY); the bulk API needs **PEC** + acceptance of conditions. Fields
> documented from the published lists; the direct CSV/JSON endpoints sit behind a dynamic portal (guessed
> URLs returned 404), so no `sample_record.json` was retrieved.

## Source Summary

- Country: Italy
- Source type: open_data_registry_subset
- Organization: InfoCamere / MIMIT
- URL: https://startup.registroimprese.it/ ; MIMIT https://www.mimit.gov.it/it/open-data
- License: IODL 2.0 / CC-BY (lists); API gated by PEC
- Access: public (XLS lists); API conditioned
- Freshness: weekly (Monday)
- Record shape: tabular per-company rows
- Primary keys: `codice_fiscale`
- Join keys: `codice_fiscale`, `denominazione + comune`

## Fields

| Path | Source field (IT) | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| denominazione | denominazione | Name | string | legal_name | |
| codice_fiscale | codice_fiscale | Fiscal code | string | identifier | join key |
| regione | regione | Region | string | geography | |
| provincia | provincia | Province | string | geography | |
| comune | comune | Municipality | string | geography | |
| ateco | codice_ateco | Activity | string | activity | ATECO |
| data_iscrizione | data_iscrizione | Registered (special section) | date | date | |
| classe_addetti | classe di addetti | Employee band | string | employment | band only |
| classe_valore_produzione | classe valore produzione | Revenue band | string | financial | **band, not a value** |

## Interpretation Notes

- **Open seed, not a master.** Innovative startups + SMEs are a small, non-representative subset, but they
  come with an **open Codice Fiscale** — a free key you can enrich authoritatively via the (paid) Registro
  Imprese / bilanci, or cross-reference against ANAC/GLEIF.
- **Financials are bands only.** `classe_valore_produzione` and `classe_addetti` are **ranges**, not exact
  figures — the only open per-company financial-ish signal Italy offers. For exact figures use bilanci XBRL.
- **Access**: the human XLS lists are freely downloadable; the machine bulk **API requires PEC** + accepting
  conditions — treat the API as conditioned. Direct file endpoints are dynamic (not stable guessable URLs).
