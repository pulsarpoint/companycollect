# GISA — Gewerbe in Österreich (open) — Field Catalog

> **OPEN** dataset of **active trade authorizations** (Gewerbeberechtigungen) **without personal data**,
> derived from GISA, on data.gv.at (CSV + JSON). The best **open** per-business artifact for Austria — but
> **trade licences, NOT a company master**, and **no guaranteed Firmenbuchnummer link**. Fields documented
> from the dataset description; the direct file URL could not be resolved in this environment → no sample.

## Source Summary

- Country: Austria
- Source type: open_data_registry_subset
- Organization: BMAW/BMWET (GISA) via data.gv.at
- URL: https://www.data.gv.at/katalog/dataset/gewerbe-in-osterreich ; query https://www.gisa.gv.at/abfrage
- License: open (data.gv.at; likely **CC-BY 4.0** — confirm)
- Access: public, no auth
- Freshness: regular (monthly statistics resource)
- Record shape: CSV/JSON, one row per active trade authorization
- Primary keys: `gisa_zahl`
- Join keys: `gisa_zahl`; `name + standort`

## Fields

| Path | Source field (DE) | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| gisa_zahl | GISA-Zahl | GISA trade-register number | string | identifier | **not** Firmenbuchnummer |
| name | Name/Firmenwortlaut | Business name | string | legal_name | no personal data |
| standort | Standort/Adresse | Location | string | address | → municipality |
| gewerbewortlaut | Gewerbewortlaut | Trade wording | string | activity | licensed activity (free text) |
| gewerbeschluessel | Gewerbeschlüssel | Trade code | string | activity | companion code list on data.gv.at |
| status | Status | Active | string | status | dataset = active |

## Interpretation Notes

- **Open seed, not a master.** Covers **trade authorizations**, not all companies, and deliberately
  **excludes natural-person sole-trader personal data**. Useful as an **open activity/location layer** and
  a name+location seed to match against the (paid) Firmenbuch.
- **Activity**: the **Gewerbeschlüssel** (trade code) + Gewerbewortlaut are the closest **open** activity
  signal Austria offers (the Firmenbuch `Geschäftszweig` is free text only). Use the companion code list.
- **Join risk**: keyed on **GISA-Zahl**, not the Firmenbuchnummer — linking to the company spine is by
  **name+location** (fuzzy) unless a UID/Firmenbuchnummer is present.
- **Access caveat**: the data.gv.at portal is JS-fronted; resolve the exact CSV/JSON resource URL + the
  confirmed license from the dataset page before ingestion (documented as a follow-up).
