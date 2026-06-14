# InfoCamere / CCIAA Regional Open Data — Field Catalog

> **Open (CC-BY 4.0) but AGGREGATE** — counts of active companies by ATECO + territory + time. **NOT a
> per-company master.** Cataloged from the real downloaded `imprese-attive-ateco.csv`. Included for
> benchmarks/denominators only.

## Source Summary

- Country: Italy
- Source type: open_data_portal (statistical)
- Organization: InfoCamere / CCIAA Marche
- URL: https://opendata.marche.camcom.it/
- License: **CC-BY 4.0** (attribute portal + "CCIAA su dati InfoCamere")
- Access: public, no auth
- Freshness: monthly / quarterly
- Record shape: CSV (semicolon), one row per (ATECO division × period), values are counts
- Primary keys: ATECO section + division + period
- Join keys: none (no company id)

## Fields

| Path | Source field (IT) | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| Settore Ateco 2025 | Settore Ateco 2025 | ATECO section | string | activity | `A Agricoltura…` | dimension |
| Divisione Ateco 2025 | Divisione Ateco 2025 | ATECO division | string | activity | `A 01 Produzioni…` | dimension |
| <period_1> | `30/04/2025` | Active-company count @ p1 | integer | metadata | `651677` | header = date |
| <period_2> | `31/05/2025` | Active-company count @ p2 | integer | metadata | `651776` | time series |

## Interpretation Notes

- **Aggregate only** — there is no company identifier, name, or address; rows are **counts** of active
  companies by ATECO division and reference date. Use for sector/territory **denominators and trends**,
  never as a company list.
- The wider portal (DCAT catalog) carries the same shape sliced by region/province/comune and by
  bankruptcies (fallimenti) and active persons — all aggregate.
- `sample_record.json` omitted (the unit is an aggregate row, not a company); the real CSV is in
  `data/italy/raw/bulk/imprese-attive-ateco.csv`.
