# Slovakia — Source Inventory

| Source | Slug | Type | Access | License | Format | Status |
|---|---|---|---|---|---|---|
| RPO — Register of Legal Entities (Statistics Office) | rpo | official_registry | public | CC-BY 4.0 | json | recommended |
| RÚZ — accounting units | ruz_accounting_units | official_registry | public | CC0 | json | recommended |
| RÚZ — financial statements & reports | ruz_financial_reports | official_registry | public | CC0 | json | recommended |
| ORSR — commercial register | orsr | official_registry | public (web) | unknown | html | useful_secondary_source |
| FinStat / aggregators | finstat | aggregator | paid | commercial | json | useful_secondary_source |

## Best combination

**RPO** (identity, officers, shareholders, share capital, activities, history;
CC-BY 4.0) + **RÚZ** (accounting-unit master + full structured financial
statements; CC0), joined on **IČO**. Both official, free, machine-readable.

## Downloaded (real API samples)

- `raw/api/rpo_search_31333532.json`, `raw/api/rpo_entity_937053.json` — ESET full RPO record
- `raw/api/ruz_uctovna_jednotka_154048.json` — ESET RÚZ accounting unit (26 statements)
- `raw/api/ruz_zavierka_6500234.json` — ESET 2024 statement metadata
- `raw/api/ruz_vykaz_9793753.json` — ESET report (PDF-only, empty obsah)
- `raw/api/ruz_vykaz_populated_7221914.json` — report with **structured tables** (template 687)
- `raw/api/ruz_sablona_687.json` — template "Úč MUJ" (line-item labels)
- `normalized/companies.sample.jsonl` — one joined record (RPO identity + RÚZ financial availability)
