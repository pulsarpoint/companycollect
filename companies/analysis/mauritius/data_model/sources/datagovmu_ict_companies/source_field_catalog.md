# data.govmu.org — List of ICT Companies in Mauritius Field Catalog

## Source Summary

- Country: Mauritius
- Source type: company_directory
- Organization: Mauritius (publisher MDPA), via the national open-data portal (CKAN)
- URL: https://data.govmu.org/dataset/list-ict-companies-mauritius
- License: **CC-BY-SA-4.0**
- Access: **public open CSV** (no auth/payment)
- Freshness: static (one-off list)
- Record shape: CSV, one row per ICT company
- Primary keys: Title (company name) — no identifier in this dataset
- Join keys: Title

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| Title | Title | Company name | string | legal_name | A CHAMROO LTD | only key (name) |
| Address | Address | Street address | string | address | Best House Glaieul Av Quatre Bornes | may be blank |
| District | District | District/region | string | geography | Plaine Wilhems, Port Louis | |
| Sectors | Sectors | ICT sector(s) | string | activity | Software Development | newline-separated |
| Other Related Sectors | Other Related Sectors | Related sectors/free text | string | activity | Not Available | often empty |

## Interpretation Notes

- A genuinely **open** (CC-BY-SA-4.0) **sectoral directory** of **ICT companies** —
  **1,060 rows**. It carries **no identifier** (no BRN), no status, and no incorporation
  date; the only company key is the **name** (`Title`).
- **Encoding is Windows-1252 (cp1252)**, not UTF-8 — decode accordingly. `Sectors` is
  **newline-separated free-text** within the cell (split to get multiple sectors). `Address`
  may be `Not Available` or blank.
- **Coverage is partial** (ICT sector only) — this is **not** a full company register. To
  obtain identifiers/status, join by name to the CBRD CBRIS register (Turnstile-gated).
- **No personal data** — company name + address + sector only. Safe to store real values.
- A real sample is saved at `raw/bulk/ict_companies.csv`; `sample_record.json` included.
