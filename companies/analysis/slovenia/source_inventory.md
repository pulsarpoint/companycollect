# Slovenia — Source Inventory

| Source | Slug | Type | Access | License | Format | Status |
|---|---|---|---|---|---|---|
| AJPES PRS — business register (OPSI) | ajpes_prs | official_registry | public | CC-BY 4.0 | csv | recommended |
| FURS — tax payers (legal entities) | furs_zavezanci_po | official_tax | public | CC-BY 4.0 | csv/zip | recommended |
| AJPES restPrsInfo — REST API | ajpes_restprsinfo | official_registry | restricted (creds) | AJPES terms | json/xml | blocked_by_authentication |
| AJPES JOLP — annual reports | ajpes_jolp | official_registry | public (view-only) | unclear | html/pdf | useful_secondary_source |
| AJPES Fi=Po / S.BON — financials | ajpes_fipo | official_registry | paid | paid | web/data | blocked_by_payment |

## Best combination

**AJPES PRS** (identity + address) + **FURS** (davčna, VAT, SKD activity), joined
on **matična številka** — both free, CC-BY 4.0. Financials are **view-only**
(JOLP) or **paid** (Fi=Po) — no open structured feed.

## Downloaded (real)

- `raw/bulk/opsiprs.csv` — 127 MB, 293,222 entities (UTF-16) + metadata
- `raw/bulk/DURS_zavezanci_PO.csv` — 144,537 legal entities (UTF-8 semicolon) + zip + metadata
- `raw/samples/prs_sample.json`, `raw/samples/furs_po_sample.json` — real joined entity
- `normalized/companies.sample.jsonl` — 3 real joined records (PRS + FURS)
