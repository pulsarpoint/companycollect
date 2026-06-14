# Romania — Source Inventory

| Source | Slug | Type | Access | License | Format | Status |
|---|---|---|---|---|---|---|
| ONRC register — OD_FIRME (data.gov.ro) | onrc_od_firme | official_registry | public | open | csv | recommended |
| ONRC firm status — OD_STARE_FIRMA | onrc_od_stare_firma | official_registry | public | open | csv | recommended |
| ONRC authorized CAEN — OD_CAEN_AUTORIZAT | onrc_od_caen_autorizat | official_registry | public | open | csv | useful_secondary_source |
| ONRC legal reps — OD_REPREZENTANTI_LEGALI (PII) | onrc_od_reprezentanti_legali | official_registry | public | open (PII) | csv | useful_secondary_source |
| ONRC foreign branches — OD_SUCURSALE_ALTE_STATE_MEMBRE | onrc_od_sucursale_alte_state_membre | official_registry | public | open | csv | useful_secondary_source |
| ANAF financial statements (bilant) | anaf_bilant | official_tax | public | public info | json | recommended |
| ANAF VAT/fiscal-info (ws/tva) | anaf_ws_tva | official_tax | public | public info | json | useful_secondary_source |
| ONRC Beneficial Ownership Register (RBR) | onrc_rbr | beneficial_ownership | restricted | restricted | — | blocked_by_authentication |
| ONRC portal / RECOM (portal.onrc.ro) | onrc_portal_recom | official_registry | paid | paid | pdf/html | blocked_by_payment |

## Best combination

**OD_FIRME** (complete identified register, 4.1M companies) joined to the
**ANAF /bilant** web service (structured financials, 2014–2024) — both official
and free. Companion CSVs add status, CAEN activities, officers (PII), and
branches. Join the register companion files on **COD_INMATRICULARE**; bridge to
ANAF on **CUI** (present in OD_FIRME).

## Downloaded

- `raw/bulk/od_firme.csv` — 643 MB, 4,116,357 rows (full register) + metadata/sha256
- `raw/bulk/od_stare_firma.csv` — 89 MB (per-company status codes)
- `raw/samples/od_caen_autorizat_head.csv`, `od_reprezentanti_legali_head.csv`, `od_sucursale_head.csv`
- `raw/api/anaf_bilant_14399840_{2019,2021,2023,2024}.json` — real financials (Dante International SA)
- `normalized/companies.sample.jsonl` — one real joined record (register + financials)
