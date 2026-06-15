# Canada — Source Inventory

| Source | Slug | Type | Access | License | Format | Status |
|---|---|---|---|---|---|---|
| Corporations Canada — Federal Corporations | corporations_canada_federal | official_registry | public | OGL Canada | csv | recommended |
| Corporations Canada — real-time API | corporations_canada_api | official_registry | public | OGL Canada | json/xml | useful_secondary_source |
| SEDAR+ — reporting-issuer financials | sedar_plus | official_financial | public (per-issuer) | public disclosure | pdf | useful_secondary_source |
| Provincial registries (REQ, OrgBook, …) | provincial_registries | official_registry | mixed | varies | csv/json | useful_secondary_source |

## Best combination

**Corporations Canada Federal Corporations** (free, OGL, federal-only) keyed on
**corporation number** (+ **BN** tax id); add **provincial** registries (Québec
REQ, BC OrgBook, …) for provincial companies; **SEDAR+** for reporting-issuer
financials. **No single national register** — federal + 13 provincial.

## Downloaded (real)

- `raw/bulk/corporations-active-cbca-en.csv` — 102 MB, **642,720** active CBCA corporations + metadata/sha256
- `raw/samples/federal_corp_sample.json` — real record (MINDANGLER CAPITAL INC., corp # 8660115)
- `normalized/companies.sample.jsonl` — one real normalized record
