# Vietnam — Source Inventory

| Source | Slug | Type | Access | License | Format | Status |
|---|---|---|---|---|---|---|
| NBRP per-company search | nbrp_search | official_registry | public (CAPTCHA) | restricted/unclear | html | blocked_by_authentication |
| NBRP bulk database (MOU) | nbrp_bulk_mou | official_registry | paid | paid/contract | unknown | blocked_by_payment |
| GDT taxpayer lookup | gdt_taxpayer_lookup | official_tax | public (CAPTCHA) | restricted/unclear | html | blocked_by_authentication |
| HOSE/HNX/SSC disclosure | hose_hnx_ssc_disclosure | stock_exchange | public (per-issuer) | issuer disclosure | pdf/xls | useful_secondary_source |
| GSO Enterprise Survey | gso_ves | statistical_office | restricted | research access | microdata | not_company_data |
| Aggregators | vn_aggregators | aggregator | search/paid | vendor terms | html/json | blocked_by_license_uncertainty |

## Best (constrained) path

There is **no lawful open bulk** for Vietnamese companies. The authoritative
**NBRP** per-company search (free, CAPTCHA-gated) provides verified identity;
full coverage needs the **paid NBRP MOU**. **Financials are listed-only**
(HOSE/HNX/SSC). Everything keys on the **enterprise code = tax code (10–13 digits)**.

## Downloaded

- `raw/pages/nbrp_home.html` — NBRP landing page (evidence: no open-data/bulk links)
- `normalized/companies.sample.jsonl` — **schematic** record (no open per-company
  bulk lawfully downloadable; search is CAPTCHA-gated)
