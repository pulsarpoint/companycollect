# Armenia Source Inventory

| Source | Slug | Org | Access | Format | Status | Role |
|---|---|---|---|---|---|---|
| State Register (e-register.am) | state_register_eregister | Ministry of Justice | public search, Radware bot-protected | HTML | blocked_by_authentication | Authoritative register (reg. number, TIN, directors) |
| SRC taxpayer search | src_taxpayer_search | State Revenue Committee | browser-public per-TIN search | HTML | useful_secondary_source | Taxpayer name/status by TIN (HVHH) |
| Armenia Securities Exchange | amx_listed | AMX | browser-public SPA | HTML | useful_secondary_source | Listed securities (ISIN) |
| Open Data Armenia | opendata_armenia | civic (CKAN) | open API | JSON/CSV | not_company_data | No company register (research/sectoral only) |

## Notes

- **No open bulk file or free API** was found. The authoritative **State Register**
  (`e-register.am`) is **Radware Bot Manager-protected** (perfdrive validation).
- **SRC** (`src.am`) offers a **browser-public taxpayer search** by **TIN (ՀՎՀՀ/HVHH)**
  (`/searchTaxpayerData`) returning name/status — per-TIN, not bulk.
- **AMX** lists securities but is a **JS SPA** with no clean public JSON API found.
- **Open Data Armenia** (CKAN) has **no company register** dataset.
- **data.gov.am** and **petakamutner.am** do **not resolve** (NXDOMAIN here).
- **Identifier**: the **8-digit TIN (ՀՎՀՀ/HVHH)** is the universal key (SRC + register); ISIN
  for listed (AMX).
- Directors/founders (State Register) are personal data — redact.
