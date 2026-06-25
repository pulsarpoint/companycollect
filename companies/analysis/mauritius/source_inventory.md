# Mauritius Source Inventory

| Source | Slug | Org | Access | Format | Status | Role |
|---|---|---|---|---|---|---|
| CBRD CBRIS Online Search | cbrd_cbris_search | CBRD / MNS | public search, Turnstile-gated; docs paid | HTML | blocked_by_authentication | Authoritative register (BRN, status, directors) |
| data.govmu.org ICT Companies | datagovmu_ict_companies | MDPA (CKAN) | open | CSV | recommended | Open sectoral directory (name, address, sector) |
| Stock Exchange of Mauritius | sem_listed | SEM | browser-public | HTML/PDF | useful_secondary_source | Listed companies (published accounts, announcements) |
| data.govmu.org portal | data_govmu_portal | Govt of Mauritius | open API (CKAN) | JSON/CSV | useful_secondary_source | Catalog (only the ICT directory is company data) |

## Notes

- **No open full-register bulk/API.** The authoritative **CBRD CBRIS** search
  (`onlinesearch.mns.global`) is **Cloudflare Turnstile-gated** and documents are **paid**.
- The only **open** company data is the **CC-BY-SA-4.0 ICT-companies CSV** on data.govmu.org
  (1,060 rows: name, address, district, sectors) — **sectoral, no identifiers/status**.
- **SEM** lists companies (Official Market + DEM) browser-public; no clean list/API.
- **Identifier**: the **BRN (Business Registration Number)** is the CBRD/CBRIS company key
  (not present in the open ICT directory).
- Directors/shareholders (CBRD) are personal data — redact.
- `opendata.govmu.org` / `catalogue.data.govmu.org` do not resolve; the working portal host
  is **data.govmu.org**.
