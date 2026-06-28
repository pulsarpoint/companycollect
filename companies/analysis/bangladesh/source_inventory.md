# Bangladesh Source Inventory

| Source | Slug | Org | Access | Format | Status | Role |
|---|---|---|---|---|---|---|
| Dhaka Stock Exchange (DSE) | dse_listed | DSE | browser-public (parseable HTML) | HTML | recommended | Listed companies: trading code, sector, capital, listing year |
| RJSC Registrar | rjsc_register | RJSC (Ministry of Commerce) | name search free; documents paid | HTML | blocked_by_payment | Authoritative register (RJSC number, directors) |
| Chittagong Stock Exchange (CSE) | cse_listed | CSE | browser-public | HTML | useful_secondary_source | Listed companies (cross-check) |
| National Board of Revenue (NBR) | nbr_tax | NBR | browser-public verification | HTML | useful_secondary_source | BIN / e-TIN tax identity |
| data.gov.bd | data_gov_bd | a2i / Cabinet Division | open (DKAN) | CSV/XLSX | not_company_data | Statistics, not a register |

## Notes

- **DSE** is the genuinely open source: `company_listing.php` (~640 instruments; 637 parsed) +
  per-company `displayCompany.php?name=<CODE>` (Trading Code, Scrip Code, Sector, Authorized &
  Paid-up Capital (mn), Listing Year, Market Category, Type). Plain parseable HTML; key = DSE
  trading code; currency BDT.
- **RJSC** is the authoritative **full** register (key = RJSC registration number) but its
  documents are **pay-per-use** and the site has a **TLS cert issue** (eservices host did not
  resolve). No open bulk/API.
- **NBR** adds **BIN / e-TIN** tax identity (per-BIN/TIN verification).
- **CSE** is the second exchange (overlaps DSE).
- **data.gov.bd** (DKAN) is statistical — no company register.
- Directors (RJSC) are personal data — redact.
