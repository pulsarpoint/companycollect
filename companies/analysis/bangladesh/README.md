# Company data sources for Bangladesh

## Status

- Official bulk data: not found (RJSC register gated/paid; no open register bulk)
- Official API: not found; DSE is open browser-HTML (parseable, not an API)
- Open data portal: found (data.gov.bd, DKAN) but no company register (statistical only)
- License: restricted (RJSC); public disclosure (DSE/CSE)
- Recommended ingestion path: parse DSE listed companies; RJSC gated/manual for the register

## Best source

The **Dhaka Stock Exchange (DSE)** (`dsebd.org`) is the cleanest open Bangladeshi company
source: `company_listing.php` lists **~640 listed instruments** (637 trading-code + name pairs
parsed, e.g. `AAMRANET` / aamra networks limited, `ACMELAB` / The ACME Laboratories Limited),
and each company has a browser-public detail page `displayCompany.php?name=<CODE>` with
**Trading Code, Scrip Code, Sector, Authorized Capital (mn), Paid-up Capital (mn), Listing
Year, Market Category, Type of Instrument** — plain parseable HTML, no auth/payment.

The authoritative **full** register is **RJSC** (Registrar of Joint Stock Companies and
Firms, `roc.gov.bd`), keyed on the **RJSC registration number** — but it has a TLS
certificate issue, its eservices portal hosts a free name search with **pay-per-use**
documents, and there is no open bulk/API. **NBR** adds tax identity (**BIN / e-TIN**);
**data.gov.bd** (DKAN) carries statistics, not a register.

## Next action

Parse the DSE listed-companies listing + detail pages (keyed on the trading code) for the
open listed layer. For the full register (RJSC number, directors, all companies), use RJSC
eservices (name search free; documents pay-per-use — do not bypass payment). Use NBR for
BIN/e-TIN. Redact directors (personal data). Currency BDT.
