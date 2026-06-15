# New Zealand — Source Inventory

| Source | Org | Type | Access | Formats | License | Status |
|---|---|---|---|---|---|---|
| NZBN API | Companies Office / MBIE | official registry | free subscription key (OAuth) | JSON | Crown copyright (publicly available data, reusable) | blocked_by_authentication (free key) |
| Companies Register | Companies Office / MBIE | official registry | public search (no bulk/API) | HTML, PDF | public register | useful_secondary_source |
| Disclose Register | Companies Office / FMA | financial disclosure | public search | HTML, PDF, XBRL | public register | useful_secondary_source |
| data.govt.nz | Stats NZ / DIA | open data portal | public (bot-protected) | HTML, CSV | varies (CC-BY) | not_company_data |

## Roles

- **nzbn_api** — authoritative open(ish) **identity** layer keyed on the 13-digit
  NZBN: name, type, status, registration date, source register + identifier
  (Companies Register company number), addresses, trading names, contacts, ANZSIC
  industry. Free subscription key required (verified 401 without one).
- **companies_register** — the company number, directors/shareholders (personal
  data), and filed documents incl. financial statements for entities required to
  file. Search-only.
- **disclose_register** — FMA register of FMC offers / managed investment schemes;
  financial statements + offer documents for the FMC-reporting subset.
- **data_govt_nz** — national catalogue; bot-protected; does not host the register
  openly.

## Join key

**NZBN (13-digit)** across all sources; **company number** links to the Companies
Register. IRD/GST numbers are not public; NZ uses GST, not VAT.
