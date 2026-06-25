# Hong Kong Source Inventory

| Source | Slug | Org | Access | Format | Status | Role |
|---|---|---|---|---|---|---|
| CR Newly Inc./Reg. Companies (Open Data) | cr_open_data_newly_registered | Companies Registry | open (CSV) | CSV/XLS | recommended | Incremental company list (BR Number, names, dates) |
| ICRIS e-Search | icris_esearch | Companies Registry | interactive, pay-per-use | HTML/PDF | blocked_by_payment | Authoritative full register (CR Number, particulars, directors) |
| HKEX List of Securities | hkex_securities | HKEX | browser-public | XLSX | useful_secondary_source | Listed stocks (static xlsx is a template) |
| data.gov.hk (CKAN) | data_gov_hk | OGCIO | open API | JSON/CSV | useful_secondary_source | Catalog → enumerate CR RNC063 resources |

## Notes

- **CR open data (RNC063)** is the genuinely open feed: weekly CSV/XLS, fully open, **no
  personal data** (company name + BR Number + dates). RNC063L = local incorporations;
  RNC063F = non-HK registrations; both include name changes. **Incremental**, not the full
  register. Identifier = **BR Number** (IRD), not the CR Company Number.
- **ICRIS** is the authoritative **full** register (CR Company Number + directors + charges)
  but document/particulars search is **pay-per-use**; no open bulk/API.
- **HKEX** static `ListOfSecurities.xlsx` returns a **template skeleton** for automated
  requests; populated server-side.
- **data.gov.hk** CKAN `package_search` is the access path to enumerate the weekly RNC063
  resource URLs.
- **Two identifiers**: CR Company Number (ICRIS) vs BR Number (IRD; in the open feed).
