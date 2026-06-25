# Company data sources for Hong Kong

## Status

- Official bulk data: found (incremental open CSV feed) / full register not open
- Official API: found (data.gov.hk CKAN indexes the CR feed); no full-register API
- Open data portal: found (data.gov.hk)
- License: data.gov.hk terms (confirm); ICRIS restricted/paid
- Recommended ingestion path: **bulk** (CR weekly CSVs) + manual/paid for full particulars

## Best source

**Companies Registry — List of Newly Incorporated / Registered / Re-domiciled Companies
(Open Data on data.gov.hk)**. Official, open, no auth or payment — weekly CSV/XLS files
(RNC063 series): `RNC063L` for newly **incorporated local** companies and `RNC063F` for
newly **registered non-Hong-Kong** companies, plus name changes. Verified live
(`RNC063L_20241230.csv` = 3,286 rows). Fields: English name, Chinese name, **BR Number**
(IRD Business Registration number), incorporation/registration date, name-change date.
**No personal data** in this feed. It is **incremental**, not the full register.

The authoritative **full** register is **ICRIS e-Search** (CR Company Number, full
particulars, directors, charges) — but it is **interactive and pay-per-use**, with no open
bulk or free API. **HKEX List of Securities** covers listed stocks, but the static `.xlsx`
URL returns a template skeleton for automated requests (populated server-side).

## Next action

Ingest the CR weekly RNC063 CSVs (enumerate resource URLs via the data.gov.hk CKAN API) to
build/maintain an incremental company list keyed on the BR Number. Use ICRIS e-Search for
authoritative full particulars where a paid/interactive path is acceptable. Treat HKEX as a
listed-overlay via the browser. Note the two HK identifiers: **CR Company Number** (ICRIS)
vs **BR Number** (IRD; present in the open feed).
