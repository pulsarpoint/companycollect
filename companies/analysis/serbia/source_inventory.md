# Serbia — Source Inventory

| Source | Type | Access | Format | License/terms | Representative coverage | Status |
|---|---|---|---|---|---|---|
| APR Companies Open Data API | Official registry snapshot | Public GET, no auth | JSON | SODL (`sodl`) | None | **recommended for company core** |
| APR one-off status data delivery | Official paid delivery | Request + fee | XLS/XLSX; MDB by request | APR paid-service terms | SP3 legal reps; SP4 other reps/procurists/boards | **recommended for backfill** |
| APR automated web service | Official contracted service | Auth + contract + fee | Protocol not publicly documented | Contract | Selected status groups and daily changes | **recommended for incremental representatives** |
| APR public company search | Official public UI | Manual browser use; reCAPTCHA | HTML/UI | APR site terms | Individual verification only | sample_only |
| APR Central Register of Beneficial Owners (CEV) | Official beneficial-owner register | eID/SSO or separate contracted service | Protocol not publicly documented | Statutory/contract terms | Beneficial owners; not SP3/SP4 | blocked_authentication |
| APR Financial Statements Open API | Official registry snapshot | Public GET, no auth | JSON | SODL (`sodl`) | None | useful_secondary_source |
| APR NGO Open API | Official registry snapshot | Public GET, no auth | JSON | SODL (`sodl`) | Not company representatives | useful_secondary_source |
| OpenCorporates Serbia | Third-party aggregator | Search public; API/bulk restricted | HTML/JSON | OpenCorporates terms | Officers may be incomplete | useful_secondary_source |

## Key facts

- Live APR company snapshot: `2026-07-31`, 133,634 records.
- Company primary key: eight-digit `matični broj`.
- The open payload has seven nested fields and no representatives.
- APR's 2026 price schedule makes SP2 mandatory when ordering SP3-SP6.
- Public-search scraping is not permitted; use APR's delivery services.
- A manual one-company spot check confirmed representative name, function,
  masked JMBG availability and independent-representation concepts. No personal
  identifier was revealed or saved.
- Beneficial owners are held in separate CEV. Company members/shareholders are
  not automatically CEV beneficial owners and must not be mapped as such.
- No CEV record was accessed because the portal requires eID/SSO. The planning
  schema comes from current APR documentation and the 2025 Act.
- Current data.gov.rs metadata labels the open dataset with the Serbian Open
  Data License, not public domain.
