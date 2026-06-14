# Cyprus — Source Inventory

| Source | Type | Access | Format | License | Status |
|---|---|---|---|---|---|
| **DRCIP Registrar (open CSV + eSearch)** | Official registry | Free | **CSV**/HTML | Open data | **recommended** (open spine, incl. **officers**) |
| **HE32 + audited financial statements** | Official financials | Paid (€10 detailed search) | **PDF** (scanned) | Public/paid | blocked by payment (**financials**) |
| data.gov.cy | Open data portal | Free | CSV/JSON | Per dataset | useful secondary (discovery) |
| UBO register (beneficial ownership) | BO register | Restricted/fee | HTML | Restricted | blocked by authentication |
| Tax Department (TIC/VAT) | Official tax | Free (VIES) | HTML | Validation | useful secondary |
| OpenSanctions cy_companies | Open data mirror | Free | JSON/FTM | CC-BY-NC | useful secondary (cross-ref) |
| Commercial aggregators (CyprusRegistry, Kyckr) | Commercial API | Paid | JSON/PDF | Commercial | useful secondary (structured financials) |

## Access points

- DRCIP register: https://www.companies.gov.cy/ ; free eSearch https://efiling.drcor.mcit.gov.cy/DrcorPublic/SearchForm.aspx ; open CSV via https://www.data.gov.cy/en/group/30
- Financials (HE32 + audited statements): detailed search €10/company (scanned PDFs) via the register
- National portal: https://www.data.gov.cy/ ; OpenSanctions mirror: https://www.opensanctions.org/datasets/cy_companies/

## Key facts

- **Registry OPEN**: DRCIP CSV on data.gov.cy (companies + **officers**, ~567k companies) + free eSearch.
- **Financials PAID + PDF**: HE32 audited statements via a **€10 detailed search** (scanned) — not structured open.
- **Single key**: HE registration number (prefix encodes type: HE=company); TIC = tax id; VAT = `CY`+8 digits+letter.
- data.gov.cy CSV resource URL unresolved here (non-standard CKAN path); resolve via the portal.

See `source_inventory.json` for the machine-readable version.
