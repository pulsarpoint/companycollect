# Ireland — Source Inventory

| Source | Type | Access | Format | License | Status |
|---|---|---|---|---|---|
| **CRO Open Data — Company Records** | Official registry | Free | **CSV/zip** | CC-BY 4.0 | **recommended** (open spine) |
| **CRO Open Data — Financial Statements (index)** | Official financials | Free | **CSV** | CC-BY 4.0 | **recommended** (**filings index**) |
| CRO document retrieval (financial PDFs) | Official financials | Paid (per call) | PDF | Public doc | blocked by payment (**figures**) |
| CORE company search | Official registry | Free | HTML | Public | useful secondary (manual lookups) |
| RBO — beneficial ownership | BO register | Restricted | HTML | Restricted | blocked by authentication |
| Revenue / VIES (IE VAT) | Official tax | Free | SOAP | Validation | useful secondary |
| data.gov.ie | Open data portal | Free | CSV/JSON | CC-BY 4.0 | useful secondary (mirror) |

## Access points

- Open data portal: https://opendata.cro.ie/ (CKAN `/api/3/action/`)
- Company Records: `.../resource/3fef41bc-b8f4-4b10-8434-ce51c29b1bba/download/companies.csv.zip`
- Financial Statements: `.../download/financial_statements.csv` (2022), `.../financial_statements_2023.csv` (2023)
- Document retrieval (PDF): via opendata.cro.ie, registered account, pay-per-call
- CORE search: https://core.cro.ie/ ; RBO: https://rbo.gov.ie/ ; VIES: https://ec.europa.eu/taxation_customs/vies/
- National portal: https://data.gov.ie/dataset/companies

## Key facts

- **Single join key**: **CRO number** (`company_num`). VAT (`IE` + 7 digits + 1–2 letters) is **not** in the CRO data → VIES/Revenue.
- **Fully open** under **CC-BY 4.0** (launched late 2024): Company Records (817,068 companies; NACE, eircode, status, dates) + Financial Statements **index** (121,387 filings in 2023).
- **Financial figures are paid**: the open dataset is the filings index; the actual PDFs are pay-per-call (document retrieval). Small/micro file abridged accounts. EUR.
- **Verified live**: downloaded both datasets (real counts above).
- **RBO** (beneficial ownership) restricted post-CJEU.

See `source_inventory.json` for the machine-readable version.
