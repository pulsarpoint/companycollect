# Company data sources for Ireland

## Status

- Official bulk data: **found** (CRO Open Data Portal — Company Records + Financial Statements index, CSV bulk)
- Official API: **found** (CKAN API at opendata.cro.ie; document-retrieval API pay-per-call)
- Open data portal: **found** (opendata.cro.ie; mirrored on data.gov.ie / data.europa.eu)
- License: **known — Creative Commons Attribution 4.0 (CC-BY 4.0)**
- Recommended ingestion path: **bulk download** (Company Records + Financial-Statements index), pay-per-call for the actual financial PDFs

## Best source

The **CRO Open Data Portal** (`opendata.cro.ie`, launched late 2024 under **CC-BY 4.0**, Open Data Directive)
is the authoritative open source, keyed on the **CRO number** (`company_num`). Two open datasets, both verified
by real download:

- **Company Records** — full register, **817,068 companies** (current + dissolved): name, status, type,
  registration/dissolution dates, address + eircode, **NACE Rev.2 activity**, next annual return date. Daily
  snapshot, bulk CSV + CKAN API.
- **Financial Statements** — an **open index of filed accounts** (121,387 filings in 2023): company_num,
  submission, PDF file name, filing dates, accounts-to date. The **actual financial-statement PDFs** are
  retrieved **pay-per-call** by registered account holders (the figures are not in the open CSV).

So Ireland is **fully-open for company identity** and **open for the financial-filings index**, with the
financial **figures** behind a per-document fee. VAT (IE…) is not in the CRO data — validate via VIES.
Beneficial ownership (RBO) is restricted.

## Next action

Ingest the Company Records CSV (keyed on company_num) + the per-year Financial-Statements index; fetch financial
PDFs pay-per-call (or use a commercial provider) for structured figures. Attribute the CRO (CC-BY 4.0); treat
any officer/owner data under GDPR. Source VAT separately (VIES/Revenue).
