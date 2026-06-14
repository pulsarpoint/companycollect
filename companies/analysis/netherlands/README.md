# Company data sources for Netherlands

## Status

- Official bulk data: **found but anonymised** (KvK Open Data Set — basis + jaarrekeningen, CC-BY 4.0, no identifiers)
- Official API: **found** (free HVDS open-data API per KvK number; paid KvK Handelsregister API for identified data)
- Open data portal: **found** (data.overheid.nl catalogs the KvK open datasets)
- License: **known — CC-BY 4.0** (for the open datasets)
- Recommended ingestion path: **bulk for statistics/financials (anonymised)** + **paid KvK API (or HVDS API by KvK number)** for identified data

## Best source

The **KvK (Kamer van Koophandel)** runs the **Handelsregister**, keyed on the **KvK-nummer** (8 digits). Two
free **CC-BY 4.0** open datasets (EU High-Value DataSets), both verified by real download:

- **Basis bedrijfsgegevens** — bulk CSV, **1,891,639 records**: registration date, active/insolvency, legal form
  (BV…), postcode region, SBI activity codes. **Anonymised** (no KvK number, name, address, directors).
- **Jaarrekeningen** — bulk XML, **structured deposited annual accounts** (XBRL-derived): balance sheet (assets,
  equity, liabilities, provisions, share capital) + financial year. **Anonymised** (no identifier).

So the open data gives genuinely **structured financial statements** and rich statistics, but **anonymised** in
bulk. To get a company's **identity** (name, address, officers) you need the **paid KvK Handelsregister API**
(Basisprofiel etc.); the free **HVDS API** returns a company's basic data + jaarrekening **by a supplied KvK
number** (free with an API key). **UBO** (beneficial owners) is **restricted** (AML-obliged entities).

## Next action

Ingest the anonymised bulk (basis + jaarrekeningen) for statistics/financial benchmarks; for identified
companies use the **free HVDS API** (by KvK number, API key) and the **paid KvK API** / a commercial provider
(Company.info, Graydon) for names/officers + identified financials. Attribute KvK (CC-BY 4.0); VAT via VIES.
