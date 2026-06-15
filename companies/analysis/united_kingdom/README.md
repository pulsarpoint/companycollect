# Company data sources for United Kingdom (GB)

## Status

### Company registry data
- Official bulk data: **found (open)** — Companies House Free Company Data Product (CSV, monthly).
- Official API: **found** — Companies House REST API (free key), plus a Streaming API.
- Open data portal: **found** — download.companieshouse.gov.uk + data.gov.uk.
- License: **known** — **Open Government Licence (OGL)** / Crown copyright reuse.
- Recommended ingestion path: **bulk CSV register + bulk iXBRL accounts, keyed on company number**.

### Financial data
- Official open data: **found** — Companies House **Accounts Bulk Data** (iXBRL/XBRL, daily + monthly),
  free. Structured financial statements (turnover, profit, net assets, …) tagged to the FRC taxonomy.

## Best source

**Companies House** — the official UK register. The UK is the **only major economy where the register,
bulk data, API, and accounts are completely free**:

1. **Free Company Data Product** — full register of live companies (~5.9M) as CSV ZIPs (7 parts or one
   470 MB file), 55 columns: company number, name, full address, category (legal form), status,
   incorporation/dissolution dates, **SIC activity codes**, accounts metadata, charges, previous names.
   **OGL**, monthly snapshot.
2. **Accounts Bulk Data** — **iXBRL** financial statements of electronically-filed accounts (~60–75% of
   filings), daily + monthly ZIPs; structured facts (TurnoverRevenue, ProfitLoss, NetAssets, Equity, …)
   keyed on company number (in the filename + `bus:UKCompaniesHouseRegisteredNumber`). Free.
3. **REST API** (free key, 600 req/5 min) — company profile, **officers**, **PSC (beneficial owners)**,
   filing history, charges, documents.
4. **PSC snapshot** — persons-with-significant-control bulk (beneficial ownership), free.

## Next action

Bulk-load the register CSV and the accounts iXBRL (join on **company number**); add PSC for beneficial
owners and the REST API (free key) for officers/filing history. Person data (officers/PSC) is personal
data — handle per UK GDPR.
