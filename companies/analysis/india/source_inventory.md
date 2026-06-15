# India — Source Inventory

| Source | Org | Type | Access | Formats | License | Status |
|---|---|---|---|---|---|---|
| MCA Company Master Data (data.gov.in / OGD API) | Ministry of Corporate Affairs (via NIC data.gov.in) | official registry | public, free API key | JSON, CSV, XML | GODL-India | **recommended** |
| MCA21 portal (master data & public documents) | Ministry of Corporate Affairs | official registry | free lookup + paid docs | HTML, PDF | restricted | blocked_by_authentication |
| MCA annual financials (AOC-4 / XBRL) | Ministry of Corporate Affairs | financial disclosure | paid per-document | XBRL, PDF | restricted | blocked_by_payment |
| BSE / NSE / SEBI listed disclosures | Exchanges / SEBI | financial disclosure | public (exchange terms) | XBRL, XLSX, PDF | exchange terms | useful_secondary_source |

## Roles

- **mca_company_master_data** — authoritative open **identity** layer: CIN, name,
  status, class/category, **authorized & paid-up capital**, business activity,
  RoC, registered address; 2021 adds industrial_class + filing-year markers.
  Served via the OGD REST API under GODL-India. Verified live (128 resources,
  state × year, 2015–2021).
- **mca_portal_master_data** — the live register (free per-CIN lookup; paid
  documents incl. directors/charges). WAF-blocked; documentation only.
- **mca_xbrl_financials** — full financial statements (AOC-4/XBRL), pay-per-doc;
  the only authoritative all-company financials. Not open.
- **bse_nse_listed_financials** — open financials for **listed** companies (CIN
  starts `L`) via the exchanges; join on CIN/ISIN.

## Join key

**CIN (21-char)** across all sources. PAN/GSTIN (tax/GST ids) are **not** in the
open data; India has GST, not VAT.
