# Company data sources for Australia (AU)

## Status

- Official bulk data: **found (open)** — the ABR / ABN Lookup **Bulk Extract** (all ABN holders) on data.gov.au.
- Official API: **found** — ABN Lookup web services (free, GUID registration).
- Open data portal: **found** — data.gov.au (ATO publisher).
- License: **known** — **CC-BY 3.0 Australia** for the ABR bulk extract.
- Recommended ingestion path: **bulk XML (ABR Bulk Extract), keyed on ABN (and ACN for companies)**.

## Best source

**Australian Business Register (ABR) / ABN Lookup Bulk Extract** (Australian
Taxation Office, via data.gov.au). A **free, CC-BY 3.0 AU**, **weekly** bulk of
**every ABN holder** (companies, sole traders, trusts, partnerships, super funds,
government), as XML in two ~492 MB zips. Per record: **ABN** + status/date,
**entity type** (e.g. Australian Public Company), **legal name**, business/trading
names, **state + postcode**, **ACN/ARBN** (for companies), **GST** registration +
date, and DGR status. This is the open identity backbone — keyed on the **ABN**
(companies also carry an **ACN**).

## Financial data

**Mostly paid / limited.** Company financial reports are lodged with **ASIC** and
are available **per-document for a fee** via ASIC Connect — and **only certain
companies must lodge** (public companies, large proprietary companies, disclosing
entities, registered schemes). Most small proprietary companies **do not lodge**
financials publicly. **Listed companies** lodge with the **ASX** (announcements/
financial reports, publicly viewable). So open structured financials are
essentially **listed-only**; broader financials are paid via ASIC.

## Next action

Bulk-load the ABR Bulk Extract (2 zips), stream the `<ABR>` XML records, key on
**ABN** (+ ACN for companies). For company-register detail (registered office,
incorporation date, current status) and financials, use **ASIC** (paid per
extract) or the **ABN Lookup API** for free per-ABN enrichment; pull
listed-company financials from **ASX**. No separate VAT id (GST registration is
the indirect-tax flag).
