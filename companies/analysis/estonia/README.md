# Company data sources for Estonia

## Status

- Official bulk data: **found** (e-Business Register open data — RIK; CSV/JSON/XML/Parquet, daily)
- Official API: **found** (~16 XML/REST services, real-time)
- Open data portal: **found** (avaandmed.ariregister.rik.ee; national avaandmed.eesti.ee)
- License: **known — Creative Commons Attribution 4.0 (CC-BY 4.0)**
- Recommended ingestion path: **bulk download** (full population + financials), API for real-time lookups

## Best source

The **e-Business Register open data** (Äriregister, run by RIK) is one of the best company open-data sources in
the world. Since 1 October 2022 everything is **free** under **CC-BY 4.0**, keyed on the **registrikood**
(8-digit registry code). It uniquely publishes, as open bulk data:

- **Company data** (basic `lihtandmed` CSV + deeper `üldandmed` JSON) — verified: 373,025 companies.
- **Structured financial statements** (`majandusaasta aruanne`) — report metadata + **balance-sheet/income-
  statement line items** with XBRL-style element names + values, per year 2019–2025, + revenue by activity.
- **Beneficial owners** (`kasusaajad`) and **shareholders** (`osanikud`) as open bulk — unusual in the EU.
- **Officers / persons on the registry card**, registry cards, commercial pledges, court rulings.

## Next action

Ingest the bulk datasets keyed on registrikood: basic/general company data + the annual-report
`elemendid` financial line items (join `report_id` → `registrikood`) + beneficial owners + shareholders.
Attribute RIK per CC-BY 4.0; treat owner/officer personal data under GDPR.
