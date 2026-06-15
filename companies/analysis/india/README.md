# Company data sources for India

## Status

- Official bulk data: **found** (MCA Company Master Data, republished on data.gov.in / OGD — state-wise, open)
- Official API: **found** (data.gov.in OGD REST API; free API key; public sample key works)
- Open data portal: **found** (data.gov.in — Open Government Data Platform India)
- License: **known** — Government Open Data License – India (GODL-India): free reuse incl. commercial, attribution required
- Recommended ingestion path: **API** (data.gov.in OGD resource API) — pull the MCA Company Master Data resources state-by-state

## Best source

**MCA Company Master Data** (Ministry of Corporate Affairs), republished on
**data.gov.in** under GODL-India and served via the **OGD REST API**
(`api.data.gov.in/resource/{resource_id}`). Every Indian company is keyed by its
**CIN (Corporate Identification Number, 21-char)**, which encodes listing status,
industry code, state, incorporation year, company type, and the RoC sequence
number. The dataset gives identity, status, class/category, **authorized & paid-up
capital**, principal business activity, registrar, and registered address.

Verified live: enumerated **128 "Company Master Data" resources** (state × year,
2015–2021) and pulled real records via the OGD API with the public sample key —
e.g. CIN `L20101NL1985PLC002284` (SANGRAHALAYA TIMBER AND CRAFTS LTD, ACTIVE) and
2021 Mizoram records.

## Financial data

The master data includes **authorized & paid-up capital** and **latest filing-year
markers** (`latest_year_ar` annual return, `latest_year_bs` balance sheet) but
**not actual financial statements**. Full annual financials (AOC-4 / XBRL) are
filed with MCA and are **pay-per-document** on the MCA21 portal (not open bulk).
For **listed** companies, financials are openly available from BSE/NSE/SEBI
disclosures.

## Caveats

- The data.gov.in snapshots are **point-in-time** ("upto 31st March 2015/…/2021"),
  not a live feed. The live register is on mca.gov.in (WAF-blocked here, 403).
- Records carry a company **contact email** (often a personal gmail) — treated as
  personal data and **redacted** in committed samples.

## Next action

Ingest the MCA Company Master Data resources via the OGD API (one resource per
state×year), keyed on CIN; layer listed-company financials from BSE/NSE separately.
