# Company data sources for Belgium

## Status

### Company registry data — OPEN bulk
- Official bulk data: **found** — **KBO/BCE Open Data**: free monthly/daily **CSV bulk** of all active
  enterprises + establishment units (full copy + update file). Free **registration** + terms acceptance.
- Official API: **paid** (KBO Public Search Web Service ~€50/2000) + **free web** Public Search; free
  third-party REST mirrors (API-key gated)
- Open data portal: **found** (data.gov.be lists the KBO open data)
- License: **known** — Licence-BCE-Open-Data (reuse allowed; **personal data not for direct marketing**)
- Recommended ingestion path: **KBO Open Data bulk CSV** (the company master), via portal or SFTP

### Financial data (annual accounts) — OPEN, structured XBRL
- Official bulk data: **found** — **NBB Central Balance Sheet Office**: annual accounts **free** to the
  public; **XBRL** (since 2007) / **CSV** (since 2022) / PDF (since 1999)
- Official API: **found, free** — NBB CBSO **web services** (Authentic Data Query + Daily Extract are
  free; "Improved Data" paid) via a **free account** at developer.cbso.nbb.be; **CONSULT** for free
  per-entity download
- Format: **XBRL** (full structured balance sheet + income statement; Belgian GAAP micro/abbreviated/full)
- Recommended ingestion path: **NBB XBRL/CSV** (free), triggered/joined on the Ondernemingsnummer

## Best source

Belgium is **top-tier open** — comparable to Poland/France/Norway, with one of the **best structured
financial stories**. The **KBO/BCE Open Data** gives a free **bulk CSV company master** (all enterprises +
establishments, NACE activities, addresses), and the **NBB Central Balance Sheet Office** gives **free,
structured XBRL annual accounts** for essentially all legal entities (≈99% XBRL, back to 2007). Both are
**free** but behind a **free registration/account** (not payment). Everything joins on the
**Ondernemingsnummer** (which is also the VAT root).

## Next action

1. Register (free) for **KBO Open Data**; ingest the bulk CSV set (enterprise/establishment/denomination/
   address/activity/contact/code) as the company master + daily update file.
2. Register (free) for the **NBB CBSO** developer account; pull **XBRL/JSON annual accounts** via the
   Authentic Data web services (or CONSULT/Extract), joined on the Ondernemingsnummer.
3. Parse the NBB XBRL (Belgian GAAP schemas) into balance sheet + income statement.
4. Confirm the KBO open-data license conditions (no direct-marketing reuse of personal data).

See `investigation.md` for detail and `source_inventory.md` for the table.
