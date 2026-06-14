# Company data sources for Bulgaria

## Status

### Company registry data — partial open
- Official bulk data: **partial** — no anonymous full bulk; the Registry Agency's **daily publications**
  are open on **data.egov.bg under CC-BY**; a **full database** for commercial reuse needs a **data-sharing
  agreement**
- Official API: **found** — free **public search** (portal.registryagency.bg) + an official Commercial
  Register **web service** (registration/contract for integration)
- Open data portal: **found** (data.egov.bg — CC-BY; WAF-protected from automated access here)
- License: **known** — Registry Agency daily publications **CC-BY**; full bulk by agreement
- Recommended ingestion path: **data.egov.bg CC-BY daily publications** + official web service (registration)
  for the company spine; or a **data-sharing agreement** for the full bulk

### Financial data (ГФО / annual financial statements) — public but DOCUMENT-based
- Official bulk data: **not found** (no open structured bulk of financials)
- Official API: **not structured** — ГФО are filed to the Commercial Register and **public by 30 June**, but
  as **filed documents (PDF/scanned)**, not XBRL/CSV open data
- Format: **PDF documents** (balance sheet + income statement inside) — extraction needs OCR/parsing
- Recommended ingestion path: parse the filed ГФО documents, or use a **commercial provider** (CompanyBook,
  APIS) that already parses balance sheets + income statements

## Best source

The authoritative source is the **Търговски регистър** (Commercial Register / ТРРЮЛНЦ), run by the
**Агенция по вписванията** (Registry Agency). Identity data is **open-ish**: a free **public search**, an
official **web service** (registration for integration), and **CC-BY daily publications** on **data.egov.bg**
(a full bulk needs a data-sharing agreement). **Financials (ГФО)** are **public but document-based** (PDF in
the register), not structured open data — unlike Belgium/Poland. Everything keys on the **ЕИК** (Unified
Identification Code) = VAT root (`BG` + EIK). For structured financials at scale, a commercial provider
(CompanyBook/APIS) is the realistic route.

## Next action

1. Build the company spine from the **data.egov.bg CC-BY daily publications** + register on the Registry
   Agency portal for the **web service** (single lookups free; integration registered), or pursue a
   **data-sharing agreement** for the full bulk.
2. **Financials:** decide between parsing the filed **ГФО PDFs** (OCR) and a **commercial provider** with
   structured balance sheets/income statements.
3. Confirm the CC-BY attribution + the data-sharing terms before redistribution.
4. Note: data.egov.bg blocked automated access here — resolve resources via the portal UI / with an api_key.

See `investigation.md` for detail and `source_inventory.md` for the table.
