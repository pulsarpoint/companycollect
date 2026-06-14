# Company data sources for Cyprus

## Status

### Company registry data — OPEN (CSV)
- Official bulk data: **found** — the **Registrar of Companies (DRCIP)** publishes **open CSV** files on
  **data.gov.cy** (company list incl. **officers**; ~567k companies)
- Official API: **partial** — free **eSearch** (basic info by name/HE number); no documented open JSON API
- Open data portal: **found** (data.gov.cy — DRCIP group #30)
- License: **open data** (data.gov.cy; confirm the exact licence) — OpenSanctions mirrors it as CC-BY-NC
- Recommended ingestion path: **data.gov.cy CSV** (open company master), enriched via the free eSearch

### Financial data (HE32 + audited financial statements) — PAID, document-based
- Official bulk data: **not found** (no open structured bulk of financials)
- Official API: **not structured** — financial statements are filed with the annual return (HE32) and
  accessible only via a **paid detailed search (€10/company)** as **scanned PDFs**
- Format: **PDF/scanned** (no XBRL/CSV of figures) — extraction needs OCR/parsing
- Recommended ingestion path: paid per-company **detailed search** (€10) + OCR, or a **commercial provider**

## Best source

The authoritative source is the **Department of Registrar of Companies and Intellectual Property (DRCIP)**.
Company **identity** is **open**: the Registrar publishes **CSV** files on **data.gov.cy** (a list of all
registered organisations + their **officers**, ~567k companies), and a free **eSearch** by name/HE number.
But **financials** (the audited statements filed with the **HE32** annual return) are **public only via a
paid €10 detailed search** as **scanned PDFs** — not structured open data. Everything keys on the **HE
registration number**. For structured financials at scale, a commercial provider is the realistic route.

## Next action

1. Ingest the **DRCIP open CSV** from **data.gov.cy** (companies + officers) as the open company master
   (resolve the resource URL via the portal — data.gov.cy/en/group/30).
2. Enrich via the free **eSearch** (basic info) where needed.
3. **Financials:** decide between paid per-company **detailed search (€10) + OCR** of the audited statements,
   and a **commercial provider** with structured financials.
4. Confirm the data.gov.cy licence + UBO access conditions.

See `investigation.md` for detail and `source_inventory.md` for the table.
