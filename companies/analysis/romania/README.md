# Company data sources for Romania

## Status

- Official bulk data: **found** — ONRC full company register published openly on data.gov.ro (OD_FIRME + 5 companion CSVs).
- Official API: **found** — ANAF financial-statements web service (`/bilant`) returns structured per-company financials (2014–2024) as JSON, free.
- Open data portal: **found** — data.gov.ro (ONRC as publisher), regularly refreshed (monthly+).
- License: **open, but exact license text not stated on the dataset page** — data.gov.ro operates under Romanian open-data terms; confirm attribution before redistribution.
- Recommended ingestion path: **bulk download (register) + per-CUI API enrichment (financials)**.

## Best source

Two official sources combine into a rich profile:

1. **ONRC OD_FIRME** (data.gov.ro) — the **complete** trade register as a single
   CSV: **4,116,356 companies** (DENUMIRE, CUI, COD_INMATRICULARE, EUID,
   FORMA_JURIDICA, full address, WEB). Companion open CSVs add per-company status
   (OD_STARE_FIRMA), authorized activities (OD_CAEN_AUTORIZAT), legal
   representatives (OD_REPREZENTANTI_LEGALI — **PII**), and foreign branches.
2. **ANAF `/bilant` web service** — free, official, structured **financial
   statements** by CUI and year (turnover, revenue, expenses, gross/net profit,
   employees, fixed/current assets, liabilities, equity, …). Verified live for
   2019–2024.

Romania is therefore a **best-in-class fully-open** country: a complete
identified register **and** structured financials, both from official sources,
both free. The two join via the CUI present in OD_FIRME.

## Next action

Ingest OD_FIRME (+ companion CSVs) on the `COD_INMATRICULARE` join key, then
enrich each CUI from the ANAF `/bilant` service (browser User-Agent required;
max 1 req/sec). Redact representative/officer PII per GDPR in any published view.
