# Company data sources for Slovakia (SK)

## Status

- Official bulk data: **found** — both official registers expose full open APIs (iterable to bulk).
- Official API: **found** — RPO (`api.statistics.sk`) and RÚZ (`registeruz.sk`).
- Open data portal: **found** — cataloged on data.gov.sk / data.slovensko.sk.
- License: **known** — RPO is **CC-BY 4.0**; RÚZ Open API is **CC0** (public domain).
- Recommended ingestion path: **API iteration (RPO by IČO/changes + RÚZ incremental by `zmenene-od`)**.

## Best source

Slovakia is **best-in-class fully-open** — two official, free, machine-readable
registers that together give a very rich profile, joined on **IČO**:

1. **RPO — Register právnických osôb** (Štatistický úrad SR / Statistics Office),
   `https://api.statistics.sk/rpo/v1/`. The single public register consolidating
   the commercial register, trade register, etc. Returns identity, legal form,
   **activities**, **statutory bodies (officers)**, **stakeholders
   (shareholders)**, **share capital (equities/deposits)**, predecessors, and
   **full name/address history**. License **CC-BY 4.0**. (Officer/shareholder
   data is **personal data** — redact.)
2. **RÚZ — Register účtovných závierok** (Register of Financial Statements,
   Ministry of Finance), `https://www.registeruz.sk/cruz-public/api/`. Accounting
   units (IČO, DIČ, name, address, SK NACE, dates) **plus full structured
   financial statements** — balance sheet (Súvaha) and income statement (Výkaz
   ziskov a strát) as positional data tables decoded via templates (`sablona`).
   License **CC0**.

## Next action

Iterate RÚZ incrementally (`uctovne-jednotky?zmenene-od=…` → `uctovna-jednotka`
→ `uctovna-zavierka` → `uctovny-vykaz`, decode tables with `sablona`), and enrich
identity/officers/ownership from RPO by IČO. Join on **IČO**. Redact statutory-
body / stakeholder / deposit personal data per GDPR.
