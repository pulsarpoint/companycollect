# Company data sources for Croatia

## Status

### Company registry data — OPEN (free registration)
- Official bulk data: **partial** — no anonymous full dump, but an **open REST API** (Sudski registar) and a
  data.gov.hr dataset under the Croatian Open Licence
- Official API: **found** — **Sudski registar API** (sudreg-data.gov.hr), open (Otvorena dozvola), **free
  registration** → Client ID/Secret + subscription key; JSON
- Open data portal: **found** (data.gov.hr / CKAN) — hosts Sudski registar + RGFI
- License: **known** — **Otvorena dozvola (OD)** (Croatian Open Licence)
- Recommended ingestion path: **Sudski registar API** (register for a key) for the company spine

### Financial data (annual accounts / RGFI) — OPEN, structured (free registration)
- Official bulk data: **found** — **FINA RGFI javna objava**: annual financial statements in
  **machine-readable CSV** (balance sheet + income statement, abbreviated; notes), open-licensed
- Official API: per-company download via the RGFI public-disclosure portal (free login); CKAN dataset on
  data.gov.hr
- Format: **CSV** structured (balance sheet + income statement) — esp. micro/small; fuller FINA products paid
- Recommended ingestion path: **RGFI javna objava CSV** (free registration), joined on OIB

## Best source

Croatia is **open** — Belgium-tier (free but behind a free registration). The **Sudski registar** (Court
Register, Ministry of Justice) offers an **open REST API** (JSON) with the company spine — MBS, OIB, name,
seat, share capital, legal form, status — under the **Croatian Open Licence**. **FINA's RGFI** (Register of
Annual Financial Statements) publishes annual accounts as **open, machine-readable CSV** (balance sheet +
income statement). Both are **free** but require a **free registration/account** (sudreg API key; FINA RGFI
login). Everything joins on the **OIB** (= VAT root, `HR` + OIB) and/or **MBS**.

## Next action

1. Register (free) for the **Sudski registar API** (sudreg-data.gov.hr) → ingest the company spine (by
   OIB/MBS), under the Otvorena dozvola.
2. Register (free) for **FINA RGFI javna objava** → download the **CSV** balance sheet + income statement,
   joined on OIB.
3. Confirm coverage limits of the open RGFI CSV (micro/small vs all) and whether fuller data needs the paid
   FINA product.
4. Confirm Otvorena dozvola attribution before redistribution.

See `investigation.md` for detail and `source_inventory.md` for the table.
