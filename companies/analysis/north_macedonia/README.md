# Company data sources for North Macedonia

## Status

- Official bulk data: **paid, not open** — the Central Registry sells registry +
  financial data via its data-distribution service
- Official API: **paid distribution** (e-distribution); free public search for basic
  fields only
- Open data portal: `data.gov.mk` exists but does not host the full register
- License: registry data is commercially distributed by the Central Registry
- Recommended ingestion path: **paid Central Registry distribution** for
  bulk/financials; free per-company basic search for identity/status
- **Environment note:** the official hosts (`crm.com.mk`, `data.gov.mk`) **resolve
  via DNS but were unreachable (TCP/HTTP timeouts) from this environment**, and
  `ujp.gov.mk` returned 502 — so this investigation documents the model from
  established public documentation; **no live values were captured**.

## Best source

**Central Registry of North Macedonia** (Централен регистар на Република Северна
Македонија, **CRM**, `crm.com.mk`). It operates the **Trade Registry** (Трговски
регистар и регистар на други правни лица) and the **Registry of Annual Accounts**
(Регистар на годишни сметки). The CRM holds company identity (name, **ЕМБС**,
**ЕДБ**, legal form, status, address, activity, founders/managers) and is the
official **commercial distributor** of registry and financial data. A **free public
search** (Пребарување) returns basic existence/name/ЕМБС/status; **bulk extracts,
detailed data, and financial statements are paid** (subscription / per-document).

## Financial data — filed, paid

All companies file **annual accounts** (годишна сметка: **Биланс на состојба** /
balance sheet + **Биланс на успех** / income statement, in **MKD**) with the CRM's
**Registry of Annual Accounts**. These are available through the CRM's **paid
distribution** service; there is no open bulk financial dataset.

## Identifiers & tax

- **ЕМБС — Единствен матичен број на субјектот** — 7-digit unique entity
  registration number (company id).
- **ЕДБ — Единствен даночен број** — 13-digit tax number.
- **ДДВ број (VAT)** — VAT registration (PRO/UJP); MK VAT.
- Currency **MKD** (Macedonian denar). Languages: Macedonian (Cyrillic) + Albanian.

## Next action

Use the Central Registry's **paid distribution** for bulk + financial data and the
**free public search** for per-company identity/status. There is **no open bulk
register and no open financials**. Treat founders/managers as personal data and
redact. (Re-probe the CRM from an unblocked network — the hosts were firewalled
from this environment.)
