# Source inventory — North Macedonia

| Source | Type | Org | Access | Formats | Financials | Status |
|---|---|---|---|---|---|---|
| CRM Trade Registry (`crm.com.mk`) | Official registry | Central Registry | Free basic search; paid bulk | html, paid distribution | no (identity) | blocked_by_payment |
| CRM Registry of Annual Accounts | Financial statements | Central Registry | Paid distribution | paid, pdf | yes (MKD) | blocked_by_payment |
| UJP (`ujp.gov.mk`) | Tax/VAT registry | Public Revenue Office | Per-company (502 now) | html | no | useful_secondary_source |
| data.gov.mk (CKAN) | Open-data portal | Gov of NM | Public (unreachable now) | csv/xlsx/json | no | unavailable |

## Identifiers

- **ЕМБС — Единствен матичен број на субјектот** — 7-digit entity registration
  number = company id.
- **ЕДБ — Единствен даночен број** — 13-digit tax number.
- **ДДВ број** — VAT registration (UJP).

## Key facts

- The **Central Registry (CRM)** is the official source for both identity and
  **annual financial statements**, but it **commercially distributes** the data —
  only a **free basic per-company search** is open; bulk + financials are **paid**.
- **No open bulk register; no open financials.**
- **Environment block**: `crm.com.mk` resolved via DNS (92.55.95.145) but TCP/HTTP
  timed out; `data.gov.mk` unreachable; `ujp.gov.mk` 502 — model documented from
  public docs, no live values captured.
- Currency **MKD**. Languages: Macedonian (Cyrillic) + Albanian. Founders/managers
  are personal data → redact.
