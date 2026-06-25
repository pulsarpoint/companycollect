# Source inventory — Kenya

| Source | Type | Org | Access | Formats | Financials | Status |
|---|---|---|---|---|---|---|
| NSE listed (`nse.co.ke/listed-companies`) | Listed directory + financials | NSE | **Open** | html, json, pdf | yes (listed) | **recommended** |
| BRS via eCitizen (`brs.ecitizen.go.ke`) | Official registry + documents | BRS | eCitizen login, paid | html, pdf | yes (annual returns, paid) | blocked_by_payment |
| Kenya Open Data (`opendata.go.ke`) | Open-data portal | ICT Authority | Public; no company dataset | — | no | unavailable |

## Identifiers

- **Company registration number** — BRS (old `C.NNNNN` / `CPR/2015/NNNNNN`; new
  `PVT-XXXXXXX`).
- **BN number** — Business Name (sole proprietors / partnerships).
- **KRA PIN** — Kenya Revenue Authority tax id (e.g. `P051234567X`).
- **VAT** — registered under the KRA PIN (no separate VAT number).

## Key facts

- **BRS** is the official register but delivered via **eCitizen** — search + documents
  (**CR12**, status reports, annual returns) are **login-gated and paid**; no open
  bulk/API.
- **NSE** is the one **open** source — listed companies + financials (verified live:
  Absa Bank Kenya, Stanbic Holdings, Sasini…). Listed only (~60).
- **opendata.go.ke** has no accessible company dataset.
- Currency **KES**; English. CR12 directors/shareholders are personal data (Data
  Protection Act 2019) → redact.
