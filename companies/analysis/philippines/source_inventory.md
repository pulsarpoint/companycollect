# Source inventory — Philippines

| Source | Type | Org | Access | Formats | Financials | Status |
|---|---|---|---|---|---|---|
| SEC Express (`secexpress.ph`) | Official registry docs | SEC | Paid per document | pdf | yes (AFS, paid) | blocked_by_payment |
| PSE EDGE (`edge.pse.com.ph`) | Listed directory + disclosures | PSE | **Open** | html, pdf | yes (listed) | **recommended** |
| DTI BNRS (`bnrs.dti.gov.ph`) | Sole-prop business names | DTI | Free search | html | no | useful_secondary_source |
| data.gov.ph | Open-data portal | DICT | JS SPA (no dataset) | — | no | unavailable |

## Identifiers

- **SEC Registration Number** — corporate registration id (CSNNNNNNNN / older ANNNNNNNN).
- **TIN** — BIR tax id (9-digit + branch code).
- **DTI BN number** — sole-proprietor business name registration.
- **VAT** — uses the TIN (no separate VAT number).

## Key facts

- **SEC** is the official corporate register, but company documents (**GIS** with
  officers/stockholders/capital, **AFS** financials, Articles) are **paid** via SEC
  Express; eFAST/eSPARC are login portals; sec.gov.ph is WAF-blocked. No open bulk
  register.
- **PSE EDGE** is the one **open** source — listed companies + financial reports
  (verified: PLDT/TEL). Listed only (~280).
- **data.gov.ph** has no accessible company dataset (JS SPA); **DTI BNRS** is
  sole-props only.
- Currency **PHP**; English. GIS officers/stockholders are personal data (Data
  Privacy Act 2012) → redact.
