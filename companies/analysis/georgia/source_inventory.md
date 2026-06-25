# Georgia Source Inventory

| Source | Slug | Org | Access | Format | Status | Role |
|---|---|---|---|---|---|---|
| NAPR e-registry (enreg) | napr_enreg | NAPR (MoJ) | public search, CAPTCHA-gated | HTML/PDF | blocked_by_authentication | Authoritative registry (identification code, particulars) |
| SARAS Reporting Portal | reportal_saras | SARAS (MoF) | browser-public, token-gated | HTML/PDF | useful_secondary_source | Public financial statements + management reports |
| Georgian Stock Exchange | gse_listed | GSE | browser-public | HTML | useful_secondary_source | Listed securities (ISINs) |
| data.gov.ge | data_gov_ge | Data Exchange Agency | firewalled from environment | unknown | unavailable | National open-data portal (unreachable here) |

## Notes

- **No open bulk file or free API** was reachable. NAPR's `api.napr.gov.ge` returns
  "Access Denied"; `data.gov.ge` is firewalled / cert-broken from this environment.
- **NAPR e-registry** (`enreg.reestri.gov.ge`) is the authoritative registry but its public
  search is **CAPTCHA-gated** (free extracts only after solving the CAPTCHA).
- **reportal.ge (SARAS)** is the official **financial-statements** portal — browser-public
  search by identification code/name, but automation needs the anti-forgery token.
- **GSE** lists securities with Georgian **ISINs** (32 observed); listed only.
- **Identifier**: the **9-digit identification code** is both the registration number and
  the tax id (NAPR + Revenue Service). Listed: ISIN.
- Directors/partners (NAPR) are personal data — redact.
