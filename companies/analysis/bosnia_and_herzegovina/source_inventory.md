# Source inventory — Bosnia and Herzegovina

| Source | Type | Org | Access | Formats | Financials | Status |
|---|---|---|---|---|---|---|
| RS Business Register (`bizreg.esrpska.com`) | Official registry (RS) | APIF / RS courts | Public per-company JSON search | json, html, pdf | no (identity only) | recommended |
| FBiH & Brčko register (`bizreg.pravosudje.ba`) | Official registry (FBiH/Brčko) | VSTV/HJPC courts | Public per-company APEX search | html | no | useful_secondary_source |
| APIF — RFI + bonitet | Financial statements (RS) | APIF | Public, paid per company | pdf, html | yes (bilans, BAM) | blocked_by_payment |
| FIA | Financial statements (FBiH) | FIA | Public, paid per company | pdf, html | yes (bilans, BAM) | blocked_by_payment |
| UINO | Tax/VAT registry | UINO (state) | Public per-company lookup | html | no | useful_secondary_source |

## Identifiers

- **JIB** — Jedinstveni identifikacioni broj, **13-digit** = company id = tax id
  (RS legal entities start `44…`). The country-wide join key.
- **MBS** — Matični broj subjekta (court registration / registarski uložak).
- **MB** — Matični broj (7-digit statistical number).
- **PDV broj** — **12-digit VAT number**, separate, assigned by **UINO** for
  VAT-registered entities. BiH has a single state-level VAT (PDV).

## Key facts

- No single national register; data is split by entity (RS vs FBiH vs Brčko).
- RS register exposes structured JSON per company (best open access).
- No open bulk register and no working national open-data portal (`data.gov.ba`
  did not resolve).
- Financial statements are filed (APIF RFI / FIA) but accessed per company for a fee.
- Founders/owners (Osnivači) and representatives are personal data when
  individuals — redact in committed samples.
- Currency: **BAM (Konvertibilna marka, KM)**.
