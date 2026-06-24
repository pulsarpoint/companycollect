# ARBK — Kosovo Business Registration Agency Field Catalog

## Source Summary

- Country: Kosovo
- Source type: official_registry
- Organization: ARBK / Ministry of Industry, Entrepreneurship and Trade (MINT)
- URL: https://arbk.rks-gov.net/ (API base `/api/api/`)
- License: not stated (verification use)
- Access: **GATED** — browser only; `Services/*` return HTTP 401 (bearer) and the
  search requires a Cloudflare Turnstile CAPTCHA token
- Freshness: live register
- Record shape: JSON business detail (`Services/TeDhenatBiznesit`) behind auth
- Primary keys: NumriUnikIdentifikues (NUI)
- Join keys: NumriUnikIdentifikues, NumriFiskal

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| NumriUnikIdentifikues | Numri Unik Identifikues (NUI) | Unique id (= fiscal no), 9-digit | string | identifier |  | primary id/join key |
| NumriBiznesit | Numri i Biznesit (NRB) | Business reg number | string | identifier |  |  |
| NumriFiskal | Numri Fiskal | Fiscal/tax number | string | identifier |  | = NUI |
| NumriTVSH | Numri i TVSH-së | VAT number | string | identifier |  | separate |
| Emri | Emri i Biznesit | Business name | string | legal_name |  |  |
| StatusiBiznesit | Statusi i Biznesit | Status | string | status | Aktiv/Pasiv/Shuar |  |
| DataRegjistrimit | Data e Regjistrimit | Registration date | date | date |  | dd.MM.yyyy |
| LlojiBiznesit | Lloji i Biznesit | Legal form | string | legal_form | Sh.P.K. | B.I./O.P./Sh.A. |
| Komuna | Komuna | Municipality | string | geography |  |  |
| Adresa | Adresa | Address | string | address |  |  |
| AktivitetiKryesor | Aktiviteti Kryesor | Primary activity | string | activity |  | NACE-aligned |
| Aktivitetet | Aktivitetet | Other activities | array | activity |  |  |
| Kapitali | Kapitali | Registered capital | decimal | financial |  | EUR; only open financial field |
| Pronaret | Pronari/Pronarët | Owners (+ %) | array | ownership |  | PERSONAL DATA — redact |
| PronariHuaj | Pronari Huaj (%) | Foreign ownership % | decimal | ownership |  |  |
| NumriPunetoreve | Numri i Punëtorëve | Employees | integer | employment |  |  |

## Interpretation Notes

- **Access is gated** (verified): every `/api/api/Services/*` endpoint returns
  **HTTP 401** without the SPA's bearer token, and the search
  (`Services/KerkoBiznesin`) requires a **Cloudflare Turnstile** CAPTCHA token. The
  export endpoint (`Services/EksportoBizneset`) is also gated. **No controls were
  bypassed** and **no live values were captured** — all fields here are documented
  from the SPA's own JavaScript/i18n field model, so example values are empty.
- **Identifiers**: the **NUI** (Numri Unik Identifikues) is the primary id and for
  businesses equals the **Numri Fiskal** (9-digit). **NRB** (Numri i Biznesit) is
  the registration number. The **Numri i TVSH** (VAT) is separate.
- **Financials**: the only openly-modelled financial datapoint is registered
  **Kapitali** (EUR). No revenue/balance-sheet data is published anywhere.
- **Personal data**: owners (**Pronarët**) are personal data when natural persons
  (Kosovo Law No. 06/L-082) — redact.
- **Tri-lingual**: Albanian / Serbian / English. Currency **EUR**.
