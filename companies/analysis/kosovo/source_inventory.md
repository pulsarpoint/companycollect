# Source inventory — Kosovo

| Source | Type | Org | Access | Formats | Company-level | Status |
|---|---|---|---|---|---|---|
| ARBK Business Register (`arbk.rks-gov.net`) | Official registry | ARBK / MINT | Browser only; 401 bearer + Turnstile CAPTCHA | json, html | yes | blocked_by_authentication |
| ATK VatRegist (`apps.atk-ks.org`) | Tax/VAT verification | ATK | Per-company; CAPTCHA-gated | json, html | yes | blocked_by_authentication |
| ATK Open Data (`atk-ks.org/open-data`) | Statistics | ATK | Public XLSX | xlsx | no (aggregate) | useful_secondary_source |
| Kosovo open-data portal | Open-data portal | RKS e-gov | Did not resolve | — | — | unavailable |

## Identifiers

- **NUI — Numri Unik Identifikues** — unique id; for businesses = **Numri Fiskal**
  (fiscal/tax number), 9-digit. Primary id.
- **Numri i Biznesit / NRB** — business registration number.
- **Numri i TVSH-së** — VAT number, separate (only if VAT-registered).

## Key facts

- ARBK is the official register but **CAPTCHA + bearer gated**, no open bulk/API,
  export endpoint gated → not programmatically accessible without bypassing
  controls (not done).
- ATK VatRegist per-company lookup is **CAPTCHA-gated**; its output fields confirm
  the identifier model.
- ATK Open Data XLSX are **aggregate** (sector/municipality/year), not company-level.
- **No open financials**; ARBK exposes registered capital + ownership % only.
- No working national open-data portal.
- Owners (Pronarët) are personal data → redact.
- Currency: **EUR** (Kosovo uses the euro). Tri-lingual data (sq/sr/en).
