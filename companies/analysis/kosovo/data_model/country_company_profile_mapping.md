# Kosovo — combined profile mapping

## Join keys & precedence

- **Primary join key: NUI** (Numri Unik Identifikues = Numri Fiskal, 9-digit) —
  the company id and tax id. ATK's `FiscalNo` equals the NUI, joining the tax view
  to ARBK identity.
- **Precedence**: **ARBK** is authoritative for identity, status, activity,
  address, capital, ownership. **ATK VatRegist** is a tax-side cross-check
  (status, VAT type, tax centre).
- **Both sources are GATED** (ARBK: bearer 401 + Turnstile CAPTCHA; ATK: CAPTCHA).
  No open bulk/API; no per-company values extracted.

| Profile path | Source | Source path | Join key | Precedence | Notes |
|---|---|---|---|---|---|
| registration.nui | arbk_business_register | NumriUnikIdentifikues | NUI | authoritative | = fiscal number |
| registration.business_number_nrb | arbk_business_register | NumriBiznesit | NUI | authoritative | NRB |
| tax_identifiers.fiscal_number | arbk_business_register | NumriFiskal | NUI | authoritative | = NUI |
| tax_identifiers.tax_id | atk_vatregist | tpResult.FiscalNo | NUI | cross-check | = NUI |
| tax_identifiers.vat_id | arbk_business_register | NumriTVSH | NUI | authoritative | separate; ATK VatNo confirms |
| legal_identity.business_name | arbk_business_register | Emri | NUI | authoritative | ATK TpName cross-check |
| legal_identity.legal_form | arbk_business_register | LlojiBiznesit | NUI | authoritative | B.I./Sh.P.K./Sh.A. |
| status.status_text | arbk_business_register | StatusiBiznesit | NUI | authoritative | Aktiv/Pasiv/Shuar |
| status.registration_date | arbk_business_register | DataRegjistrimit | NUI | authoritative |  |
| activity.primary_activity | arbk_business_register | AktivitetiKryesor | NUI | authoritative |  |
| registered_location.* | arbk_business_register | Adresa / Komuna | NUI | authoritative | ATK Address/City cross-check |
| capital.registered_capital | arbk_business_register | Kapitali | NUI | authoritative | EUR; only open financial field |
| owners[] | arbk_business_register | Pronaret | NUI | authoritative | REDACT natural persons |
| employment.employees | arbk_business_register | NumriPunetoreve | NUI | authoritative |  |
| tax_status.* | atk_vatregist | tpResult.TpStatus/VatTypeAl/TaxCentreName | NUI (FiscalNo) | cross-check | CAPTCHA-gated |

## Freshness

- ARBK / ATK: **live** (but gated).

## Missing-data notes

- **No open bulk / API** — both sources are CAPTCHA/bearer gated → `blocked_authentication`.
- **No financial statements** — only ARBK registered capital (EUR).
- **No working national open-data portal**; ATK Open Data is aggregate only.
- **No per-company values extracted** (controls not bypassed).
- **Owners** redacted as personal data.
