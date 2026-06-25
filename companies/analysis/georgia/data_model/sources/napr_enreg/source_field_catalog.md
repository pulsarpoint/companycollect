# NAPR e-registry (enreg) Field Catalog

## Source Summary

- Country: Georgia
- Source type: official_registry
- Organization: National Agency of Public Registry (NAPR), Ministry of Justice of Georgia
- URL: https://enreg.reestri.gov.ge/main.php
- License: restricted
- Access: **public search, CAPTCHA-gated** (free extracts only after solving the CAPTCHA)
- Freshness: live
- Record shape: per-company extract (planning-only)
- Primary keys: identification_code
- Join keys: identification_code, legal_name

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| identification_code | საიდენტიფიკაციო კოდი | 9-digit ID code | string | identifier |  | reg. number + tax id |
| legal_name | დასახელება | Company name | string | legal_name |  | Georgian |
| legal_form | სამართლებრივი ფორმა | Legal form | string | legal_form |  | შპს/სს/ი.მ |
| status | სტატუსი | Status | string | status |  | active/liquidated |
| registration_date | რეგისტრაციის თარიღი | Registration date | date | date |  | Gregorian |
| registered_address | მისამართი | Legal address | string | address |  | |
| director | დირექტორი | Director | string | person |  | **PERSONAL DATA — redact** |
| partners | პარტნიორები | Partners/shareholders | array | ownership |  | **PERSONAL DATA — redact** |

## Interpretation Notes

- **NAPR e-registry** (`enreg.reestri.gov.ge/main.php`) is the **authoritative** Georgian
  company registry. The public search form carries a **`captcha_validator_field`** (and a
  login: `auth_username`/`auth_password`), so the free company search and **extract
  (amonaweri)** are **CAPTCHA-gated**. `api.napr.gov.ge` returns **"Access Denied"**. No
  open bulk or free API. All fields here are **planning-only** from public knowledge — **no
  values captured** (the CAPTCHA was not bypassed).
- **Identifier**: the **9-digit identification code (საიდენტიფიკაციო კოდი)** is the
  registration number **and** the tax id (shared with the Revenue Service) — the universal
  Georgian company key.
- **Language**: Georgian (Mkhedruli script); some English. **Personal data**: director and
  partners (natural persons) are protected under Georgia's Law on Personal Data Protection —
  redact.
- No `sample_record.json`: restricted/CAPTCHA-gated source, nothing captured.
