# State Register of Legal Entities (e-register.am) Field Catalog

## Source Summary

- Country: Armenia
- Source type: official_registry
- Organization: State Register of Legal Entities, Ministry of Justice of the Republic of Armenia
- URL: https://www.e-register.am/
- License: restricted
- Access: **public search, Radware Bot Manager-protected** (no open bulk/API)
- Freshness: live
- Record shape: per-company search (planning-only)
- Primary keys: state_registration_number
- Join keys: state_registration_number, tin_hvhh, legal_name

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| state_registration_number | State Registration Number | Registry id | string | identifier |  | registry key |
| tin_hvhh | ՀՎՀՀ (TIN) | 8-digit taxpayer id | string | identifier |  | **join key to SRC** |
| legal_name | Անվանում | Entity name | string | legal_name |  | Armenian |
| legal_form | Կազմակերպական-իրավական ձև | Legal form | string | legal_form |  | ՍՊԸ/ԲԲԸ/ՓԲԸ |
| status | Կարգավիճակ | Status | string | status |  | active/liquidated |
| registration_date | Գրանցման ամսաթիվ | Registration date | date | date |  | Gregorian |
| registered_address | Հասցե | Legal address | string | address |  | |
| director_and_founders | Տնօրեն / Հիմնադիրներ | Director / founders | array | ownership |  | **PERSONAL DATA — redact** |

## Interpretation Notes

- The **State Register of Legal Entities** (`e-register.am` / `e-register.moj.am`, Ministry of
  Justice) is the **authoritative** Armenian company register with a free public search. From
  this environment it is protected by **Radware Bot Manager**: `e-register.am/en` and
  `/companies` redirect to `validate.perfdrive.com`. **No open bulk or free API.** All fields
  here are **planning-only** from public knowledge — **no values captured** (bot manager not
  bypassed).
- **Identifiers**: the **state registration number** is the registry key; the **TIN (ՀՎՀՀ /
  HVHH, 8-digit)** is shared with the SRC and is the practical universal join key.
- **Language**: Armenian (Mkhedruli/Armenian script); some English. **Personal data**:
  director and founders (natural persons) are protected under Armenia's Law on the Protection
  of Personal Data — redact.
- No `sample_record.json`: restricted/bot-protected source, nothing captured.
