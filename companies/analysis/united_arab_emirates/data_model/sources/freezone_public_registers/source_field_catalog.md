# Free-zone public registers — DIFC & ADGM Field Catalog

## Source Summary

- Country: United Arab Emirates
- Source type: official_registry
- Organization: DIFC Registrar of Companies / ADGM Registration Authority
- URL: https://www.adgm.com/public-registers
- License: public register (WAF-gated)
- Access: **public via browser; WAF/rate-limited** for automation
- Freshness: live
- Record shape: per-entity free-zone register record (WAF-gated)
- Primary keys: registration_number
- Join keys: registration_number, entity_name

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| entity_name | Entity Name | Registered entity name | string | legal_name |  | |
| registration_number | Registration Number | DIFC/ADGM number | string | identifier |  | primary key (per free zone) |
| legal_form | Legal Form / Type | Entity type | string | legal_form | SPV, Foundation | |
| status | Status | Status | string | status |  | Active/Dissolved/Struck off |
| registered_address | Registered Address | Registered address | string | address |  | within the free zone |
| incorporation_date | Incorporation Date | Incorporation date | date | date |  | |
| free_zone | Free Zone | Which free zone | string | metadata | DIFC, ADGM | routes to registrar |

## Interpretation Notes

- **DIFC** (Dubai International Financial Centre) and **ADGM** (Abu Dhabi Global
  Market) are **common-law financial free zones** with their own **registrars and
  public registers** of entities (companies, branches, SPVs, funds, partnerships,
  foundations). These are the most genuinely **public** UAE registers.
- **Access (verified):** the **ADGM** public-registers page loads, but the search app
  (`registration.adgm.com`) returned **HTTP 403 (WAF)**; the **DIFC** public register
  returned **HTTP 429** (rate-limited/WAF); no downloadable register file was found. So
  they are **public via the browser** but **WAF/rate-limited** for automation. **Not
  bypassed** — field model from public knowledge, no live values.
- **Identifiers**: the **registration number** is per-free-zone; the **free_zone**
  field routes to the specific registrar. These join to the **NER** unified number by
  name. Other free zones (DMCC, JAFZA, etc.) have their own registers (mostly login/
  per-zone).
- DIFC/ADGM have their **own data-protection laws** (DIFC DP Law 2020; ADGM DP
  Regulations 2021) — redact personal data. Currency **USD** (DIFC/ADGM) or AED.
- Implementation is **blocked on authentication/WAF**; planning-only.
