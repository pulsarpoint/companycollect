# Common Field Mapping Suggestions — Saudi Arabia

This file is a **suggestion** for a future cross-country mapper. It does **not**
constrain the country-specific Saudi Arabia profile, which is the source of truth.

Both Saudi sources are gated (MoC CR = Nafath login-gated + firewalled hosts; Tadawul =
public-via-browser but WAF-gated for automation), so these mappings are **planning-only**
until an access path is established.

| Common field | Saudi mapping | Source | Notes |
|---|---|---|---|
| company_id | registration.cr_number | moc_commercial_register | 10-digit CR number; Unified Number `700…` as alt |
| registration_number | registration.cr_number | moc_commercial_register | CR number |
| tax_id | tax_identifiers.vat_number | moc_commercial_register | ZATCA VAT / Unified Number |
| vat_id | tax_identifiers.vat_number | moc_commercial_register | 15-digit |
| legal_name | legal_identity.legal_name | moc_commercial_register / tadawul_listed | CR preferred; Tadawul for listed |
| status | status.status_text | moc_commercial_register | Active/Expired/Cancelled/Suspended |
| legal_form | legal_identity.company_type | moc_commercial_register | JSC/LLC/SJSC/Sole Proprietorship/Branch |
| incorporation_date | status.issue_date | moc_commercial_register | Hijri→Gregorian |
| dissolution_date | status.expiry_date | moc_commercial_register | CR expiry (proxy) |
| registered_address | registered_location.head_office | moc_commercial_register | |
| activity_code | activity.activities | moc_commercial_register | ISIC; Tadawul sector for listed |
| financials | financial_statements | tadawul_listed | SAR; **listed only** |
| officers | officers | moc_commercial_register | **REDACT — PDPL personal data** |
| owners | not_available_in_open_sources | — | gated CR; personal data (PDPL) |
| source_provenance | source_provenance | both | per-section |

Concepts **not available from open sources** for Saudi Arabia:

- `owners` / beneficial ownership — gated CR; personal data under PDPL.
- Private-company `financials` — only Tadawul-listed financials are public.
- Any per-company value at all without clearing the Nafath login (CR) or the Tadawul WAF.
