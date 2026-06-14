# Hungary — Common Field Mapping Suggestions

> **Suggestion layer only.** This does NOT constrain the country-specific Hungary profile
> (`country_company_profile.schema.json`). It is a hint for a future cross-country mapper.

| Common field | Hungary source | Hungary path | Notes |
|---|---|---|---|
| company_id | ecegjegyzek | cegjegyzekszam | register id (NN-NN-NNNNNN) |
| registration_number | ecegjegyzek | cegjegyzekszam | same |
| tax_id | ecegjegyzek / nav_afaalany | adoszam | 11-digit; 8-digit base = stem |
| vat_id | vies_vat | HU + 8-digit base | validate via VIES; NAV for status |
| legal_name | ecegjegyzek | name | |
| status | ecegjegyzek | status | bejegyezve/törölve/felszámolás |
| legal_form | ecegjegyzek | legal_form | Kft/Zrt/Nyrt/Bt |
| incorporation_date | ecegjegyzek (full) | bejegyzés dátuma | basic free; date in full extract |
| dissolution_date | not_available_in_open_sources | — | derive from status / NAV cancellation |
| registered_address | ecegjegyzek | registered_seat | parse település/megye |
| activity_code | ecegjegyzek / ksh_register | TEÁOR | KSH canonical scheme |
| financials | ebeszamolo (reCAPTCHA-gated) / commercial_aggregators (paid) | key figures / vendor | free to view but gated; HUF/EUR |
| officers | ecegjegyzek (paid) / commercial_aggregators | képviselők | PAID; PII |
| owners | commercial_aggregators / ecegjegyzek (paid) | tulajdonosok | PAID; PII |
| source_provenance | (all) | source_provenance[] | per-section provenance |

## Cross-Country Notes

- Hungary is **partial-open / automation-blocked**: free basic identity + free-to-view financials, but **no open
  bulk**, **e-beszámoló automation is reCAPTCHA-gated**, and **officers/owners are paid**. A cross-country
  pipeline needs manual lookups, paid full-register access, or a **commercial provider** (OPTEN/Bisnode/Céginfo/
  companyapi.hu).
- `financials` maps to e-beszámoló (structured key figures, but gated) or a vendor — not an open structured feed.
- `owners` and detailed `officers` are **paid** (full cégjegyzék / vendor); beneficial ownership is
  `not_available_in_open_sources`.
- `dissolution_date` is `not_available_in_open_sources` as a field — derive from status / NAV tax-number
  cancellation.
- The **adószám 8-digit base** is the clean universal join stem (also inside EU VAT and the statistical code).
