# Malta — Common Field Mapping Suggestions

> **Suggestion layer only.** This does NOT constrain the country-specific Malta profile
> (`country_company_profile.schema.json`). It is a hint for a future cross-country mapper.

| Common field | Malta source | Malta path | Notes |
|---|---|---|---|
| company_id | mbr_register | registration_number | prefix = entity class (C = companies) |
| registration_number | mbr_register | registration_number | same |
| tax_id | not_available_in_open_sources | — | income-tax TIN separate; not in free register |
| vat_id | vies_vat | MT + 8 digits | separate; not in register; validate via VIES |
| legal_name | mbr_register | name | |
| status | mbr_register | status | Active/Struck off/Liquidated |
| legal_form | mbr_register | company_type | Ltd/plc/partnership |
| incorporation_date | mbr_register | registration_date | |
| dissolution_date | not_available_in_open_sources | — | derive from status (Struck off/Dissolved) |
| registered_address | mbr_register | registered_address | parse locality |
| activity_code | not_available_in_open_sources | — | no public NACE in free register |
| financials | mbr_annual_accounts (paid PDF) / mbr_api (paid) / commercial_aggregators (paid) | annual accounts / API / vendor | paid; EUR; IFRS/GAPSME |
| officers | mbr_register (paid) / mbr_api / commercial_aggregators | directors + secretary | paid; PII |
| owners | mbr_register shareholders (paid) / rbe_register (restricted) | shareholders / beneficial owners | registered shareholders paid; UBO restricted |
| source_provenance | (all) | source_provenance[] | per-section provenance |

## Cross-Country Notes

- Malta is a **partial-open / automation-blocked** country: the MBR gives **free** identity + status, but
  officers, shareholders and financials are **paid** (documents or the paid API), the registry portals are
  **WAF-blocked** for automation, and there is **no open bulk**. A cross-country pipeline needs manual lookups,
  the **paid MBR API**, or a **commercial provider** (Kyckr, Creditinfo).
- `financials` maps to the paid MBR annual accounts (PDF, OCR/parse), the paid MBR API, or a vendor — not a
  structured open feed.
- `owners` should carry **registered shareholders** (in the register, paid — a Malta distinctive) **and**
  **beneficial owners** (restricted UBO) as distinct sub-concepts.
- `tax_id`, `activity_code`, and `dissolution_date` (derive from status) are `not_available_in_open_sources`;
  `vat_id` via VIES (no open crosswalk).
