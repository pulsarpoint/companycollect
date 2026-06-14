# Patiesie labuma guvēji — Beneficial owners Field Catalog

## Source Summary

- Country: Latvia
- Source type: beneficial_ownership_register
- Organization: Latvijas Republikas Uzņēmumu reģistrs (UR)
- URL: https://data.gov.lv/dati/lv/dataset/patiesie-labuma-guveji (download: beneficial_owners.csv)
- License: CC0-1.0 (public domain)
- Access: public (free)
- Freshness: regular
- Record shape: one row per beneficial owner, keyed by entity regcode
- Primary keys: `regcode`, `beneficial_owner`
- Join keys: `regcode`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| regcode | legal entity regcode | Company id | string | identifier | (not parsed) | join key |
| beneficial_owner | name | BO name | string | ownership | (not parsed) | **PII** |
| birth_date / nationality / residence | … | BO attributes | object | person | (not parsed) | **PII** |
| nature_of_control | control | Nature of control | string | ownership | (not parsed) | direct/indirect |

## Interpretation Notes

- **Open beneficial ownership — unusual.** Latvia publishes the beneficial-owner register as **open bulk CSV**
  under **CC0**, where many EU states restricted public access after the 2022 CJEU ruling. Reachable via CKAN;
  not parsed field-by-field here, so field names/confidence are documented and no real values are copied.
- **GDPR.** Beneficial owners (name, birth date, nationality, residence) are **personal data** — apply a lawful
  basis + retention; no direct-marketing reuse. CC0 governs IP, not data protection. Join on **regcode**.
