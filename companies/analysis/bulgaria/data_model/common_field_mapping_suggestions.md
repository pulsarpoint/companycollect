# Bulgaria — Common Field Mapping Suggestions

> **Suggestion only.** Proposes how Bulgaria's country-specific profile *could* map onto a future
> cross-country company schema. It does **not** constrain `country_company_profile.schema.json`. The
> country-specific model is authoritative.

| Common field | Bulgaria source path | Notes |
|---|---|---|
| company_id | registration.eik | Single 9-digit (13 for branches) key; = VAT root. |
| registration_number | registration.eik | Same number. |
| tax_id | registration.eik | No separate tax id; EIK is the fiscal id. |
| vat_id | registration.vat_id ("BG" + eik) | Derived. |
| legal_name | legal_identity.name | Cyrillic (+ Latin transliteration). |
| status | status.derived | вписано/заличено/ликвидация/несъстоятелност. |
| legal_form | legal_identity.legal_form | ЕООД/ООД/АД/ЕТ. |
| incorporation_date | (commercial_register дата на вписване) | open-ish. |
| dissolution_date | (заличаване) | open-ish. |
| registered_address | registered_location.* | open-ish (register). |
| activity_code | not_available_in_open_sources | предмет на дейност is free text; no coded КИД/NACE — derive. |
| financials | financial_statements[] | Public PDFs (parse/OCR) or a paid provider; no XBRL. |
| officers | officers[] | управители/съвет — OPEN in the register (PII). |
| owners | owners[] (share owners) + beneficial_owners[] (restricted) | Share owners OPEN; beneficial ownership restricted. |
| source_provenance | source_provenance[] | per-source + access flag. |

## Cross-country notes for a future mapper

- **Bulgaria is a partial-open case** — between the fully-open group (BE/PL/NO/FR) and the paid-register
  group (DE/AT/IT). Registry data is open-ish (free search + **CC-BY publications**; full bulk needs an
  agreement), and uniquely it carries **officers AND share owners openly**. But **financials are public
  PDFs** (parse/OCR), not structured open.
- **Single clean key** (EIK = VAT root) — no fuzzy matching; **no separate tax id**.
- **Activity code** is `not_available_in_open_sources` (free-text объект; derive).
- **Financials**: a cross-country mapper must treat Bulgaria's `financials` as **document-based** (parse) or
  paid-structured; revenue null for micro/small (size category). Currency BGN → EUR (2026).
- **Cyrillic**: keep a Latin transliteration for cross-system matching.
- **Beneficial ownership** restricted; share owners open.
