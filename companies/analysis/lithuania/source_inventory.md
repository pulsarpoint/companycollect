# Lithuania — Source Inventory

| Source | Org | Type | Access | Formats | License | Status |
|---|---|---|---|---|---|---|
| RC JAR — Register of Legal Entities (JuridinisAsmuo) | Registrų centras via data.gov.lt | official registry | public, no key | JSON, CSV | CC-BY 4.0 (confirm per dataset) | **recommended** |
| RC JAR — Balance sheets (BalansoAtaskaita) | Registrų centras via data.gov.lt | financial statements | public, no key | JSON, CSV | CC-BY 4.0 | **recommended** |
| RC JAR — Profit & loss (PelnoAtaskaita) | Registrų centras via data.gov.lt | financial statements | public, no key | JSON, CSV | CC-BY 4.0 | **recommended** |
| RC JAR — supplementary models | Registrų centras via data.gov.lt | official registry | public, no key | JSON, CSV | CC-BY 4.0 | useful_secondary_source |

## Roles

- **rc_jar_legal_entities** — authoritative open **identity** keyed on the 9-digit
  company code (name, legal form, status, reg/dereg dates); addresses in `Buveine`;
  `Forma`/`Statusas` code lists (LT+EN). Verified live.
- **rc_jar_balance_sheets** / **rc_jar_income_statements** — open **financial
  statements** as granular line items (EUR), linked to the company; verified live.
- **rc_jar_supplementary** — capital (`ja_kapitalas`), management bodies
  (`valdymo_organai`, personal data), documents, late/non-filers, NGOs, charity
  recipients. All keyless.

## Join keys

**Company code (`ja_kodas`, 9-digit)** is the human join key; the Spinta **`_id`
UUID** is the API's internal join (references point to it). VAT (PVM kodas) is
separate (VIES). Company code also serves as the legal-entity taxpayer code.
