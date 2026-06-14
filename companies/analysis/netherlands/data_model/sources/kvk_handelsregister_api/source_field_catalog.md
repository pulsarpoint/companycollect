# KvK Handelsregister API (Zoeken / Basisprofiel / Vestigingsprofiel / Naamgeving) Field Catalog

> **Paid / subscription.** The route to identified data (names, addresses, officers, KvK-nummer). Fields
> described from public documentation; no records/values copied. No `sample_record.json`.

## Source Summary

- Country: Netherlands
- Source type: official_registry
- Organization: Kamer van Koophandel (KvK)
- URL: https://developers.kvk.nl/ (api.kvk.nl)
- License: commercial / paid (KvK API subscription)
- Access: paid
- Freshness: real-time
- Record shape: JSON per query (identified)
- Primary keys: `kvkNummer`
- Join keys: `kvkNummer`, `rsin`, `vestigingsnummer`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| kvkNummer | kvkNummer | KvK number (8-digit) | string | identifier | (paid) | company id / join key |
| rsin | rsin | RSIN (9-digit) | string | identifier | (paid) | VAT base |
| vestigingsnummer | vestigingsnummer | Establishment (12-digit) | string | identifier | (paid) | per-establishment |
| naam / handelsnamen | naam | Name + trade names | string | legal_name | (paid) | stripped from open data |
| adressen | adressen | Addresses | array | address | (paid) | full address |
| sbiActiviteiten | sbiActiviteiten | SBI activities | array | activity | (paid) | with descriptions |
| functionarissen | functionarissen | Officers | array | person | (paid) | **PII (GDPR)** |

## Interpretation Notes

- **The identified-data route.** The KvK Handelsregister API has several products: **Zoeken** (search by name /
  KvK number), **Basisprofiel** (full company profile incl. **KvK-nummer**, **RSIN**, names, addresses,
  **officers**), **Vestigingsprofiel** (establishments / vestigingen), and **Naamgeving** (names/trade names).
  It is **paid (subscription + API key)**; a free test environment exists. This is the way to obtain the
  **identity** (name, address, officers) that the open data strips, and to map a **KvK number → company**.
- **Join keys.** KvK-nummer (8), RSIN (9, = VAT base), vestigingsnummer (12). **GDPR** applies to officers.
