# Registrų centras JAR — Register of Legal Entities (JuridinisAsmuo) Field Catalog

## Source Summary

- Country: Lithuania
- Source type: official_registry
- Organization: Registrų centras (Centre of Registers) via data.gov.lt
- URL: https://get.data.gov.lt/datasets/gov/rc/jar/iregistruoti/JuridinisAsmuo
- License: CC-BY 4.0 (open data; confirm per dataset)
- Access: public, **no API key**
- Freshness: regularly updated (live register feed)
- Record shape: Spinta JSON rows, one per legal entity
- Primary keys: `ja_kodas` (9-digit company code)
- Join keys: `ja_kodas` (business), `_id` (Spinta UUID)

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| ja_kodas | ja_kodas | Company code (9-digit) | integer | identifier | 110000291 | key; store as string |
| ja_pavadinimas | ja_pavadinimas | Legal name | string | legal_name | …"STT Inc." | form phrase embedded |
| pilnas_adresas / adresas | pilnas_adresas | Full / short address | string | address | (often null) | prefer Buveine |
| reg_data | reg_data | Registration date | date | date | 1991-03-11 | incorporation |
| isreg_data | isreg_data | Deregistration date | date | date | 2002-03-22 | null = active |
| forma._id | forma | Legal-form ref | string | legal_form | 5c444113-… | → Forma (168) |
| statusas._id | statusas | Status ref | string | status | 5bcfd61f-… | → Statusas (31) |
| stat_data | stat_data | Status date | date | date | 2002-03-22 | |

## Interpretation Notes

- **Verified from real data** via the keyless Spinta API: e.g. `ja_kodas`
  110000291, "Bendra Lietuvos – JAV įmonė … STT Inc.", `reg_data` 1991-03-11.
- **Company code (`ja_kodas`)** is the 9-digit company id, the legal-entity
  taxpayer code, and the **universal join key** across every JAR model. Returned as
  an integer — store as a string for safe joins.
- **References**: `forma` and `statusas` are `{"_id": "<uuid>"}` pointers. Resolve
  by joining `_id` against the code-list models:
  - **Forma** (168 rows): `kodas`, `pavadinimas` (LT), `pav_ilgas`, `name` (EN),
    `tipas`/`type`. E.g. 110 Valstybės įmonė / State Enterprise.
  - **Statusas** (31 rows): `kodas`, `pavadinimas` (LT), `name` (EN). E.g. 0 not
    registered, 5 going bankrupt, 6 bankrupt, 7 under liquidation, 10 removed, 11
    liquidated.
- **Status derivation**: use `statusas` (resolved); `isreg_data` present implies
  removed/deregistered.
- **Address** is usually null here — pull the **Buveine** model
  (`juridinis_asmuo`, `adresas`, `adresas_nuo`) for the current address.
- **VAT (PVM kodas)** is not in this model — separate registration via EU VIES.
- Pagination via `?limit(N)` + the `_page.next` cursor; pace requests (rapid bursts
  occasionally return transient errors).
