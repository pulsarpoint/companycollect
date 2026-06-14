# Poland — Source Inventory

| Source | Type | Access | Format | License | Status |
|---|---|---|---|---|---|
| **KRS API** | Official registry API | Free, no auth | JSON | Open | **recommended** ✅ verified |
| **RDF — Financial statements** | Official financials | Free per-company | **XML/XBRL**/PDF | Open | **recommended** (**financials**) |
| **Biała lista VAT** | Official tax API | Free, no auth | JSON | Open | **recommended** ✅ verified (NIP↔REGON↔KRS bridge) |
| **CEIDG** | Sole-proprietor registry | Free token | JSON | Open | **recommended** (sole traders) |
| REGON / GUS BIR1 | Statistical register | Free key | XML/JSON | Open | useful secondary (all entities) |
| CRBR (beneficial ownership) | BO register | Free, no auth | JSON/PDF | Open | useful secondary (**open ownership**) |
| dane.gov.pl | Open data portal | Free | various | Per dataset | useful secondary (discovery) |
| Commercial aggregators (Rejestr.io, MGBI) | Commercial API | Paid/freemium | JSON | Commercial | useful secondary (resell open data) |

## Access points

- **KRS API**: `https://api-krs.ms.gov.pl/api/krs/OdpisAktualny/{krs}?rejestr={P|S}&format=json` (also OdpisPelny)
- **RDF financials**: https://ekrs.ms.gov.pl/rdf/pd/search_df (free per-company XML+PDF); PRS-eKRS mass API (registration)
- **VAT white list**: `https://wl-api.mf.gov.pl/api/search/nip/{nip}?date=YYYY-MM-DD` + daily flat file
- **CEIDG**: `https://dane.biznes.gov.pl/api/ceidg/v3` (free token)
- **REGON BIR1**: `https://wyszukiwarkaregon.stat.gov.pl/wsBIR/` (free key)
- **CRBR**: https://crbr.podatki.gov.pl/ (lookup by NIP)
- National catalog: https://dane.gov.pl/

## Verified live (2026-06-14)

- KRS API — HTTP 200, 59 KB JSON (PKO BP KRS 0000026438) → `raw/api/krs_odpisaktualny_0000026438.json`
- VAT white list — HTTP 200 (NIP 5250007738) → `raw/api/whitelist_nip_5250007738.json` (bridges NIP/REGON/KRS + bank accounts)

## Key facts

- **Open registry** (KRS API) + **open structured financials** (RDF XML) + **open VAT bridge** (white list)
  + **open beneficial ownership** (CRBR) — Poland is among the most open jurisdictions.
- KRS = companies; CEIDG = sole traders; REGON = all entities — combine for full coverage.
- Identifiers: **KRS** (10-digit), **NIP** (tax/VAT), **REGON** (9 or 14-digit) — white list bridges them.

See `source_inventory.json` for the machine-readable version.
