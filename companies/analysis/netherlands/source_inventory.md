# Netherlands — Source Inventory

| Source | Type | Access | Format | License | Status |
|---|---|---|---|---|---|
| **KvK Open Data — basis bedrijfsgegevens** | Official registry | Free | **CSV** / JSON API | CC-BY 4.0 | **recommended** (anonymised stats) |
| **KvK Open Data — jaarrekeningen** | Official financials | Free | **XML (XBRL)** / JSON API | CC-BY 4.0 | **recommended** (**structured financials**, anonymised) |
| **KvK Handelsregister API** (Zoeken/Basisprofiel/…) | Official registry | Paid (subscription) | JSON | Commercial | blocked by payment (identified data) |
| UBO-register | BO register | Restricted (AML) | HTML/JSON | Restricted | blocked by authentication |
| Belastingdienst / VIES (NL VAT) | Official tax | Free | SOAP | Validation | useful secondary |
| data.overheid.nl | Open data portal | Free | CSV/JSON | Per dataset (CC-BY 4.0) | useful secondary (catalog) |
| Commercial aggregators (Company.info, Graydon, Kyckr) | Commercial API | Paid | JSON/PDF | Commercial | useful secondary (identified bulk + financials) |

## Access points

- Open data: https://www.kvk.nl/producten-bestellen/kvk-handelsregister-open-data-set/
  - Basis bulk: `https://www.kvk.nl/download/kvk-open-dataset-basis-bedrijfsgegevens.zip`
  - Jaarrekeningen bulk: `https://www.kvk.nl/download/kvk-open-data-set-jaarrekeningen{0..5}.zip`
  - HVDS API: `https://opendata.kvk.nl/api/v1/hvds/{basisbedrijfsgegevens|jaarrekeningen}/kvknummer/{kvknummer}` (free API key)
- Paid API: https://developers.kvk.nl/ (Zoeken/Basisprofiel/Vestigingsprofiel/Naamgeving)
- UBO: https://www.kvk.nl/ubo/ ; VIES: https://ec.europa.eu/taxation_customs/vies/
- Catalog: https://data.overheid.nl/ ; Commercial: https://www.company.info/

## Key facts

- **Join key**: **KvK-nummer** (8 digits) — required by the HVDS + paid APIs, but **NOT in the open bulk** (anonymised). RSIN (9 digits) + vestigingsnummer (12 digits). **VAT** = NL + 9 digits (RSIN) + B + 2.
- **Open (CC-BY 4.0) but anonymised in bulk**: basis bedrijfsgegevens (1,891,639 records) + jaarrekeningen (structured XBRL-derived balance sheets). Statistics/financial benchmarks.
- **Identified data**: free **HVDS API** by KvK number (basic + financials, with key); **paid KvK API** for names/addresses/officers; commercial providers for identified bulk.
- **Financials**: jaarrekeningen = structured balance-sheet figures (assets, equity, liabilities, provisions, share capital) + year; EUR. Income statement limited (micro/small abridged).
- **UBO** restricted (AML-obliged). **Verified live**: downloaded both bulks (real counts); HVDS API rate-limited without key.

See `source_inventory.json` for the machine-readable version.
