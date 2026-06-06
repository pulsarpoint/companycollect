# Serbia — Schema Notes

All APR open-data payloads share the same envelope:

```json
{
  "DatumPreseka": "2026-05-31",          // snapshot date (ISO YYYY-MM-DD)
  "Podaci": { "<maticni_broj>": { ... } } // map keyed by 8-digit registration number
}
```

- **Primary key:** `matični broj` (MB) — 8-digit company registration number, the
  map key. It is the join key across all three datasets.
- **Encoding:** UTF-8. `PoslovnoIme` is Latin; `NazivOpstine`, `NazivStatus`,
  `NazivPravneForme` are **Cyrillic**.
- **Dates:** ISO `YYYY-MM-DD`.

## 1. Companies — `/api/opendata/companies`

| Field | Meaning | Example |
|---|---|---|
| `PoslovnoIme` | Full business name | `GRAFIČKO PREDUZEĆE GRAFOPRINT D O O GORNJI MILANOVAC` |
| `SifraOpstine` | Municipality code (registered seat) | `70483` |
| `NazivOpstine` | Municipality name (Cyrillic) | `ГОРЊИ МИЛАНОВАЦ` |
| `NazivStatus` | Status | `Активан`, `У стечају`, `У ликвидацији`, `У принудној ликвидацији` |
| `DatumOsnivanja` | Incorporation date | `1989-09-14` |
| `NazivPravneForme` | Legal form | `Друштво са ограниченом одговорношћу` |
| `SifraDelatnosti` | Activity code (KD2010 / NACE-aligned) | `1812` |

Observed distribution (133,357 records, 2026-05-31):
- Legal forms: DOO (LLC) 125,780; Zadruga (cooperative) 3,108; foreign rep. office
  1,284; foreign branch 920; AD (joint-stock) 716; partnership 618; public
  enterprise 539; …
- Statuses: Активан 124,931; У ликвидацији 6,226; У стечају 1,314; У принудној
  ликвидацији 886.

## 2. Financial statements — `/api/opendata/companies/financial-statements`

| Field | Meaning |
|---|---|
| `GodinaFi` | Reporting year of the statement (int) |
| `PoslovnoIme` | Business name as at statement date |
| `SifraOpstine` / `NazivOpstine` | Municipality code / name |
| `PoslovnaImovina` | Business assets |
| `Kapital` | Capital |
| `Gubitak` | Loss (accumulated) |
| `UkupniPrihodi` | Total revenue |
| `NetoDobitak` | Net profit |
| `NetoGubitak` | Net loss |
| `ProsecanBrojZaposlenih` | Average number of employees |

Join to companies on the MB key.

## 3. NGO — `/api/opendata/ngo`

| Field | Meaning |
|---|---|
| `Naziv` | Name |
| `SifraMesta` | Place code |
| `SifraDelatnosti` | Activity code |
| `DatumOsnivanja` | Founding date |
| `TipLica` | Entity type (e.g. `Удружење`, foundation, endowment) |
| `OblastiOstvarivanjaCiljeva` | Array of objects: areas/goals (Naziv + Opis) |

## Mapping to internal company model

| Internal field | Source (companies API) |
|---|---|
| `company_id` | MB (map key) |
| `registration_number` | MB |
| `tax_id` / `vat_id` | **not available** (open feed has no PIB) |
| `legal_name` | `PoslovnoIme` |
| `normalized_name` | derived (uppercase/trim of `PoslovnoIme`) |
| `company_type` | `NazivPravneForme` |
| `status` | `NazivStatus` (map: Активан→active, У стечају→bankruptcy, У ликвидацији→liquidation, У принудној ликвидацији→compulsory_liquidation) |
| `incorporation_date` | `DatumOsnivanja` |
| `dissolution_date` | **not available** |
| `registered_address` | **not available** (only municipality, not street) |
| `municipality` | `NazivOpstine` (+ `SifraOpstine`) |
| `region` | derive from municipality code if needed |
| `country` | `Serbia` (constant) |
| `source_url` | `https://openapi.apr.gov.rs/api/opendata/companies` |
| `source_name` | `APR Registar privrednih drustava` |
| `source_retrieved_at` | download timestamp / `DatumPreseka` |
| `raw_record` | full source object |

Financials (revenue, profit, assets, employees) come from the financial-statements
dataset joined on MB.

## Reference code lists to obtain

- `SifraOpstine` / `SifraMesta` → municipality/place lookup (RZS / APR code list).
- `SifraDelatnosti` → KD2010 (Serbian classification of activities, NACE Rev.2
  aligned) lookup for human-readable activity names.
