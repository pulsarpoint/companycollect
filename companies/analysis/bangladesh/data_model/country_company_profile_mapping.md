# Bangladesh Company Profile — Mapping

Bangladesh splits between an **open listed layer** (DSE) and a **gated/paid full register**
(RJSC). The **Dhaka Stock Exchange** is the cleanest open source (~640 listed instruments,
plain parseable HTML, keyed on the **DSE trading code**). The authoritative **RJSC** register
(keyed on the **RJSC registration number**) has a free name search but **pay-per-use**
documents (planning-only). The **NBR** adds tax identity (**BIN** / **e-TIN**) via per-ID
verification. There is **no single national identifier** shared across all sources — they join
by **name**.

## Identifiers

- **RJSC registration number** — authoritative registrar id (RJSC; paid).
- **DSE trading code** (+ **scrip code**) — listed-company key (DSE; open).
- **BIN** (VAT) / **e-TIN** (income tax) — NBR tax identifiers (per-ID verification).

## Mapping table

| Profile path | Source | Source path | Join key | Precedence | Notes |
|---|---|---|---|---|---|
| registration.rjsc_registration_number | rjsc_register | rjsc_registration_number | yes | RJSC | documents paid (planning-only) |
| registration.dse_trading_code | dse_listed | trading_code | yes | DSE | open; listed only |
| registration.bin / e_tin | nbr_tax | bin / e_tin | yes | NBR | per-ID verification |
| legal_identity.legal_name | dse_listed | company_name | yes | RJSC > DSE | RJSC authoritative; DSE open |
| legal_identity.entity_type | rjsc_register | entity_type | no | RJSC | company/firm/society (paid) |
| status.registration_status | rjsc_register | status | no | RJSC | active/struck-off (paid) |
| status.vat_status | nbr_tax | vat_status | no | NBR | VAT status |
| status.incorporation_date | rjsc_register | registration_date | no | RJSC | true incorporation date (paid) |
| status.listing_year | dse_listed | listing_year | no | DSE | NOT incorporation date |
| activity.sector | dse_listed | sector | no | DSE | listed only |
| registered_location.registered_address | rjsc_register | registered_address | no | RJSC | paid |
| officers[] | rjsc_register | directors | no | RJSC | **PERSONAL DATA — REDACT** (paid) |
| listing.* | dse_listed | trading_code/scrip_code/market_category/type_of_instrument | no | DSE | listed only |
| financial_statements[] | dse_listed | authorized_capital_mn / paid_up_capital_mn | no | DSE > RJSC | BDT mn (listed); RJSC capital for all (paid) |
| source_provenance[] | all | n/a | n/a | n/a | per-section provenance |

## Precedence and joins

- **Listed identity (name, sector, capital, listing)**: from **DSE** (free, open). **Full
  registry identity (RJSC number, entity type, status, incorporation date, registered office,
  directors, all entity types)**: from **RJSC** (paid, planning-only). **Tax identity (BIN,
  e-TIN, VAT status)**: from **NBR** (per-ID verification).
- **Join**: all three join by **company name** (no shared national id; DSE has no RJSC number,
  RJSC/NBR not openly cross-linked). The **RJSC registration number** is canonical once obtained.
- **Do not conflate dates**: DSE `listing_year` ≠ RJSC `incorporation_date`.
- **Language** English + Bangla; **currency** BDT (DSE capital in millions).

## Missing / restricted

- **The full register (RJSC) is gated/paid** → its fields are **planning-only**; only the free
  name search (name + number) is open. **Directors** are personal data (paid documents) — redact.
- **DSE** covers **listed companies only** (~640); the broader company population needs RJSC.
- **NBR** is **per-BIN/TIN verification** (no bulk) and covers individuals (personal data — redact).
- **data.gov.bd** (DKAN) has no company register (statistics only).
