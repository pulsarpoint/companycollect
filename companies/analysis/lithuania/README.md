# Company data sources for Lithuania

## Status

- Official bulk data: **found** (Registrų centras JAR — full register via the open data.gov.lt API)
- Official API: **found** (data.gov.lt Spinta REST API — no key required)
- Open data portal: **found** (data.gov.lt — `get.data.gov.lt`)
- License: **known-ish** — open data (CC-BY 4.0 style); confirm per dataset
- Recommended ingestion path: **API** (paginated REST over the JAR models)

## Best source

**Registrų centras — Juridinių asmenų registras (JAR, Register of Legal Entities)**
published as **open data** through Lithuania's national portal **data.gov.lt** via
the modern **Spinta REST API** at `get.data.gov.lt`. **No API key required.**

Every legal entity is keyed by its **įmonės kodas / juridinio asmens kodas
(company code, 9-digit)**. The `gov/rc/jar` namespace exposes a rich set of models:

- **`iregistruoti/JuridinisAsmuo`** — registered entities (company code, name,
  legal form, status, registration & deregistration dates).
- **`buveines/Buveine`** — registered addresses.
- **`ja_kapitalas`** — capital; **`valdymo_organai`** — management bodies (directors).
- **`formos_statusai/Forma` + `/Statusas`** — legal-form (168) and status (31) code
  lists, with Lithuanian **and English** labels.

Verified live: pulled real records — e.g. company code `110000291`,
"Bendra Lietuvos – JAV įmonė … STT Inc." (registered 1991-03-11).

## Financial data — also fully open

Lithuania publishes **financial statements** openly through the same API:

- **`balanso_ataskaitos/BalansoAtaskaita`** — balance-sheet line items.
- **`pelno_ataskaitos/PelnoAtaskaita`** — profit & loss line items.

Each row is one statement line (e.g. `PARDAVIMO PAJAMOS` / sales revenue €58,708
for FY2021; `TRUMPALAIKIS TURTAS` / current assets €13,532 for FY2023), linked to
a company, with the period and **EUR** value. This makes Lithuania a **fully-open
tier-1 source**: open identity register **and** open financial statements, no key.

## Identifiers & tax

- **įmonės kodas** (company code, 9-digit) — company id and the legal-entity
  taxpayer code. Universal join key across all JAR models.
- **PVM kodas** (VAT) — `LT` + digits; a separate registration, **not in the JAR
  base model** (check via EU VIES). Lithuania uses EU VAT.

## Next action

Ingest `JuridinisAsmuo` (+ `Buveine`, `Forma`/`Statusas`) for identity, then
`BalansoAtaskaita` + `PelnoAtaskaita` for financials, joined on the company code.
All via the keyless paginated API.
