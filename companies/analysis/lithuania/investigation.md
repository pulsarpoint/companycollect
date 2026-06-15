# Lithuania Company Data Investigation

## Conclusion

Lithuania is a **fully-open tier-1 source**: both the **company register** and
**financial statements** are openly available with **no API key**, via the modern
data.gov.lt Spinta REST API.

- **Identity (open):** Registrų centras **Juridinių asmenų registras (JAR,
  Register of Legal Entities)**, namespace `gov/rc/jar`. The `JuridinisAsmuo` model
  gives the **company code (įmonės kodas, 9-digit)**, name, legal form, status, and
  registration/deregistration dates. `Buveine` holds addresses; `Forma` (168) and
  `Statusas` (31) are code lists with **Lithuanian and English** labels.
- **Financials (open):** `BalansoAtaskaita` (balance sheet) and `PelnoAtaskaita`
  (profit & loss) — granular line items in **EUR**, per fiscal period, linked to a
  company. This is rare and valuable: open, structured financial statements for the
  whole filing population.

## What was verified live

- `gov/rc/jar` namespace enumerated: `iregistruoti` (JuridinisAsmuo),
  `balanso_ataskaitos`, `pelno_ataskaitos`, `buveines`, `ja_kapitalas`,
  `valdymo_organai`, `formos_statusai`, plus NGO / charity-recipient / late-filer
  models.
- **JuridinisAsmuo**: real records, e.g. `ja_kodas` 110000291 — "Bendra Lietuvos –
  JAV įmonė … STT Inc.", `reg_data` 1991-03-11, `isreg_data` 2002-03-22.
- **BalansoAtaskaita**: real line item `TRUMPALAIKIS TURTAS` (current assets)
  `reiksme` 13532, period 2023-01-01…2023-12-31.
- **PelnoAtaskaita**: real line item `PARDAVIMO PAJAMOS` (sales revenue) `reiksme`
  58708, period 2021.
- **Statusas** code list resolved (kodas + LT `pavadinimas` + EN `name`): 0 not
  registered, 5 going bankrupt, 6 bankrupt, 7 under liquidation, 10 removed, 11
  liquidated, etc.
- **Forma** code list: 168 forms with LT + EN labels (e.g. 110 Valstybės įmonė /
  State Enterprise).

## API shape (Spinta)

- Base: `https://get.data.gov.lt/datasets/gov/rc/jar/{model}`.
- Browse namespaces with the `/:ns` suffix; query a model directly for rows.
- `?limit(N)` bounds rows; pagination via the opaque `_page.next` cursor.
- Records carry `_id` (UUID), `_revision`, and `_type`. References (`forma`,
  `statusas`, `juridinis_asmuo`) are `{"_id": "<uuid>"}` pointers into other models
  — resolve by joining on `_id`.
- **No API key.** Rapid sequential bursts occasionally return curl code 000
  (transient); pace requests.

## Identifiers

- **įmonės kodas / juridinio asmens kodas** (`ja_kodas`) — **9-digit** company
  code; the company id, the legal-entity **taxpayer code**, and the **universal
  join key** across every JAR model.
- **PVM kodas** (VAT) — `LT` + digits; a **separate** registration, **not in the
  JAR base identity model**. Lithuania uses EU VAT — verify via **EU VIES**.

## Join model

- `JuridinisAsmuo._id` (UUID) ↔ `BalansoAtaskaita.juridinis_asmuo._id`,
  `PelnoAtaskaita.juridinis_asmuo._id`, `Buveine.juridinis_asmuo`, etc. The
  human-readable join is the **company code** (`ja_kodas`); the API's internal join
  is the **`_id` UUID**.
- `forma._id` → `Forma`; `statusas._id` → `Statusas`.

## What is NOT in the base identity model

- **Address** is in `Buveine`, not always inline on `JuridinisAsmuo` (samples had
  `adresas` null).
- **VAT number** — separate (VIES).
- **Directors / management** — `valdymo_organai` (personal data, GDPR).

## Recommended ingestion

1. Pull `JuridinisAsmuo` (identity) + `Buveine` (address); resolve `Forma` /
   `Statusas` code lists once.
2. Pull `BalansoAtaskaita` + `PelnoAtaskaita` and aggregate line items per company
   + period for financials (EUR).
3. Optionally enrich with `ja_kapitalas` (capital) and `valdymo_organai`
   (directors — redact personal data).
4. All keyless; paginate via `_page.next`; pace requests.
