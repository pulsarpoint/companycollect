# Lithuania — Schema Notes

## Identifiers

- **įmonės kodas / juridinio asmens kodas** (`ja_kodas`) — **9-digit** company
  code. Company id, legal-entity **taxpayer code**, and **universal join key**
  across all JAR models.
- **PVM kodas** (VAT) — `LT` + digits; a separate registration, **not** in the JAR
  base identity model. Verify via **EU VIES**. Lithuania uses EU VAT.
- **`_id`** — a UUID assigned by the Spinta API to each row; the API's internal
  join key (references point to it). Not a business identifier — prefer `ja_kodas`
  for stable joins.

## API / record shape (Spinta, `get.data.gov.lt`)

- Base: `https://get.data.gov.lt/datasets/gov/rc/jar/{model}`.
- Each row has `_type`, `_id` (UUID), `_revision`, `_base`, plus business fields.
- References are `{"_id": "<uuid>"}` pointers (e.g. `forma`, `statusas`,
  `juridinis_asmuo`) — resolve by joining on `_id`.
- Pagination: `?limit(N)` and the opaque cursor `_page.next`. Output JSON or CSV
  (`?format(csv)`); UTF-8. **No API key.**

## `iregistruoti/JuridinisAsmuo` (identity)

| Field | Meaning |
|---|---|
| ja_kodas | Company code (9-digit) — key |
| ja_pavadinimas | Legal name |
| pilnas_adresas / adresas | Address (often null here; see Buveine) |
| reg_data | Registration date (YYYY-MM-DD) |
| isreg_data | Deregistration date (null if active) |
| forma._id | Legal form → `formos_statusai/Forma` |
| statusas._id | Legal status → `formos_statusai/Statusas` |
| stat_data | Status date |

## `formos_statusai/Statusas` (status code list, 31 rows)

`kodas` + `pavadinimas` (LT) + `name` (EN). Examples:
`0` not registered · `1` under reorganization · `3` under reformation ·
`4` under restructuring · `5` going bankrupt · `6` bankrupt · `7` under
liquidation · `9` liquidation initiated · `10` removed · `11` liquidated.

## `formos_statusai/Forma` (legal form code list, 168 rows)

`kodas` + `pavadinimas` (LT) + `pav_ilgas` (long) + `name` (EN) + `tipas`/`type`.
Examples: `110` Valstybės įmonė (State Enterprise). Common business forms include
`AB` (akcinė bendrovė / public limited) and `UAB` (uždaroji akcinė bendrovė /
private limited).

## `buveines/Buveine` (addresses)

`juridinis_asmuo` (ref), `adresas` (address text), `adresas_nuo` (valid-from date).

## `balanso_ataskaitos/BalansoAtaskaita` (balance sheet)

| Field | Meaning |
|---|---|
| juridinis_asmuo._id | Company reference |
| template_id / template_name | Statement-set template (e.g. FS0422 small-partnership set) |
| standard_id / standard_name | Statement standard (e.g. BST124 BALANSAS Sutrumpintas) |
| line_type_id / line_name | Line item (e.g. BSLT00021 TRUMPALAIKIS TURTAS = current assets) |
| reiksme | Value (EUR) |
| laikotarpis_nuo / laikotarpis_iki | Period from / to |
| reg_date | Filing registration date |

## `pelno_ataskaitos/PelnoAtaskaita` (profit & loss)

Same shape as the balance sheet; line items such as `PARDAVIMO PAJAMOS` (sales
revenue). Example: `reiksme` 58708 EUR, period 2021. `standard_name` e.g.
`PELNO (NUOSTOLIŲ) ATASKAITA (Trumpa)`.

> Financials are **granular line items** (one row per account), not a single
> aggregated record. Aggregate per company + period to build a statement.

## Dates, money, encoding

- Dates: ISO `YYYY-MM-DD`.
- Money: integers in **EUR**.
- Encoding: UTF-8 JSON (Lithuanian diacritics).

## Internal model mapping

```text
company_id          <- ja_kodas (9-digit company code)
registration_number <- ja_kodas
tax_id              <- ja_kodas (legal-entity taxpayer code)
vat_id              <- null in JAR (PVM kodas via VIES)
legal_name          <- ja_pavadinimas
status              <- statusas (Statusas code list; isreg_data => deregistered)
legal_form          <- forma (Forma code list)
incorporation_date  <- reg_data
dissolution_date    <- isreg_data
registered_address  <- Buveine.adresas
financials          <- BalansoAtaskaita + PelnoAtaskaita line items (EUR)
officers            <- valdymo_organai (personal data, GDPR)
```
