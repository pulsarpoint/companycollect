# Albania — Schema Notes

## Identifiers

- **NIPT/NUIS** (Numri i Identifikimit për Personin e Tatueshëm) — the unique
  business identifier: **letter + 8 digits + letter** (e.g. `K12345678L`,
  `L67508702G`). It is the **company id, the tax id, AND the VAT id** — Albania has
  VAT and the NIPT serves as the VAT number. Universal join key.

## QKB / Open Data Albania company fields

| Field | Meaning |
|---|---|
| NIPT/NUIS | Unique business id (= tax id = VAT id) |
| Emri | Company name |
| Forma ligjore | Legal form (Sh.p.k. = Ltd, Sh.a. = JSC, Person Fizik = sole trader, Degë = branch) |
| Data e regjistrimit | Registration date |
| Administrator | Administrator (director) — **personal data** |
| Ortakë / Aksionarë | Owners / shareholders — **personal data** |
| Kapitali | Capital (ALL) |
| Objekti i veprimtarisë | Activity (free text; NACE-aligned) |
| Adresa | Registered address |
| Statusi | Status (Aktiv / Pasiv / Çregjistruar) |
| Emra të mëparshëm (ish) | Former names |

## Financials

Annual financial statements (bilanci / pasqyrat financiare), filed with QKB,
currency **ALL (Lek)**. No clean open bulk; some indicators via Open Data Albania.

## Dates, money, encoding

- Dates: `DD.MM.YYYY` — normalize to `YYYY-MM-DD`.
- Money: **ALL (Albanian Lek)**.
- Encoding: UTF-8 (Albanian characters: ç ë).

## Internal model mapping

```text
company_id          <- NIPT/NUIS
registration_number <- NIPT/NUIS
tax_id              <- NIPT/NUIS
vat_id              <- NIPT/NUIS (NIPT serves as the VAT id)
legal_name          <- Emri
company_type        <- Forma ligjore (Sh.p.k. / Sh.a. / Person Fizik / Degë)
status              <- Statusi (Aktiv/Pasiv/Çregjistruar)
incorporation_date  <- Data e regjistrimit
registered_address  <- Adresa
activity            <- Objekti i veprimtarisë (free text)
financials          <- bilanci (QKB; ALL)
officers            <- Administrator / Ortakë (personal data) — redact
```
