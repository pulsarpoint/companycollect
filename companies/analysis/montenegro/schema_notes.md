# Schema notes — Montenegro

## Identifiers

| Field | Description |
|---|---|
| **PIB — Poreski identifikacioni broj** | Tax id, **8-digit**. Company id / tax id. |
| **Registarski / Matični broj** | CRPS registration number. |
| **PDV broj** | VAT number — separate, only for VAT-registered entities. |

`PIB` is the primary join key across CRPS and the tax administration.

## CRPS business record (field model, portal currently down)

| Field (me) | English | Notes |
|---|---|---|
| Naziv | Business name | |
| PIB | Tax id (8-digit) | primary id |
| Registarski broj | Registration number | |
| Oblik organizovanja | Legal form | DOO/AD/OD/KD/preduzetnik |
| Status | Status | aktivno / u likvidaciji / u stečaju / brisano |
| Datum registracije | Registration date | |
| Sjedište | Registered seat / address | |
| Šifra djelatnosti | Activity code | KD (~NACE) |
| Osnivači | Founders | **PERSONAL DATA — redact** |
| Ovlašćeno lice | Authorised representative | **PERSONAL DATA — redact** |
| Finansijski izvještaji | Financial statements | filed at CRPS; not open |

## data.gov.me — Javna preduzeća (public enterprises) fields

`Naziv`, `Status` (Aktivna), `Tip` (e.g. Javno preduzeće / Državni fond), `Osnivač`
(e.g. Vlada CG), `Adresa`, `Website`, `Kontakt`, `Pravni osnov`. No PIB/registration
number.

## Legal forms (oblik organizovanja)

| Local | English |
|---|---|
| DOO (društvo sa ograničenom odgovornošću) | Limited liability company |
| AD (akcionarsko društvo) | Joint-stock company |
| OD (ortačko društvo) | General partnership |
| KD (komanditno društvo) | Limited partnership |
| preduzetnik | Sole entrepreneur |

## Status values

`aktivno` (active), `u likvidaciji` (in liquidation), `u stečaju` (in bankruptcy),
`brisano` (struck off).

## Internal model mapping

```
company_id          <- PIB (8-digit) [or CRPS registration number]
registration_number <- Registarski / Matični broj
tax_id              <- PIB
vat_id              <- PDV broj (separate)
legal_name          <- Naziv
company_type        <- Oblik organizovanja (DOO/AD/OD/KD)
status              <- Status (aktivno/likvidacija/stečaj/brisano)
incorporation_date  <- Datum registracije
registered_address  <- Sjedište
activity_code       <- Šifra djelatnosti (KD ~NACE)
financials          <- CRPS annual statements (NOT open)
owners/officers     <- Osnivači / Ovlašćeno lice (PERSONAL DATA — redact)
country             <- "Montenegro"
```

## Encoding / formats

- UTF-8; Montenegrin (Latin). Currency **EUR**. Dates dd.mm.yyyy.
- **No open financial statements**; CRPS portal must be online for per-company data.
