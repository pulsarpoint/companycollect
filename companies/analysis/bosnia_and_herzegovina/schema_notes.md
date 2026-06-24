# Schema notes — Bosnia and Herzegovina

## Identifiers

| Field | Description |
|---|---|
| **JIB** | Jedinstveni identifikacioni broj — **13-digit** unique id = company id = tax id. RS legal entities start `44…`. Country-wide join key. |
| **MBS** | Matični broj subjekta — court registration number (registarski uložak). |
| **MB** | Matični broj — 7-digit statistical number. |
| **PDV broj** | **12-digit VAT number**, separate, assigned by UINO for VAT-registered entities (single state-level VAT/PDV). |

`JIB` is the primary join key across registers (court + financial + tax).

## RS register JSON (`/Home/SearchPoslovniSubjekt`) — observed fields

| Path | Meaning | Notes |
|---|---|---|
| `PrivrednoDrustvoId` | Internal subject id | used in `/Home/PregledPoslovnogSubjekta/{id}` and PDF extract |
| `JIB` | 13-digit unique id (company id = tax id) | join key |
| `MBS` | Court registration number | |
| `MB` | Statistical number (7-digit) | |
| `PoslovnoIme` | Full business name | Latin/Cyrillic |
| `SkracenoPoslovnoIme` | Short name | nullable |
| `Sjediste` | Registered seat / address | |
| `PreteznaDjelatnost` | Primary activity (KD BiH ~ NACE), e.g. `64.19` | code + text |
| `StatusPoslovniSubjekatOpis` | Status (e.g. `registrovan`) | |
| `Osnivaci` | Founders | **PERSONAL DATA if individuals — redact** |
| `OdgovornoLice` | Responsible person | **PERSONAL DATA — redact** |
| `Email`, `Telefon` | Contacts | |
| `PoslovneJedinice` | Number of business units/branches | integer |

Per-company **PDF official extract**: `/Home/DetaljiPoslovnogSubjekta/{id}`.

## Legal forms (oblik organizovanja)

| Local | English |
|---|---|
| d.o.o. (društvo sa ograničenom odgovornošću) | Limited liability company |
| a.d. / d.d. (akcionarsko / dioničko društvo) | Joint-stock company |
| s.p. / preduzetnik / obrt | Sole trader / entrepreneur |
| javno preduzeće / javna ustanova | Public enterprise / institution |
| podružnica / poslovna jedinica | Branch / business unit |

## Status values

`registrovan` (registered/active), `u likvidaciji` (in liquidation), `u stečaju`
(in bankruptcy), `brisan` (struck off / deleted).

## Financial statements (APIF RFI / FIA) — paid per company

Annual **bilans stanja** (balance sheet) and **bilans uspjeha** (income
statement); fiscal year; currency **BAM (KM)**. Per-company paid; no open bulk —
modelled as planning-only.

## Internal model mapping

```
company_id          <- JIB (13-digit)
registration_number <- MBS (court) / MB (statistical)
tax_id              <- JIB
vat_id              <- PDV broj (12-digit, UINO, separate)
legal_name          <- PoslovnoIme
company_type        <- legal form (from name / extract)
status              <- StatusPoslovniSubjekatOpis
registered_address  <- Sjediste
municipality/region <- from Sjediste / entity (RS / FBiH / Brčko)
activity_code       <- PreteznaDjelatnost
financials          <- APIF RFI / FIA (paid, per company; BAM)
owners/officers     <- Osnivaci / OdgovornoLice (PERSONAL DATA — redact)
country             <- "Bosnia and Herzegovina"
```

## Encoding / formats

- UTF-8; names in Latin and Cyrillic (RS is bi-scriptal).
- Dates dd.mm.yyyy. Currency BAM.
