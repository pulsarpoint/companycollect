# Schema notes — Kosovo

## Identifiers

| Field | Description |
|---|---|
| **NUI — Numri Unik Identifikues** | Unique Identification Number. For businesses = **Numri Fiskal** (fiscal/tax number), 9-digit. Primary company id. |
| **Numri i Biznesit / NRB** | Business registration number (Numri Regjistral i Biznesit). |
| **Numri Fiskal** | Fiscal/tax number (= NUI for businesses). |
| **Numri i TVSH-së** | VAT number — separate, only for VAT-registered entities. |

`NUI` (= fiscal number) is the join key across ARBK and ATK.

## ARBK business record (field model from the SPA)

| Field (sq) | English | Notes |
|---|---|---|
| Numri Unik Identifikues | Unique id (NUI) | = Numri Fiskal; primary id |
| Numri i Biznesit (NRB) | Business reg number | |
| Numri Fiskal | Fiscal number | |
| Numri i TVSH | VAT number | separate; if VAT-registered |
| Emri (i Biznesit) | Business name | |
| Statusi i Biznesit | Status | Aktiv / Pasiv / Shuar (active/passive/dissolved) |
| Data e Regjistrimit | Registration date | |
| Lloji i Biznesit | Business type / legal form | B.I., O.P., Sh.P.K., Sh.A., etc. |
| Komuna | Municipality | |
| Adresa | Address | |
| Aktiviteti Kryesor | Primary activity | NACE/activity code list |
| Aktivitetet (Tjera) | Other activities | array |
| Sektori | Sector | |
| Kapitali (+ %) | Capital (+ share %) | EUR |
| Pronari / Pronarët | Owner(s) | **PERSONAL DATA — redact** |
| Pronari Huaj (%) | Foreign owner (%) | |
| Numri i Punëtorëve | Number of employees | |
| Numri i Njësive | Number of units/branches | |

Endpoints (gated): `Services/KerkoBiznesin` (search, Turnstile),
`Services/TeDhenatBiznesit` (details), `Services/EksportoBizneset` (export).

## ATK VatRegist output fields (per-company, CAPTCHA-gated)

`FiscalNo`, `NrbID`, `TpStatus`, `TpName`, `Address`, `CityName`, `ParishName`,
`TaxCentreName`, `VatNo`, `VatTypeAl`.

## Legal forms (Lloji i Biznesit)

| Local | English |
|---|---|
| B.I. (Biznes Individual) | Individual business / sole trader |
| O.P. (Ortakëri e Përgjithshme) | General partnership |
| Sh.P.K. (Shoqëri me Përgjegjësi të Kufizuar) | Limited liability company |
| Sh.A. (Shoqëri Aksionare) | Joint-stock company |
| Dega e shoqërisë së huaj | Branch of a foreign company |

## Status values

`Aktiv` (active), `Pasiv` (passive/inactive), `Shuar` / `Çregjistruar`
(dissolved/deregistered).

## Internal model mapping

```
company_id          <- NUI (Numri Unik Identifikues = Numri Fiskal, 9-digit)
registration_number <- Numri i Biznesit / NRB
tax_id              <- Numri Fiskal (= NUI)
vat_id              <- Numri i TVSH-së (separate)
legal_name          <- Emri i Biznesit
company_type        <- Lloji i Biznesit (B.I./O.P./Sh.P.K./Sh.A.)
status              <- Statusi i Biznesit (Aktiv/Pasiv/Shuar)
incorporation_date  <- Data e Regjistrimit
registered_address  <- Adresa
municipality        <- Komuna
activity_code       <- Aktiviteti Kryesor
capital             <- Kapitali (EUR)  [only financial-ish field that is open]
owners              <- Pronarët (PERSONAL DATA — redact)
employees           <- Numri i Punëtorëve
country             <- "Kosovo"
```

## Encoding / formats

- UTF-8; tri-lingual (Albanian / Serbian / English).
- Currency **EUR**. Dates dd.MM.yyyy.
- **No open financial statements** — only ARBK registered capital + ATK aggregates.
