# Schema notes — North Macedonia

## Identifiers

| Field | Description |
|---|---|
| **ЕМБС — Единствен матичен број на субјектот** | 7-digit unique entity registration number = company id. Primary join key. |
| **ЕДБ — Единствен даночен број** | 13-digit tax number. |
| **ДДВ број** | VAT registration (UJP). MK VAT. |

`ЕМБС` is the primary join key; `ЕДБ` is the tax id (links to UJP).

## CRM Trade Registry record (field model, from public docs)

| Field (mk) | English | Notes |
|---|---|---|
| Назив / Име | Business name | Cyrillic (+ Latin/Albanian) |
| ЕМБС | Entity registration number (7-digit) | primary id |
| ЕДБ | Tax number (13-digit) | tax id |
| Правна форма | Legal form | ДОО/ДООЕЛ/АД/ТП/ЈТД/КД |
| Седиште / Адреса | Registered seat / address | |
| Дејност (НКД) | Activity code | NKD (~NACE) |
| Статус | Status | активен / во ликвидација / во стечај / избришан |
| Управители / Основачи | Managers / founders | **PERSONAL DATA — redact** |
| Основна главнина | Registered capital | MKD |

## Registry of Annual Accounts (financials, paid)

| Field | English | Notes |
|---|---|---|
| Биланс на состојба | Balance sheet | MKD |
| Биланс на успех | Income statement | MKD |
| Деловна година | Fiscal year | |
| Број на вработени | Employees | |

## Legal forms (правна форма)

| Local | English |
|---|---|
| ДОО (друштво со ограничена одговорност) | Limited liability company |
| ДООЕЛ (ДОО основано од едно лице) | Single-member LLC |
| АД (акционерско друштво) | Joint-stock company |
| ТП (трговец поединец) | Sole trader |
| ЈТД (јавно трговско друштво) | General partnership |
| КД (командитно друштво) | Limited partnership |
| Подружница | Branch |

## Status values

`активен` (active), `во ликвидација` (in liquidation), `во стечај` (in bankruptcy),
`избришан` (struck off).

## Internal model mapping

```
company_id          <- ЕМБС (7-digit)
registration_number <- ЕМБС
tax_id              <- ЕДБ (13-digit)
vat_id              <- ДДВ број (UJP)
legal_name          <- Назив / Име
company_type        <- Правна форма (ДОО/ДООЕЛ/АД/ТП…)
status              <- Статус (активен/ликвидација/стечај/избришан)
registered_address  <- Седиште / Адреса
activity_code       <- Дејност (НКД ~NACE)
financials          <- Registry of Annual Accounts (баланс/успех, MKD; PAID)
owners/officers     <- Управители / Основачи (PERSONAL DATA — redact)
country             <- "North Macedonia"
```

## Encoding / formats

- UTF-8; Macedonian (Cyrillic) + Albanian + Latin transliteration.
- Currency **MKD**. Dates dd.mm.yyyy.
- Register + financials are **paid** (CRM distribution); only basic search is free.
