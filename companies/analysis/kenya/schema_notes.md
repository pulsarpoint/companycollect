# Schema notes — Kenya

## Identifiers

| Field | Description |
|---|---|
| **Company registration number** | BRS-issued. Old formats `C.NNNNN` / `CPR/2015/NNNNNN`; new eCitizen format e.g. `PVT-XXXXXXX`. Company id. |
| **BN number** | Business Name registration — sole proprietorships / partnerships. |
| **KRA PIN** | Kenya Revenue Authority tax id (e.g. `P051234567X` for companies; `A…` for individuals). |
| **VAT** | VAT obligation registered **under the KRA PIN** — no separate VAT number. |

`Company registration number` is the company id; **KRA PIN** links tax.

## NSE listed-company record (open) — observed fields

| Field | Meaning | Notes |
|---|---|---|
| company_name | Listed company name | e.g. Absa Bank Kenya PLC |
| ticker | NSE ticker | e.g. ABSA, SBIC, SASN (public knowledge) |
| sector_segment | NSE sector / market segment | Banking / Agricultural / Commercial & Services / Manufacturing / Investment |
| announcements | Listed-company announcements | financial results, notices |
| financial_results | Financial statements | KES |

## BRS company record (field model, eCitizen/paid — from public knowledge)

| Field | Notes |
|---|---|
| Company name | Ltd / PLC / company limited by guarantee |
| Registration number | |
| Company type | Private Ltd / Public Ltd / Ltd by guarantee / Business name |
| Status | Active / Dormant / Dissolved / Struck off |
| Registration date | |
| Registered office address | |
| Nominal / issued capital | KES |
| Directors | **PERSONAL DATA — redact** (CR12) |
| Shareholders | **PERSONAL DATA — redact** (CR12) |
| KRA PIN | |

## Company types

| Type | Notes |
|---|---|
| Private limited company (Ltd) | most companies |
| Public limited company (PLC) | listed/public |
| Company limited by guarantee (CLG) | non-profit |
| Business name | sole proprietorship / partnership (BN) |
| Limited liability partnership (LLP) | |

## Status values

`Active`, `Dormant`, `Dissolved`, `Struck off`.

## Internal model mapping

```
company_id          <- Company registration number (BRS)
registration_number <- Company registration number / BN
tax_id              <- KRA PIN
vat_id              <- none separate (VAT under the PIN)
legal_name          <- Company name
company_type        <- Private/Public Ltd / CLG / Business name / LLP
status              <- Active/Dormant/Dissolved/Struck off
registered_address  <- Registered office address
activity_code       <- (not consistently coded; NSE sector for listed)
capital             <- Nominal / issued capital (KES)
financials          <- NSE (listed) / BRS annual returns (paid), KES
owners/officers     <- Directors/Shareholders (CR12 — PERSONAL DATA, redact)
country             <- "Kenya"
```

## Encoding / formats

- UTF-8; English. Currency **KES**. Dates dd/mm/yyyy.
- Only NSE (listed) is open; BRS is eCitizen-login/paid; KODI has no company dataset.
