# Schema notes — Nigeria

## Identifiers

| Field | Description |
|---|---|
| **RC number** | Registration of Company number — limited companies (e.g. `RC 123456`). Company id for companies. |
| **BN number** | Business Name registration — sole proprietorships / partnerships. |
| **IT number** | Incorporated Trustees — NGOs / associations. |
| **TIN** | Tax Identification Number (FIRS). |
| **VAT** | VAT registration (FIRS). |

`RC number` is the company id for limited companies; **TIN** links tax.

## NGX equities API record (open) — observed fields

| Field | Meaning | Notes |
|---|---|---|
| `Symbol` | Ticker symbol | e.g. DANGCEM, MTNN, GTCO |
| `Sector` | NGX sector | FINANCIAL SERVICES / ICT / INDUSTRIAL GOODS / CONSUMER GOODS / OIL AND GAS / SERVICES |
| `Market` | Listing board | Premium Board / Main Board / Growth Board |
| `OpeningPrice` / `ClosePrice` / `HighPrice` / `LowPrice` | Daily prices (NGN) | |
| `Change` / `PercChange` | Daily change | |
| `Trades` / `Volume` / `Value` | Trading activity | Value in NGN |
| `TradeDate` | Trade date | |

NGX also publishes listed-company **financial statements** and a delisted list.

## CAC company record (field model, gated/paid — from public knowledge)

| Field | Notes |
|---|---|
| Company name | Plc / Ltd / Ltd/Gte |
| RC number | |
| Company type | Private/Public Ltd, Ltd by guarantee, business name, incorporated trustees |
| Status | Active / Inactive / Dissolved / Delisted |
| Registration date | |
| Registered address | |
| Nature of business | |
| Share capital | NGN |
| Directors | **PERSONAL DATA — redact** |
| Shareholders | **PERSONAL DATA — redact** |

## CAC BO Register (PSC) — fields (token-gated)

`company_name`, `rc_number`, **`psc_name`** (beneficial owner — PERSONAL DATA),
`nature_of_control`, `shareholding_percent`, `nationality`.

## Company types

| Type | Notes |
|---|---|
| Plc (Public limited company) | listed/public |
| Ltd (Private limited company) | most companies |
| Ltd/Gte (Limited by guarantee) | non-profit |
| Business Name (Enterprise) | sole prop / partnership (BN) |
| Incorporated Trustees | NGO / association (IT) |

## Status values

`Active`, `Inactive`, `Dissolved`, `Delisted`.

## Internal model mapping

```
company_id          <- RC number (companies) / BN / IT
registration_number <- RC / BN / IT number
tax_id              <- TIN (FIRS)
vat_id              <- VAT registration (FIRS)
legal_name          <- Company name
company_type        <- Plc / Ltd / Ltd-Gte / Business Name / Incorporated Trustees
status              <- Active/Inactive/Dissolved/Delisted
registered_address  <- Registered address
activity_code       <- Nature of business
capital             <- Share capital (NGN)
financials          <- CAC AFS (paid) / NGX (listed), NGN
owners/officers     <- Directors/Shareholders/PSC (PERSONAL DATA — redact)
country             <- "Nigeria"
```

## Encoding / formats

- UTF-8; English. Currency **NGN**. Dates dd-mm-yyyy.
- Only NGX (listed) is open; CAC is Cloudflare-gated/paid; BO register is token-gated.
