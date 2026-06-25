# Schema notes — Ghana

## Identifiers

| Field | Description |
|---|---|
| **Company registration number** | ORC-issued company registration number (new CIN-style format). Company id. |
| **TIN — Tax Identification Number** | Ghana Revenue Authority (businesses). |
| **Ghana Card PIN** | Individuals' national ID/tax number (replacing individual TINs; appears for directors/sole proprietors). |
| **Business registration number** | Sole proprietorships / partnerships. |

`Company registration number` is the company id; **TIN** links tax.

## GSE listed-company record (open) — observed fields

| Field | Meaning | Notes |
|---|---|---|
| company_name | Listed company name | e.g. Ecobank Ghana PLC |
| ticker | GSE ticker | e.g. EGH, GCB, AGA (public knowledge) |
| sector | GSE sector | Banking / Mining / Manufacturing / Agriculture / ICT |
| profile | Company profile | /profile-of-listed-companies/ |
| financial_statements | Financial statements | GHS |

## ORC company record (field model, eServices/paid — public knowledge)

| Field | Notes |
|---|---|
| Company name | Ltd / PLC / Ltd by guarantee |
| Registration number | |
| Company type | Company limited by shares / by guarantee / unlimited / external (foreign) |
| Status | Active / In good standing / Dissolved / Struck off |
| Incorporation date | |
| Registered office address | |
| Nature of business | |
| Stated capital | GHS |
| Directors | **PERSONAL DATA — redact** |
| Shareholders / subscribers | **PERSONAL DATA — redact** |
| TIN | |

## Company types

| Type | Notes |
|---|---|
| Company limited by shares (Ltd / PLC) | most companies |
| Company limited by guarantee | non-profit |
| Unlimited company | |
| External company | branch of a foreign company |
| Sole proprietorship / partnership (business name) | registered business name |

## Status values

`Active` / `In good standing`, `Dissolved`, `Struck off`, `In receivership`.

## Internal model mapping

```
company_id          <- Company registration number (ORC)
registration_number <- Company registration number / business registration
tax_id              <- TIN (GRA)
vat_id              <- none separate (VAT registration tied to the TIN)
legal_name          <- Company name
company_type        <- Ltd by shares / by guarantee / unlimited / external
status              <- Active/Dissolved/Struck off/In receivership
registered_address  <- Registered office address
activity_code       <- Nature of business
capital             <- Stated capital (GHS)
financials          <- GSE (listed) / ORC annual returns (paid), GHS
owners/officers     <- Directors/Shareholders (PERSONAL DATA — redact)
country             <- "Ghana"
```

## Encoding / formats

- UTF-8; English. Currency **GHS** (Ghana cedi). Dates dd/mm/yyyy.
- Only GSE (listed) is open; ORC is eServices/paid (firewalled here); data.gov.gh
  firewalled.
