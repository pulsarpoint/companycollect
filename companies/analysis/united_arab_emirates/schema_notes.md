# Schema notes — United Arab Emirates

## Identifiers

| Field | Description |
|---|---|
| **Trade / Commercial License number** | Issued per **emirate DED** (Dubai/Abu Dhabi/Sharjah/…) or **free zone**. The mainland company id (per-emirate). |
| **Economic register number** | National unified number under the **NER** (Ministry of Economy). |
| **TRN — Tax Registration Number** | 15-digit (FTA; VAT + corporate tax). |
| **Free-zone registration number** | DIFC / ADGM / DMCC / JAFZA registration number. |

There is **no single national company id** — identity depends on the registering
authority (emirate DED, free zone, or NER unified number).

## NER / emirate DED record (field model, gated — public knowledge)

| Field | Notes |
|---|---|
| Legal / trade name | Arabic + English |
| Trade license number | per emirate / free zone |
| Economic register number | NER unified |
| License authority | emirate DED / free zone |
| Legal form | LLC / PJSC / PrJSC / Sole Establishment / Branch / Civil Company |
| Status | Active / Expired / Cancelled |
| Activities | DED activity codes (ISIC-based) |
| Emirate | Dubai / Abu Dhabi / Sharjah / … |
| Establishment / issue date | |
| Expiry date | trade-license expiry |
| Owners / partners / managers | **PERSONAL DATA — redact** |

## Free-zone register record (DIFC / ADGM)

| Field | Notes |
|---|---|
| Entity name | |
| Registration number | DIFC / ADGM number |
| Legal form | Private/Public company, branch, SPV, fund, partnership |
| Status | Active / Dissolved / Struck off |
| Registered address | within the free zone |
| Incorporation date | |

## DFM / ADX (listed) record

`company_name`, `symbol`, `isin` (AE…), `sector`, `financial_statements` (AED),
`disclosures`. Listed companies only.

## Company types

| Type | Notes |
|---|---|
| LLC (Limited Liability Company) | most mainland companies |
| PJSC (Public Joint Stock Company) | listed/public |
| PrJSC (Private Joint Stock Company) | |
| Sole Establishment / Civil Company | |
| Branch of a foreign company | |
| Free-zone company / FZE / FZ-LLC / SPV | DIFC/ADGM/DMCC etc. |

## Status values

`Active`, `Expired` (license lapsed), `Cancelled`, `Dissolved`, `Struck off`.

## Internal model mapping

```
company_id          <- Trade license number (emirate) / free-zone reg number / NER number
registration_number <- Trade license number / free-zone registration number
tax_id              <- TRN (FTA, 15-digit)
vat_id              <- TRN (same number; VAT under the TRN)
legal_name          <- Legal / trade name
company_type        <- Legal form (LLC/PJSC/FZE/branch)
status              <- Active/Expired/Cancelled/Dissolved
registered_address  <- Registered address / emirate
activity_code       <- DED activities (ISIC-based)
financials          <- DFM/ADX (listed, AED); private not open
owners/officers     <- Owners/partners/managers (PERSONAL DATA — redact)
country             <- "United Arab Emirates"
```

## Encoding / formats

- UTF-8; Arabic + English. Currency **AED**. Dates dd/mm/yyyy.
- No open register; everything is login/WAF/rate-limited; listed financials browser-
  only (DFM/ADX).
