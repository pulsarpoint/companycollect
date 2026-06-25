# Schema notes — Saudi Arabia

## Identifiers

| Field | Description |
|---|---|
| **CR number (رقم السجل التجاري)** | 10-digit Commercial Registration number with a **region prefix** (1010 = Riyadh, 2050/2051 = Eastern Province, 4030 = Jeddah, 5950 = Madinah, …). Company id. |
| **Unified National Number / "700 number" (الرقم الموحد)** | A `700…` unified company id linking the CR with government agencies. |
| **VAT number** | 15-digit (ZATCA), starts and ends with `3`. |

`CR number` is the company id; the **Unified National Number (700)** is the
cross-agency key; the **VAT number** links tax.

## MoC Commercial Register record (field model, Nafath-gated — public knowledge)

| Field (en) | Notes |
|---|---|
| Company name | Arabic + English |
| CR number | 10-digit, region-prefixed |
| Unified National Number (700) | unified company id |
| Company type | JSC / LLC / Sole Proprietorship / Branch / Foreign branch |
| Status | Active / Expired / Cancelled / Suspended |
| Issue date / Expiry date | Hijri (and Gregorian); CRs are renewed |
| Capital | SAR |
| Activities | ISIC-based activity codes |
| Head office / Branches | |
| Managers | **PERSONAL DATA — redact** |
| Owners / Partners | **PERSONAL DATA — redact** |

## Tadawul (listed) record

`company_name`, `symbol` (4-digit, e.g. 2222), `isin` (SA…), `sector`,
`financial_statements` (SAR), `disclosures`. Listed companies only.

## Company types

| Type | Notes |
|---|---|
| JSC (Joint Stock Company / شركة مساهمة) | listed/public + closed |
| LLC (شركة ذات مسؤولية محدودة) | most companies |
| Simplified Joint Stock Company (SJSC) | newer form |
| Sole Proprietorship (مؤسسة فردية) | |
| Branch of a foreign company | |

## Status values

`Active`, `Expired` (CR lapsed), `Cancelled`, `Suspended`.

## Internal model mapping

```
company_id          <- CR number (10-digit) [or Unified National Number 700]
registration_number <- CR number
tax_id              <- VAT number (ZATCA) / Unified Number for tax
vat_id              <- VAT number (15-digit, ZATCA)
legal_name          <- Company name
company_type        <- JSC/LLC/SJSC/Sole Proprietorship/Branch
status              <- Active/Expired/Cancelled/Suspended
registered_address  <- Head office
activity_code       <- Activities (ISIC)
capital             <- Capital (SAR)
financials          <- Tadawul (listed, SAR); private not open
owners/officers     <- Managers/Owners/Partners (PERSONAL DATA — redact)
country             <- "Saudi Arabia"
```

## Encoding / formats

- UTF-8; Arabic + English. Currency **SAR**. Dates **Hijri** (and Gregorian).
- No open register; CR inquiry Nafath-gated; Tadawul WAF-gated; open data firewalled.
