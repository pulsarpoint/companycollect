# Schema notes — Egypt

## Identifiers

| Field | Description |
|---|---|
| **Commercial Registry number (رقم السجل التجاري)** | Commercial registration id (GOEIC / Commercial Registry). Company id. |
| **Tax ID (الرقم الضريبي)** | Egyptian Tax Authority (ETA) tax number — 9-digit. |
| **Unified company number** | Links the commercial registry and tax records. |
| **EGX symbol / ISIN** | Listed-company stock symbol (e.g. COMI) / ISIN (`EG…`). |

`Commercial Registry number` is the company id; **Tax ID** links tax.

## EGX listed-company record (browser-public, WAF-gated) — fields

| Field | Meaning | Notes |
|---|---|---|
| company_name | Listed company name | e.g. Commercial International Bank (Egypt) S.A.E. |
| egx_symbol | Stock symbol | COMI / TMGH / SWDY |
| isin | ISIN | EG… |
| sector | EGX sector | Banks / Real Estate / Industrial / etc. |
| disclosures | Disclosures | financial statements, board reports |
| financial_statements | Financials | EGP |

## GAFI / Commercial Registry company record (field model, gated — public knowledge)

| Field | Notes |
|---|---|
| Company name | S.A.E. (joint-stock) / Ltd / branch |
| Commercial Registry number | |
| Tax ID | |
| Company type | Joint-stock (S.A.E.) / LLC / sole person company / branch |
| Status | Active / under liquidation / struck off |
| Authorised / paid-up capital | EGP |
| Activity / purpose | |
| Directors / board | **PERSONAL DATA — redact** |
| Shareholders / partners | **PERSONAL DATA — redact** |

## Company types

| Type | Notes |
|---|---|
| S.A.E. (شركة مساهمة مصرية) | Joint-stock company |
| LLC (ش.ذ.م.م) | Limited liability company |
| One-person company | single shareholder |
| Branch of a foreign company | |
| Sole proprietorship | commercial registry only |

## Status values

`Active`, `Under liquidation`, `Struck off / cancelled`.

## Internal model mapping

```
company_id          <- Commercial Registry number
registration_number <- Commercial Registry number
tax_id              <- Tax ID (الرقم الضريبي)
vat_id              <- VAT under the Tax ID (no separate number)
legal_name          <- Company name
company_type        <- S.A.E. / LLC / one-person / branch
status              <- Active/Under liquidation/Struck off
registered_address  <- Registered address
activity_code       <- Activity / purpose (ISIC-like)
capital             <- Authorised / paid-up capital (EGP)
financials          <- EGX (listed, WAF-gated) / GAFI filings (gated), EGP
owners/officers     <- Directors/Shareholders (PERSONAL DATA — redact)
country             <- "Egypt"
```

## Encoding / formats

- UTF-8; Arabic + English. Currency **EGP**. Dates dd/mm/yyyy.
- No open register; EGX is WAF-gated; GAFI/Commercial Registry are login/restricted.
