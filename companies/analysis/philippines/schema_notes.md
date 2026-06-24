# Schema notes — Philippines

## Identifiers

| Field | Description |
|---|---|
| **SEC Registration Number** | Corporate registration id (e.g. `CSNNNNNNNN`, older `ANNNNNNNN`). Company id for corporations/partnerships. |
| **TIN (Tax Identification Number)** | BIR tax id — 9-digit base + 3/5-digit branch code. |
| **DTI BN number** | Business name registration for **sole proprietorships** (DTI BNRS). |
| **VAT** | VAT-registered businesses use the **TIN** — no separate VAT number. |

`SEC Registration Number` is the corporate company id; **TIN** links tax.

## SEC GIS / Company record (field model, from public product descriptions — paid)

| Field | Notes |
|---|---|
| Company name | Inc. / Corp. / OPC / Partnership |
| SEC Registration Number | |
| Company type | Stock / Non-stock / OPC / Partnership / Branch of foreign corp |
| Status | Active / Revoked / Dissolved / Suspended |
| Incorporation date | |
| Registered (principal) office address | |
| Primary purpose | line of business |
| Authorised / subscribed / paid-up capital | PHP |
| Directors / Trustees | **PERSONAL DATA — redact** |
| Officers | **PERSONAL DATA — redact** |
| Stockholders | **PERSONAL DATA — redact** |

## AFS (Audited Financial Statements, paid)

Balance sheet (assets, liabilities, equity) + income statement (revenue, net income),
PHP, filed annually via eFAST; obtained via SEC Express (paid).

## PSE EDGE (listed, open)

`company_name`, `stock_symbol`, `sector`, `subsector`, `listing_date`, per-company
disclosures + financial reports (PHP). Listed companies only.

## Company types

| Type | Notes |
|---|---|
| Stock corporation (Inc. / Corp.) | most companies |
| One Person Corporation (OPC) | single stockholder (since 2019) |
| Non-stock corporation | non-profit |
| Partnership | general / limited |
| Branch / Representative office (foreign) | |
| Sole proprietorship | DTI BNRS (not SEC) |

## Status values

`Active`, `Revoked`, `Suspended`, `Dissolved`.

## Internal model mapping

```
company_id          <- SEC Registration Number
registration_number <- SEC Registration Number (DTI BN for sole props)
tax_id              <- TIN (BIR)
vat_id              <- none separate (TIN-based)
legal_name          <- Company name
company_type        <- Stock/Non-stock/OPC/Partnership
status              <- Active/Revoked/Suspended/Dissolved
incorporation_date  <- Incorporation date
registered_address  <- Principal office address
activity_code       <- Primary purpose (PSIC where coded)
capital             <- Authorised/paid-up capital (PHP)
financials          <- AFS (SEC eFAST, paid) / PSE EDGE (listed), PHP
owners/officers     <- Directors/Officers/Stockholders (GIS — PERSONAL DATA, redact)
country             <- "Philippines"
```

## Encoding / formats

- UTF-8; English. Currency **PHP**. Dates `Mon dd, yyyy` (PSE) / dd-mm-yyyy.
- SEC documents are **paid** (SEC Express); only PSE EDGE (listed) is open.
