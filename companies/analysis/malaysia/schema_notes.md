# Schema notes — Malaysia

## Identifiers

| Field | Description |
|---|---|
| **Company registration number (new)** | **12-digit** format since 2019 (e.g. 201901000005). SSM-issued. Company id. |
| **Company registration number (old)** | Legacy format NNNNNNN-A (e.g. 1234567-A). Still widely referenced. |
| **ROB number** | Business registration number for sole proprietorships / partnerships. |
| **TIN (Nombor Pengenalan Cukai)** | Income tax number (LHDN/HASIL); companies prefixed `C`. |
| **SST registration number** | Sales & Service Tax registration (no VAT/GST since 2018). |

`Company registration number` is the SSM company id; **TIN** links to tax.

## SSM Company Profile record (field model, from published sample templates)

| Field | Notes |
|---|---|
| Company name | Sdn. Bhd. / Bhd. / PLT |
| Registration number (new + old) | SSM |
| Incorporation date | |
| Company type / status | private/public; existing/struck-off/wound-up |
| Registered office address | |
| Business address | |
| Nature of business (MSIC) | Malaysia Standard Industrial Classification |
| Authorised / paid-up capital | MYR |
| Directors | **PERSONAL DATA — redact** |
| Shareholders | **PERSONAL DATA — redact** |
| Charges | secured charges/debentures |
| Financial Comparison / Historical | revenue, profit, assets (paid) |

## Financial products (paid)

**Financial Comparison** (2/3/5/10 years) and **Financial Historical** — annual
financial statement figures (revenue, profit/loss, assets, equity), MYR. Sold per
company.

## Legal forms

| Local | English |
|---|---|
| Sdn. Bhd. (Sendirian Berhad) | Private limited company |
| Bhd. (Berhad) | Public limited company |
| PLT (Perkongsian Liabiliti Terhad) | Limited liability partnership (LLP) |
| Enterprise / Trading (Perniagaan) | Sole proprietorship / partnership (ROB) |

## Status values

`Existing` (active), `Dissolved`, `Wound up`, `Struck off`.

## Internal model mapping

```
company_id          <- Company registration number (new 12-digit)
registration_number <- Company registration number (new + old)
tax_id              <- TIN (LHDN)
vat_id              <- none (SST number; no VAT/GST since 2018)
legal_name          <- Company name
company_type        <- Sdn. Bhd. / Bhd. / PLT
status              <- Existing/Dissolved/Wound up/Struck off
incorporation_date  <- Incorporation date
registered_address  <- Registered office address
activity_code       <- Nature of business (MSIC)
capital             <- Authorised / paid-up capital (MYR)
financials          <- SSM Financial Comparison/Historical (paid, MYR) / Bursa (listed)
owners/officers     <- Directors / Shareholders (PERSONAL DATA — redact)
country             <- "Malaysia"
```

## Encoding / formats

- UTF-8; Malay + English. Currency **MYR**. Dates dd-mm-yyyy.
- SSM data is **paid** (e-Info / MyData-SSM); only e-Search (existence) is free.
