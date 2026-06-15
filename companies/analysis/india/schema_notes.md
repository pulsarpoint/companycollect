# India — Schema Notes

## Identifiers

- **CIN (Corporate Identification Number)** — 21 characters, structured:
  `L20101NL1985PLC002284`
  - position 1: **listing status** — `L` listed / `U` unlisted
  - positions 2–6: **industry code** (5-digit, MCA/NIC-derived)
  - positions 7–8: **state code** (e.g. `NL` Nagaland, `MZ` Mizoram, `MH`
    Maharashtra, `DL` Delhi)
  - positions 9–12: **year of incorporation**
  - positions 13–15: **ownership/type** — `PLC` public ltd, `PTC` private ltd,
    `GAP`/`GOI` government, `NPL` sec-8/not-for-profit, `FTC` foreign, `OPC`
    one-person, `ULL`/`ULT` unlimited, etc.
  - positions 16–21: **6-digit RoC registration sequence**
- The CIN is the universal **join key** across MCA, exchanges, and aggregators.
- **Not in the open master data**: **PAN** (10-char income-tax id, the corporate
  tax id) and **GSTIN** (15-char = state code + PAN + entity/check digit, the GST
  registration). India has GST, not VAT; there is no VAT number.
- **LLPs** use an **LLPIN** (not a CIN) and a separate master dataset.

## MCA Company Master Data fields (data.gov.in / OGD API)

### Stable across snapshots

| Field | Meaning |
|---|---|
| CORPORATE_IDENTIFICATION_NUMBER | CIN (key) |
| COMPANY_NAME | Registered company name |
| COMPANY_STATUS | ACTIVE / STRIKE OFF / AMALGAMATED / UNDER LIQUIDATION / DORMANT / etc. |
| COMPANY_CLASS | Public / Private |
| COMPANY_CATEGORY | Company Limited by Shares / by Guarantee / Unlimited |
| (SUB_)CATEGORY / company_sub_category | Indian Non-Government / Government / Subsidiary of Foreign |
| AUTHORIZED_CAPITAL / authorized_cap | Authorized share capital (₹ INR) |
| PAIDUP_CAPITAL | Paid-up share capital (₹ INR) |
| DATE_OF_REGISTRATION | Incorporation date (`D-M-YYYY` in 2015 files; ISO datetime in 2021) |
| REGISTERED_STATE | State |
| REGISTRAR_OF_COMPANIES | RoC office (e.g. RoC-Shillong, RoC-Mumbai) |
| PRINCIPAL_BUSINESS_ACTIVITY(_AS_PER_CIN) | Business activity description |
| REGISTERED_OFFICE_ADDRESS | Registered office address (free text) |

### 2021 snapshots add

| Field | Meaning |
|---|---|
| industrial_class | 4-digit industrial class code |
| email_addr | Company contact email — **PERSONAL DATA (redact)** |
| latest_year_ar | Latest financial year an **annual return** was filed (marker, not figures) |
| latest_year_bs | Latest financial year a **balance sheet** was filed (marker, not figures) |

> The capital fields and the `latest_year_*` markers are the only financial
> signal in the open data — there are **no P&L / balance-sheet figures**.

## Dates, encoding, money

- **Dates**: 2015 files use `D-M-YYYY` (e.g. `1-4-1985`); 2021 files use ISO
  datetime (`2016-05-25T16:40:09Z`) — normalize to `YYYY-MM-DD`.
- **Money**: integer rupees (INR). No decimals observed.
- **Encoding**: UTF-8 JSON via the OGD API; addresses are free text with
  duplicated state tokens.

## Status normalization

```text
ACTIVE / Active              -> active
STRIKE OFF / Strike Off      -> struck_off
AMALGAMATED                  -> amalgamated
UNDER LIQUIDATION            -> under_liquidation
DISSOLVED                    -> dissolved
DORMANT                      -> dormant
CONVERTED TO LLP             -> converted_to_llp
```

## Internal model mapping

```text
company_id          <- CIN
registration_number <- CIN (RoC sequence = last 6 digits)
tax_id              <- null (PAN not in open data)
vat_id              <- null (India uses GST; GSTIN not in open data)
legal_name          <- COMPANY_NAME
status              <- COMPANY_STATUS (normalized)
legal_form          <- COMPANY_CLASS + COMPANY_CATEGORY (+ CIN type segment)
incorporation_date  <- DATE_OF_REGISTRATION
registered_address  <- REGISTERED_OFFICE_ADDRESS (+ REGISTERED_STATE)
activity_code       <- industry code from CIN (positions 2-6) / industrial_class / principal_business_activity
capital             <- AUTHORIZED_CAPITAL, PAIDUP_CAPITAL (INR)
financials          <- not in open data (MCA AOC-4/XBRL paid; BSE/NSE for listed)
officers            <- not in open data (MCA portal; personal data)
```
