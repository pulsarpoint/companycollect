# Company Data Analysis For Kenya

## Summary

Kenya's official register is the **BRS** (Business Registration Service), keyed on the
**company registration number** (old `C.`/`CPR` formats; new eCitizen `PVT-XXXXXXX`;
**BN** for business names), but it is **not openly accessible**: company **search** and
**documents** (**CR12** directors/shareholders, status report, annual returns) are
delivered through the **eCitizen** platform — **login-gated and paid per transaction**.
The **KRA PIN** is the tax id; VAT is registered **under the PIN** (no separate VAT
number).

The one genuinely **open** source is the **NSE (Nairobi Securities Exchange)
listed-company directory** — verified live (Absa Bank Kenya PLC, Stanbic Holdings Plc,
Standard Chartered Bank, Sasini Ltd…) — plus listed announcements/financial results.
**opendata.go.ke** has no accessible company dataset. So there is **no open bulk
register and no open private financials** — ingestion is `blocked_payment` (BRS) +
open-for-listed (NSE). Currency **KES**; CR12 directors/shareholders are personal data
(Data Protection Act 2019). No BRS per-company values were captured.

## Sources Analyzed

| Source slug | Name | Status | Access | License | Role |
|---|---|---|---|---|---|
| nse_listed | NSE — listed companies + financials | ready | **open** | public disclosure | Listed identity + financials |
| brs_ecitizen | BRS — register + documents (eCitizen) | blocked_payment | login + paid | paid | Corporate identity + financials |

(opendata.go.ke is recorded in discovery as unavailable — no company dataset.)

## What Each Source Contributes

- **nse_listed** — open listed-company directory (name, ticker, sector/segment) +
  announcements/financial results (KES). Verified live (Absa Bank Kenya PLC, etc.).
- **brs_ecitizen** — the canonical corporate record (registration number, type, status,
  registration date, address, nominal capital, directors, shareholders, KRA PIN,
  annual returns), via eCitizen, paid. Field model from public knowledge.

## Proposed Country Company Profile

`country_company_profile.schema.json` keys on **registration_number** with sections:
`tax_identifiers` (kra_pin; VAT under PIN), `legal_identity`, `status`, `activity`
(NSE sector), `registered_location`, `capital` (KES, paid), `owners`/`officers`
(redacted, paid), `listing` (NSE, open), `financial_statements[]` (NSE listed / BRS
paid), and `source_provenance[]`. The example uses the NSE-verified **Absa Bank Kenya
PLC** with BRS identifiers null.

## Join And Precedence Rules

- **Registration number** is the corporate key; **KRA PIN** links tax; **NSE ticker**
  keys the listed entity (join to BRS by name).
- **BRS** authoritative for corporate identity + financials (paid); **NSE** for listed
  (open).

## Missing Or Restricted Data

- **No open bulk corporate register; no open private financials** — BRS paid/gated;
  only NSE (listed) is open.
- **No company dataset on opendata.go.ke**.
- **No separate VAT number** (VAT under the KRA PIN).
- **Directors/shareholders (CR12)** redacted as personal data (Data Protection Act
  2019).

## Common Mapper Notes

`company_id == registration_number`; `tax_id == KRA PIN`; no separate `vat_id`. The
blocker is **eCitizen-gated, paid BRS**; the open path is **NSE** (listed). Currency
**KES**. See `common_field_mapping_suggestions.md`.
