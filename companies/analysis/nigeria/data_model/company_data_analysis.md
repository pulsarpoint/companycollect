# Company Data Analysis For Nigeria

## Summary

Nigeria's official register is the **CAC** (Corporate Affairs Commission), keyed on
the **RC number** (companies; **BN** for business names, **IT** for incorporated
trustees), but it is **not openly accessible**: the public search
(`search.cac.gov.ng`) is **Cloudflare-gated** and company documents (status report,
annual returns, **AFS**) are **paid**. The **TIN** (FIRS) is the tax id; **VAT** is a
separate FIRS registration. The **CAC Beneficial Ownership Register (PSC)** is public
via the browser but **token-gated** for automation (and exposed a misconfigured
PII-leaking endpoint, which was **not used**).

The one genuinely **open** source is the **NGX (Nigerian Exchange) equities API** for
**listed companies** — verified live (DANGCEM, MTNN, GTCO, ZENITHBANK, SEPLAT…) with
symbol/sector/market/prices, plus listed financial statements. So there is **no open
bulk corporate register and no open private financials** — ingestion is
`blocked_payment` / Cloudflare (CAC) + open-for-listed (NGX). Currency **NGN**;
directors/shareholders/PSC are personal data (NDPA 2023). No CAC per-company values
were captured.

## Sources Analyzed

| Source slug | Name | Status | Access | License | Role |
|---|---|---|---|---|---|
| ngx_equities | NGX — listed equities + financials | ready | **open** JSON | public disclosure | Listed identity + financials |
| cac_registry | CAC — register + documents | blocked_payment | Cloudflare-gated; paid | restricted | Corporate identity + financials |
| cac_bor_psc | CAC Beneficial Ownership Register | blocked_authentication | browser; token-gated | public/token | Beneficial owners (PSC) |

(data.gov.ng is recorded in discovery as unavailable.)

## What Each Source Contributes

- **ngx_equities** — open listed-company directory (symbol, sector, market board,
  prices, volume) + issuer financial statements (NGN). Verified live (146 equities).
- **cac_registry** — the canonical corporate record (RC number, type, status,
  registration date, address, share capital, directors, shareholders, AFS), paid.
  Field model from public knowledge.
- **cac_bor_psc** — beneficial owners / persons with significant control (token-gated;
  personal data — redact). A PII-leaking misconfiguration was flagged and avoided.

## Proposed Country Company Profile

`country_company_profile.schema.json` keys on **rc_number** with sections:
`tax_identifiers` (tin/vat), `legal_identity`, `status`, `activity` (CAC nature /
NGX sector), `registered_location`, `capital` (NGN, paid), `owners`/`officers`
(redacted, paid/token), `listing` (NGX, open), `financial_statements[]` (NGX listed /
CAC AFS paid), and `source_provenance[]`. The example uses the NGX-verified **Dangote
Cement Plc** (DANGCEM) with CAC identifiers null.

## Join And Precedence Rules

- **RC number** is the corporate key; **TIN** links tax; **NGX symbol** keys the
  listed entity (join to CAC by name).
- **CAC** authoritative for corporate identity + financials (gated/paid); **NGX** for
  listed (open); **BO register** for PSC (token-gated).

## Missing Or Restricted Data

- **No open bulk corporate register; no open private financials** — CAC gated/paid;
  only NGX (listed) is open.
- **CAC BO register** token-gated; PII-leaking endpoint avoided.
- **data.gov.ng** unreachable.
- **Directors/shareholders/PSC** redacted as personal data (NDPA 2023).

## Common Mapper Notes

`company_id == RC number` (companies; BN/IT for other types); `tax_id == TIN`;
`vat_id` separate. The blocker is **gated/paid CAC**; the open path is **NGX**
(listed). Currency **NGN**. See `common_field_mapping_suggestions.md`.
