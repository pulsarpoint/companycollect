# Company Data Analysis For Pakistan

## Summary

Pakistan has an **excellent open API for listed companies** but a **firewalled/gated**
authoritative registrar. The open layer is the **Pakistan Stock Exchange data portal**
(`dps.psx.com.pk/symbols`, JSON) — **verified 1,068 symbols (744 equities)** with
symbol/name/sector, plus per-company HTML pages (registered address, free float, shares).
The authoritative full registrar is **SECP eServices** (keyed on the **CUIN**), but the SECP
site returned **HTTP 403 (WAF)** and the eServices portal **timed out** — firewalled from
this environment (planning-only). The **FBR Active Taxpayers List** (keyed on the **NTN**) is
**per-NTN online verification** with no open bulk file located. A rich **listed-company**
layer can be built openly; the full company population requires SECP from an unblocked
network. Nothing was bypassed and no identifiers were fabricated.

## Sources Analyzed

| Source slug | Source name | Status | Access | License | Role |
|---|---|---|---|---|---|
| psx_dataportal | PSX Data Portal | ready | open JSON API + HTML | PSX terms (unconfirmed) | Listed companies: symbol, name, sector, address, free float |
| secp_eservices | SECP eServices | blocked_authentication | WAF/firewalled; login | restricted | Authoritative registrar: CUIN, kind, status, directors |
| fbr_atl | FBR Active Taxpayers List | insufficient_transport_info | per-NTN verification | restricted | Tax status: NTN, ATL status (no open bulk) |

(`opendata_com_pk` is a third-party portal with no authoritative register — not modeled.)

## What Each Source Contributes

- **PSX data portal** — the open, free layer: ticker `symbol`, company `name`, PSX
  `sectorName`, and `isETF`/`isDebt` flags (filter for equities); per-company pages add
  registered address, free float, and shares (PKR). Listed companies only.
- **SECP eServices** — the authoritative registrar: **CUIN**, company name, company kind
  (private/public limited, SMC, LLP), registration status, incorporation date, registered
  office, directors. Firewalled here; planning-only; directors are personal data — redact.
- **FBR ATL** — **NTN**, registration number (bridge to SECP), name, ATL (tax-filing) status,
  and category (company/AOP/individual). Per-NTN verification; covers individuals too
  (personal data); planning-only.

## Proposed Country Company Profile

A name-anchored object (CUIN canonical once obtained) with sections: `registration` (cuin,
ntn, psx_symbol), `legal_identity` (name, company_kind), `status` (SECP registration_status +
FBR atl_status + incorporation_date), `activity` (PSX sector), `registered_location`,
`officers` (SECP, redacted), `listing` (PSX), and `financial_statements` (PSX free float/
shares, PKR), each with `source_provenance`. The example is anchored on a **real PSX-listed
company** (Habib Bank Limited, HBL, COMMERCIAL BANKS) with CUIN/NTN/officers null/redacted
(those need SECP/FBR).

## Join And Precedence Rules

- **Three identifiers**: CUIN (SECP), NTN (FBR), PSX symbol (listed). **Join** PSX ⟷ SECP by
  **name** (PSX has no CUIN); FBR's **Registration No.** can bridge NTN ⟷ SECP.
- **Precedence**: SECP authoritative for identity/status/officers (firewalled); PSX open for
  listed identity/sector/financials; FBR for tax status.
- **Keep two statuses distinct**: SECP `registration_status` vs FBR `atl_status` (tax-filing).
- **Language** English; **currency** PKR.

## Missing Or Restricted Data

- **SECP is WAF-blocked/firewalled** here → CUIN, kind, registration status, incorporation
  date, officers are **planning-only** (use SECP from an unblocked network).
- **FBR ATL** has **no open bulk file** (per-NTN verification) and mixes **individuals**
  (personal data — redact).
- **Directors** (SECP) and **shareholders/owners** are not openly available (login/filings);
  **VAT/STRN** is not openly published.
- Open coverage is **listed companies only** (PSX).

## Common Mapper Notes

`company_id`/`registration_number` → CUIN (SECP, firewalled); `tax_id` → NTN (FBR,
verification); `legal_name`/`activity_code`/`financials` → PSX (open, listed). `owners`,
`vat_id`, `dissolution_date` are `not_available_in_open_sources`. Only PSX is `ready`; SECP
is `blocked_authentication`; FBR ATL is `insufficient_transport_info`.
