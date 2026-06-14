# Company data sources for Portugal

## Status

- Official bulk data: **not found** (no open per-company register/financials bulk)
- Official API: **not found** (register is paid; publicacoes.mj.pt search is reCAPTCHA-protected)
- Open data portal: **found but statistical only** (dados.justica.gov.pt / dados.gov.pt = aggregate counts, not a register)
- License: **mixed** (open datasets CC-BY-SA / CC-BY; register reuse terms unclear)
- Recommended ingestion path: **manual review / per-entity lookup**; a commercial provider for identified bulk + structured financials

## Best source

The authoritative register is the **Registo Comercial** (IRN), keyed on the **NIPC** (9-digit collective-entity
number, which is also the company's NIF/tax number; VAT = `PT` + NIPC). Per-company data (name, sede, CAE, legal
form, capital, **sócios**, **gerência**) is accessed via the **paid certidão permanente** (~€25/year) — **no open
bulk/API**.

- **publicacoes.mj.pt** publishes company **acts** (incorporations, statute/capital/management changes,
  dissolutions) **for free**, but the search is **reCAPTCHA-protected** → manual lookups only, no automation.
- **IES** (Informação Empresarial Simplificada) financial statements (balance sheet + income statement) are filed
  to AT/INE/Banco de Portugal and are **not openly published** per company (Banco de Portugal publishes only
  aggregate sector statistics).
- **dados.justica.gov.pt** publishes only **statistical aggregates** (counts of incorporations/extinctions,
  registrations, insolvencies) — verified, not a per-company register.

So Portugal is a **partial-open / paid + automation-blocked** country: free manual company-acts lookups, but no
open bulk/automation and the register + financials are paid/restricted.

## Next action

For automation/bulk + structured financials, use a **commercial provider** (Racius, Informa D&B/einforma,
Iberinform) or the **paid certidão permanente**; do **manual** publicacoes.mj.pt / Racius lookups for free basic
info. Validate VAT (`PT` + NIPC) via VIES. Confirm reuse terms; treat officer/shareholder/UBO data under GDPR
(RCBE restricted).
