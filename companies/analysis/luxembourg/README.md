# Company data sources for Luxembourg

## Status

- Official bulk data: **not found** (no open RCS bulk export)
- Official API: **not found** (RCS search is web-UI + captcha-gated; no open API)
- Open data portal: **found but not the register** (data.public.lu = STATEC statistical aggregates only)
- License: **unclear** (public register; reuse/redistribution terms not stated)
- Recommended ingestion path: **manual review / per-entity lookup**; a commercial provider for bulk + structured financials

## Best source

The authoritative register is the **RCS** (Registre de Commerce et des Sociétés), run by **Luxembourg Business
Registers (LBR)**, keyed on the **RCS number** (e.g. `B123456`) and the **matricule** (13-digit national id).
It is a **public** register:

- **Free** basic search by name / RCS number / matricule (name, legal form, registered address, status).
- Many filed **documents** — articles of association, board resolutions, and **annual accounts (comptes
  annuels)** — are **free to download** on the company's RCS page (PDF; filed via the structured eCDF format).
- **Certified extracts** are **paid**.

But there is **no open bulk export and no open API**, the search UI is **captcha-gated**, and **data.public.lu**
only carries STATEC **statistical** aggregates (not a company register). So Luxembourg is a **partial-open**
country: rich free per-company access (incl. free financial documents), but no lawful open bulk/automation.

## Next action

For automation/bulk, use a **commercial provider** (Kyckr, Creditreform, …) for the register + structured
financials, or do **manual** RCS lookups (documents free). Validate VAT (`LU`+8 digits) via VIES. Confirm RCS
reuse terms before redistribution; treat officer/beneficial-owner data under GDPR (RBE is restricted).
