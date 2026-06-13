# Spain — License & Terms Notes

> Public ≠ freely reusable. Confirm before redistribution or commercial use.

## BORME / BOE (open data)
- Published by the **Agencia Estatal Boletín Oficial del Estado**. The BOE operates an official
  **open-data (datos abiertos)** programme explicitly to enable **download, use and reuse** of its
  content, including BORME, via its REST API.
- Treat as **reusable open data**, but **confirm the exact attribution/reuse conditions** on the BOE
  datos-abiertos terms page before redistribution (Spanish public-sector reuse — Ley 37/2007 / Real
  Decreto 1495/2011 framework; generally permissive with attribution and no misrepresentation).

## OpenMercantil
- Republished BORME-derived data under **Creative Commons Attribution 4.0 (CC BY 4.0)** — **commercial
  use permitted with attribution**. Attribute OpenMercantil and cite the original source (BORME/BOE).
- It is a **community/NGO reconstruction**, not an official register — verify critical fields against
  BORME/Registro Mercantil for authoritative use.

## Registro Mercantil / CORPME (registradores.org) — incl. cuentas anuales
- The authoritative register and the **annual-accounts deposits** are accessed via **per-document fees**
  (cuentas anuales ~€8.99–€20 per company). No bulk redistribution rights implied; documents are
  individually licensed/paid. **No open API.**
- Do **not** assume the right to redistribute purchased documents or scraped register content.

## CNMV (listed financials)
- CNMV is a public authority publishing **Información Pública Periódica** (IFA/IFI) as open XBRL+PDF for
  reuse. Generally reusable with attribution; confirm CNMV's reuse note for redistribution at scale.

## datos.gob.es
- License is **per dataset** (frequently open/reusable, often with attribution). Check each dataset's
  license field individually before reuse.

## INE — DIRCE
- INE content is reusable under INE's standard conditions (attribution). DIRCE is **aggregate** anyway,
  so no per-company personal/commercial data concerns.

## Registro Central de Titularidades Reales (beneficial ownership)
- **Restricted.** Post-2022 CJEU ruling, general public bulk access was curtailed; access is fee-based /
  legitimate-interest. **Not** open data; do not ingest as open.

## Commercial aggregators (eInforma/Informa D&B, Axesor, Iberinform)
- Proprietary, paid, per-vendor contract. Redistribution typically prohibited without a license.

## Personal data / GDPR
- BORME acts name **administrators / sole shareholders / liquidators** (natural persons). This is
  personal data published in an official gazette — apply a **GDPR lawful basis and retention policy**
  before persisting beyond the raw zone.

## Summary recommendation
- Safe to **ingest and use** BORME (BOE) and OpenMercantil (CC-BY) now, with attribution; verify exact
  BOE reuse terms before large-scale redistribution.
- Financials: **CNMV open**; general-population accounts are **paid per-document** — no open bulk.
- Treat beneficial ownership as **restricted**. Handle officer personal data under GDPR.
