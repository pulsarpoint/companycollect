# Company Data Analysis For Spain

## Summary

Spain supports a **strong open identity/events profile** and a **split financial profile**. Company
identity and corporate events are **fully open**: the **BORME** (official commercial-register gazette) is
free via the **BOE open-data API**, and **OpenMercantil** republishes a **CC-BY** company master derived
from it (~2.8M companies). The catch is the **CIF (tax id) is sparse in open data (~18%)**, so the
reliable open join key is the **Hoja registral** (province + number). **Financials are split**: **listed
issuers** are fully open via **CNMV** (XBRL), but the **general population**'s annual accounts, while
**XBRL**, are **paid per company (~€9–20)** at the Registro Mercantil — no open bulk.

## Sources Analyzed

| Slug | Source name | Status | Access | License | Role in profile |
|---|---|---|---|---|---|
| borme_boe_api | BORME via BOE open-data API | recommended | public, no auth | BOE open | **Authoritative event spine** (acts, capital, officers) |
| openmercantil | OpenMercantil (BORME-derived) | recommended | public | CC BY 4.0 | **Open company master** (fast bulk) |
| cnmv_financials | CNMV listed-company reports | recommended | public | open | **Financials — listed** (open XBRL) |
| registro_mercantil_cuentas_anuales | Depósito de Cuentas Anuales | blocked_payment | paid ~€9–20/co. | per-doc fee | **Financials — general** (XBRL, planning-only) |

Excluded / not given their own catalog (in `source_inventory.json`): Registro Mercantil/CORPME
(authoritative but paid; overlaps BORME fields), datos.gob.es (discovery portal), INE DIRCE (aggregate
only, not per-company), Registro Central de Titularidades Reales (beneficial ownership, restricted),
commercial aggregators (eInforma/Axesor — alternative paid transport for the same financials).

## What Each Source Contributes

- **borme_boe_api (event spine).** Official daily acts: incorporations (Constitución — capital, domicilio,
  objeto social, administradores), appointments/cessations, capital changes, sole-shareholder
  declarations, dissolutions. Data is **semi-structured Spanish prose** needing a parser (bormeparser);
  the **Hoja registral** in "Datos registrales" is the stable per-company key. CIF usually absent.
- **openmercantil (open master).** CC-BY reconstruction: ~2.8M companies, ~5.8M acts. Company-master rows
  (`slug,name,cif,province,first_seen,last_seen,acts_count`) + a 12-column act-level full CSV (Capital,
  Workers, Website, Address…). The fast way to a broad master, but **excludes financial statements** and
  has **sparse CIF (~18%)**.
- **cnmv_financials (open financials, listed).** IFA (annual) + IFI (intermediate) reports as **open
  XBRL+PDF** for entities with securities admitted to trading. Small population, fully open and
  standardized; join via NIF/CIF.
- **registro_mercantil_cuentas_anuales (general financials).** Annual accounts in **XBRL** (PGC taxonomy)
  + PDF, individual + consolidated, retained 6 years. The financial source for the ~3.3M general
  population — but **paid per company, no bulk, no free API**.

## Proposed Country Company Profile

`country_company_profile.schema.json` (+ `.example.json`, built from a real CC-BY OpenMercantil row,
ASESORIA POLO MARIVELA SL / CIF B88314463) models a Spain-specific object with sections: `registration`
(CIF + Hoja registral + tomo/folio + slug + province), `legal_identity`, `status`,
`registered_location`, `share_capital` (BORME register capital), `officers[]` (PII), `ownership`
(sole-shareholder only), `financial_statements[]` (multi-source, size-class-aware nullability), `acts[]`
(raw BORME events), and `source_provenance[]`. Repeatable concepts (officers, acts, yearly financials)
are arrays; every section carries `x-source`.

## Join And Precedence Rules

- **Identity key**: `cif` when present, else **`hoja_registral`** (province+number), else `name+province`.
- **Identity ↔ financials**: by **NIF/CIF**; low open CIF coverage (~18%) means a matching/enrichment step
  is required first — the main engineering risk.
- **Authority**: BORME (BOE) authoritative; OpenMercantil is a CC-BY convenience reconstruction. Prefer
  BORME for authoritative fields, OpenMercantil for fast coverage.
- **Financial precedence**: CNMV (open) when listed; otherwise Registro Mercantil (paid) or an aggregator.
  Prefer individual accounts for own figures, keep consolidado as the group view; always store currency.
- **Freshness**: identity/events daily; financials annual.

## Missing Or Restricted Data

- **Open financials for the general population**: none — paid per-company (Registro Mercantil) or aggregator.
- **activity / CNAE**: not a clean open field (only inside Constitución objeto social text; DIRCE aggregate).
- **Full ownership / beneficial ownership**: only the sole-shareholder case is open; Titularidades Reales
  is restricted.
- **CIF/tax_id**: sparse in open data (~18%) — enrichment needed to join financials.
- **PII**: officers / sole shareholders are named in BORME — GDPR lawful basis + retention required.
- **License**: BORME/BOE reusable and OpenMercantil CC-BY — confirm attribution before redistribution.

## Common Mapper Notes

See `common_field_mapping_suggestions.md`. Key cross-country points: Spain has **no single always-present
company number** (use CIF, else province-scoped Hoja registral), **sparse open tax_id**, **split
financials** (open-listed vs paid-general; tolerate empty `financial_statements[]` and null `revenue` for
micro filers), **no open activity code**, and ownership limited to the sole-shareholder case. Store
currency per figure (EUR for Spain) rather than hardcoding.
