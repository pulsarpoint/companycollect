# RCS — Registre de Commerce et des Sociétés (LBR public search) Field Catalog

> Field model documented from the RCS/LBR. **No `sample_record.json`**: the search is captcha-gated with no open
> bulk/API, so no per-company open record was lawfully downloadable in bulk; no real values copied.

## Source Summary

- Country: Luxembourg
- Source type: official_registry
- Organization: Luxembourg Business Registers (LBR, GIE)
- URL: https://www.lbr.lu/ (search captcha-gated)
- License: public register (free search + free document download; certified extracts paid; reuse terms unclear)
- Access: public (manual); search captcha-gated, no open bulk/API
- Freshness: real-time
- Record shape: company page (HTML; documents PDF)
- Primary keys: `rcs_number`
- Join keys: `rcs_number`, `matricule`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| rcs_number | Numéro RCS | Register id | string | identifier | (not copied) | prefix = entity class (B/A/F) |
| matricule | Matricule | 13-digit national id | string | identifier | (not copied) | cross-source key; tax-side |
| denomination | dénomination | Legal name | string | legal_name | (not copied) | |
| forme_juridique | forme juridique | Legal form | string | legal_form | (not copied) | S.A./S.à r.l./SCSp |
| siege_social | siège social | Registered office | string | address | (not copied) | |
| statut | statut | Status | string | status | (not copied) | inscrite/en liquidation/radiée |
| date_constitution | date de constitution | Incorporation date | date | date | (not copied) | |
| documents[] | documents déposés | Filed documents (PDF) | array | document | (not copied) | **free**; officers + accounts inside |

## Interpretation Notes

- **Authoritative register, manual-only.** The RCS holds company identity (name, legal form, registered office,
  status, incorporation) plus filed **documents** — and basic info **and** the documents (articles, **annual
  accounts**, resolutions) are **free**. But the search UI is **captcha-gated** with **no open bulk/API** —
  automated/bulk access is **blocked and must not be bypassed**; certified extracts are paid.
- **Two identifiers.** RCS number (prefix = entity class: B = sociétés, A = personnes physiques, F =
  succursales) and the **matricule** (13-digit national id, also the tax-side id; cross-source join key).
  **VAT** = `LU` + 8 digits, separate.
- **Officers + financials live in the documents.** Directors/managers and the comptes annuels are inside the
  free filed documents (PDF) — the route to officers and financials for Luxembourg.
- **License.** Reuse/redistribution terms not clearly stated — confirm before redistribution.
