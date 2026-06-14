# Luxembourg — Search Attempts Log

## Attempt 1
- Date/time: 2026-06-14
- Search engine or source: WebSearch
- Query: `Luxembourg RCS Registre de Commerce et des Sociétés LBR open data data.public.lu company register download comptes annuels financial statements API`
- Language: English + French
- Why this query was tried: Identify the register, any open bulk/API, and financials access.
- Top relevant URLs:
  - https://www.lbr.lu/
  - https://guichet.public.lu/.../immatriculation-entreprise-publication-rcs.html
  - https://data.public.lu/
- Result: RCS = authoritative, run by LBR; free basic search; filed documents (statutes, annual accounts) free to download; certified extracts paid. RESA = legal gazette. Open bulk/API uncertain.
- Decision: Check data.public.lu for an open RCS dataset; probe LBR/RCS.

## Attempt 2
- Date/time: 2026-06-14
- Source: curl (live) — data.public.lu uData API
- Query: /api/1/datasets/?q=RCS / registre+commerce / LBR / entreprises / bilan / société / TVA
- Result: RCS/registre/LBR/société/TVA = 0 datasets. entreprises = 71, all STATEC statistical aggregates (demography, structural business stats, creations/cessations). No company-register or financials bulk.
- Decision: No open RCS register/financials on the portal; statistical only.

## Attempt 3
- Date/time: 2026-06-14
- Source: curl (live) — LBR / RCS / RESA + provenance
- Query: lbr.lu; RCS RechercheController; RESA; OpenSanctions lu_rcsl; OpenCorporates 115
- Result: lbr.lu + RCS search + RESA all HTTP 200. RCS search page = State login/search, references captcha + login. OpenSanctions lu_rcsl 404 (no mirror); OpenCorporates has LU register.
- Decision: RCS = free manual search + free document download, captcha-gated, no open bulk/API. RESA = useful secondary (gazette).

## Attempt 4
- Date/time: 2026-06-14
- Source: WebSearch + documentation
- Query: comptes annuels / eCDF / RBE / VAT
- Result: Annual accounts filed via eCDF, free to download per company as PDF; no open bulk. RBE (beneficial ownership) restricted post-CJEU. VAT = LU + 8 digits (VIES/AED).
- Decision: rcs_annual_accounts = useful_secondary (free PDF, no open bulk); rbe_register = blocked_by_authentication; vies_vat = useful_secondary; commercial aggregators = realistic bulk/financials path. Built a schematic normalized sample (no per-company open record lawfully downloadable in bulk).
