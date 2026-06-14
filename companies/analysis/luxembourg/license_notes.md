# Luxembourg — License & Terms Notes

> Luxembourg's RCS is a public register with free search and free per-company document downloads (incl. annual
> accounts), but there is no stated open-data licence, no bulk export, and no open API; certified extracts are
> paid and beneficial ownership is restricted.

## RCS register (LBR)
- The RCS is **public** and free to **search manually**; basic identification info (name, RCS number, legal form,
  registered address, status) is free, and many filed **documents** (articles of association, board resolutions,
  **annual accounts**) are **free to download**. However:
  - **Certified extracts / official copies** carry a **fee**.
  - No open-data **licence** for reuse/redistribution of the register data is stated — **confirm terms** before
    redistribution. Public access ≠ permission to redistribute.
  - The search UI is **captcha-gated**; there is **no open bulk/API**. Automated/bulk scraping is an
    access-control bypass — **do not** do it.

## Annual accounts (comptes annuels / eCDF)
- Filed annual accounts (bilan + compte de profits et pertes + annexes) are **public and free to download** on
  the company's RCS page, usually as **PDF** (filed via the structured eCDF format). No bulk redistribution
  rights implied; no open structured bulk. Treat figures as requiring OCR/parsing.

## RESA (legal gazette)
- Free legal publications (incorporations, amendments, accounts deposits, dissolutions) referencing the RCS
  number. Free to consult.

## RBE (Registre des bénéficiaires effectifs)
- Beneficial-ownership register; general public access was **withdrawn after the 2022 CJEU ruling** (professionals
  / legitimate interest only). **Not** open. Personal data (GDPR).

## AED / VIES
- Validates a supplied LU VAT number (`LU` + 8 digits). Validation/enrichment only; not redistributable as a
  list.

## data.public.lu / STATEC
- Open statistical data (CC0/open per dataset) — **not** the company register.

## Commercial aggregators
- Kyckr, Creditreform, etc. resell the RCS register + parsed annual accounts under **commercial, per-vendor
  contracts**.

## Personal data / GDPR
- Directors/officers (in documents) and beneficial owners (RBE) are **personal data** — apply a GDPR lawful basis
  + retention before persisting; no direct-marketing reuse.

## Summary recommendation
- **Free (manual)**: RCS search + downloading filed documents incl. annual accounts (PDF).
- **Blocked for automation**: RCS search (captcha; no open bulk/API) — do not bypass.
- **Paid**: certified extracts; bulk + structured financials via a vendor.
- **Restricted**: RBE beneficial ownership.
- Confirm RCS reuse terms; treat officer/owner data under GDPR.
