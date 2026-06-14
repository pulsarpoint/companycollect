# Greece — License & Terms Notes

> Greek company identity is publicly searchable for free, but there is no stated open-data licence and no bulk
> export; the API is access-controlled (reCAPTCHA + rate limits). Financials are public but document-based.

## GEMI publicity portal
- The General Commercial Registry is **public** and free to **search manually** at businessportal.gr /
  publicity.businessportal.gr. However:
  - No open-data **licence** is stated for reuse/redistribution of the register data — **confirm terms** before
    any redistribution. Public access ≠ permission to redistribute.
  - The underlying `/api` is **reCAPTCHA-protected** and **rate-limited** (verified HTTP 429). Automated/bulk
    scraping is an **access-control bypass** — **do not** do it. There is no sanctioned open bulk endpoint.

## GEMI financial statements
- Annual financial statements (ΕΛΠ Greek GAAP / IFRS) and balance sheets are **publicly filed** and viewable on
  the company's GEMI page, but as **PDF documents**. No bulk redistribution rights implied; no structured open
  figures. Treat figures as requiring OCR/parsing.

## AADE RgWsPublic
- The tax registry's company-data SOAP service requires **registered TaxisNet web-service credentials** issued
  by AADE. Use is governed by AADE's terms; **not** open bulk.

## VIES
- Validates a supplied EL VAT number. Validation/enrichment only; not redistributable as a list.

## data.gov.gr / Diavgeia / procurement
- **data.gov.gr**: curated **statistical** open data (token-gated); per-dataset licence; not the company
  register.
- **Diavgeia** and **procurement (ΚΗΜΔΗΣ)**: open government data that incidentally reference company **ΑΦΜ +
  name**; reusable under their open terms as a cross-reference, not as a company master.

## Commercial aggregators
- ICAP/CRIF, Kyckr, etc. resell GEMI/AADE data + parsed financials under **commercial, per-vendor contracts**.

## Personal data / GDPR
- Directors/representatives and any natural-person data are **personal data** — apply a GDPR lawful basis +
  retention before persisting; no direct-marketing reuse.

## Summary recommendation
- **Free (manual only)**: GEMI search + viewing financial-statement PDFs.
- **Blocked for automation**: GEMI `/api` (reCAPTCHA + rate limits) — do not bypass.
- **Credentials required**: AADE RgWsPublic.
- **Paid/structured**: commercial providers for parsed financials at scale.
- Confirm GEMI reuse terms; treat director data under GDPR.
