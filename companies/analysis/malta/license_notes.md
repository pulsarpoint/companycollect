# Malta — License & Terms Notes

> Malta's MBR is a public register with free basic search, but the register's deeper data and documents (incl.
> annual accounts) are paid, the official API is a paid subscription, there is no open bulk, and beneficial
> ownership is restricted.

## MBR register
- The MBR is **public** and free to **search** (basic info, status). However:
  - **Certified documents** (extracts, **annual accounts**, etc.) carry a **fee** (EUR 5–25 per document).
  - The deeper register data (officers, shareholders, financial info) is delivered via paid documents or the
    paid API.
  - No open-data **licence** for reuse/redistribution is stated — **confirm terms** before redistribution.
  - The online registry portals (registry.mbr.mt, baros.mbr.mt) and data.gov.mt are **WAF-protected (HTTP 403)**
    against non-browser clients; there is **no open bulk/API**. Automated/bulk scraping is an access-control
    bypass — **do not** do it.

## MBR API packages
- The MBR has launched official **API packages** (e.g. a Company Search API) on a **subscription/paid** basis —
  the sanctioned automation path. Use under the package terms.

## Annual accounts / annual return
- Filed annual accounts (IFRS / GAPSME for small companies) + annual return are **public** but obtained as
  **paid documents** (EUR 5–25), usually PDF. No open structured bulk; treat figures as requiring OCR/parsing or
  the paid API.

## UBO (beneficial ownership)
- Access to the beneficial-ownership register was restricted after the 2022 CJEU ruling; since **July 2025**,
  access is for those demonstrating a **legitimate interest** (without alerting the company). **Not** open.
  Personal data (GDPR).

## CFR / VIES
- Validates a supplied MT VAT number (`MT` + 8 digits). Validation/enrichment only; not redistributable as a
  list.

## data.gov.mt
- National open-data portal; WAF-blocked to bots and does not publish the company register.

## Personal data / GDPR
- Officers, shareholders and beneficial owners are **personal data** — apply a GDPR lawful basis + retention
  before persisting; no direct-marketing reuse.

## Summary recommendation
- **Free (manual)**: MBR basic company search.
- **Paid**: documents (annual accounts, extracts); the MBR API packages; structured financials at scale via a
  vendor.
- **Blocked for automation**: registry portals + data.gov.mt (WAF) — do not bypass.
- **Restricted**: UBO (legitimate interest).
- Confirm MBR reuse terms; treat officer/shareholder/owner data under GDPR.
