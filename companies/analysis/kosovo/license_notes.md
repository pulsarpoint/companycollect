# License & terms — Kosovo

## Summary

No source publishes an explicit open-data reuse license for company-level data.
The company register (**ARBK**) and the **ATK** per-company lookup are public for
**verification** but **technically gated** (bearer auth + CAPTCHA). Treat reuse
terms as **uncertain**, and do not bypass controls.

## Per source

### ARBK (`arbk.rks-gov.net`)
- Official register; usable via the browser. The backing API requires the SPA's
  **bearer token** (all `Services/*` → 401) and the search requires a **Cloudflare
  Turnstile** CAPTCHA token. No open bulk; the export endpoint is gated.
- **Do not bypass** the CAPTCHA or bearer auth. No stated bulk-reuse license.

### ATK VatRegist (`apps.atk-ks.org`)
- Per-company fiscal/VAT verification; **CAPTCHA-gated** ("I'm not a robot"). For
  verification use; no bulk; no stated reuse license. Do not bypass the CAPTCHA.

### ATK Open Data (`atk-ks.org/open-data`)
- Published "Open Data" XLSX, intended for public reuse, but **aggregate** only
  (sector/municipality/year). Attribution to ATK is appropriate. No personal data.

### Kosovo open-data portal
- Did not resolve; nothing to license.

## Personal data

ARBK exposes **owners (Pronarët)** with ownership percentages and ATK exposes the
**taxpayer name** — personal data when natural persons (Kosovo Law No. 06/L-082 on
Protection of Personal Data). These are **redacted** in committed outputs. Because
both per-company sources are CAPTCHA-gated, **no real per-company values were
extracted** in this investigation.

## Practical guidance

- Realistic access is **manual/browser per-company lookup** (ARBK, ATK VatRegist).
- Do not bypass CAPTCHA/bearer; do not scrape.
- ATK Open Data XLSX may be reused with attribution (aggregate, non-personal).
- Currency EUR; data tri-lingual (Albanian/Serbian/English).
