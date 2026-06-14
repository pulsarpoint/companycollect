# Company data sources for Hungary

## Status

- Official bulk data: **not found** (no open cégjegyzék/financials bulk export)
- Official API: **found but blocked/limited** (e-beszámoló search is reCAPTCHA-protected; full register data is paid)
- Open data portal: **partial** (NAV taxpayer databases; KSH statistics — no company-register bulk)
- License: **unclear** (public register; reuse terms not stated)
- Recommended ingestion path: **manual review / per-entity lookup**; a commercial provider for full register + structured financials at scale

## Best source

Hungary splits across two official Ministry-of-Justice services, joined on the **cégjegyzékszám** (registration
number) and the **adószám** (tax number):

- **e-beszámoló** (`e-beszamolo.im.gov.hu`) — **all annual financial statements are FREE to view** (no
  registration), with structured key figures (sales revenue, profit after tax, assets, equity, liabilities) +
  PDF + electronic form (XML). **But** the search endpoint is **reCAPTCHA-protected** (verified) →
  automated/bulk access is blocked; do not bypass.
- **e-cégjegyzék / Cégszolgálat** (`e-cegjegyzek.hu`) — **free basic** company info (name, cégjegyzékszám, seat,
  status, main activity); **full** extracts (officers, owners, history) are **paid**.

**NAV** publishes daily **VAT-subject (áfaalanyok)** databases for tax-number/VAT validation. So Hungary is a
**partial-open** country: free financials and basic identity (manual), but no open bulk and automation is
gated/paid.

## Next action

For lawful automation, use a **commercial provider** (OPTEN, Bisnode, Céginfo, companyapi.hu) for full register
+ structured financials, or manual e-beszámoló/e-cégjegyzék lookups. Enrich/validate tax numbers via the **NAV
áfaalany** database and **VIES**. Confirm reuse terms before redistribution; treat officer data under GDPR.
