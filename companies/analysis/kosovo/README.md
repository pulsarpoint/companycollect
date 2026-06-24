# Company data sources for Kosovo

## Status

- Official bulk data: **not found (open)** — the company register (ARBK) has no
  open bulk; its export endpoint is auth-gated
- Official API: **exists but gated** — ARBK's SPA API returns **401 (bearer)** and
  the search is behind **Cloudflare Turnstile (CAPTCHA)**; the ATK VAT lookup is
  also CAPTCHA-gated
- Open data portal: **partial** — ATK publishes "Open Data" XLSX, but they are
  **aggregate statistics** (by sector/municipality/year), not company-level; the
  national portal `opendata.rks-gov.net` did not resolve
- License: not stated; access for verification only
- Recommended ingestion path: **manual / browser per-company lookup** (ARBK, ATK);
  no programmatic open bulk

## Best source

**ARBK — Agjencia për Regjistrimin e Bizneseve të Kosovës** (Kosovo Business
Registration Agency, `arbk.rks-gov.net`), under the Ministry of Industry,
Entrepreneurship and Trade. It is the official company register and its public
SPA exposes per-business identity, ownership, capital, activity, and status. But
the backing API (`/api/api/Services/*`) returns **401 Unauthorized** without the
app's bearer token, and the search (`Services/KerkoBiznesin`) requires a
**Cloudflare Turnstile** CAPTCHA token. There is **no open bulk** and the
`Services/EksportoBizneset` export is also gated. Usable via the browser only.

## Financial data

**Not available openly.** Kosovo has no public register of company financial
statements (no equivalent annual-accounts filing portal for private companies).
ARBK holds registered **capital** (Kapitali) and ownership percentages; revenue/
balance-sheet data is not published. ATK Open Data is **aggregate** only.

## Identifiers & tax

- **NUI — Numri Unik Identifikues** (Unique Identification Number), the primary
  business id; for businesses it equals the **Numri Fiskal** (fiscal/tax number),
  typically **9-digit**.
- **Numri i Biznesit / NRB** — business registration number.
- **Numri i TVSH-së** — VAT number, **separate**, only for VAT-registered entities.
- ARBK data is **tri-lingual** (Albanian / Serbian / English).

## Next action

Use ARBK per-business lookup via the browser (Turnstile-gated) and the ATK
VatRegist app for fiscal/VAT verification (also CAPTCHA-gated). Treat
**owners (Pronarët)** as personal data and redact. No open bulk/API; do not
bypass the CAPTCHA or bearer auth. ATK Open Data XLSX give only sector/municipality
aggregates.
