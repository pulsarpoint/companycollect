# Company data sources for Egypt

## Status

- Official bulk data: **not found (open)** — no open company register
- Official API: **not open** — GAFI eServices are login-gated; the commercial
  registry is not openly searchable; EGX data endpoints are WAF-gated
- Open data portal: `data.gov.eg` / `egypt.gov.eg` **unreachable** at investigation
  time
- License: registry data is restricted; EGX listed disclosures are public (browser)
- Recommended ingestion path: **manual / browser** (EGX for listed; GAFI per-company
  via login) — no open bulk/API

## Best source

The official company authority is **GAFI — the General Authority for Investment and
Free Zones** (`gafi.gov.eg`), which establishes companies and runs **investor
eServices** (registration/incorporation) — **login-gated**. The **Commercial
Registry** (السجل التجاري, under GOEIC / Ministry of Supply) holds the commercial
registration but is **not openly searchable online**. There is **no open company
register or open API**.

The most usable **open-ish** source is the **EGX (Egyptian Exchange)** for listed
companies, but its data endpoints are WAF-gated for automation.

## Financial data

**EGX** (`egx.com.eg`) publishes **listed-company** profiles, disclosures, and
financial statements. The `ListedStocks.aspx` and `companiesprofilesearch.aspx`
pages load in a browser, but the underlying data endpoints (`getinformation.aspx`)
returned **"Request Rejected" (WAF)** to automated requests — public via the browser
only. **Private-company financials** are not openly available. Currency **EGP**.

## Identifiers & tax

- **Commercial Registry number (رقم السجل التجاري)** — commercial registration id.
- **Tax ID (الرقم الضريبي)** — Egyptian Tax Authority (ETA), 9-digit.
- **Unified company number** — increasingly used to link registry + tax.
- **EGX symbol / ISIN** (`EG…`) for listed companies.
- Currency **EGP**. Languages: Arabic + English.

## Next action

Use **EGX** (browser) for listed-company profiles + financials, and **GAFI**
eServices (login) for company establishment/registry per company. There is **no
open bulk register and no open programmatic financials** (EGX is WAF-gated, GAFI is
login-gated, data.gov.eg unreachable). Directors/shareholders are personal data
(Egypt PDP Law 151/2020) — redact if obtained.
