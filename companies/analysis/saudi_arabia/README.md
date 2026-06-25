# Company data sources for Saudi Arabia

## Status

- Official bulk data: **not found (open)** — no open bulk register
- Official API: **not open** — the CR inquiry is Nafath login-gated; inquiry hosts
  firewalled
- Open data portal: `open.data.gov.sa` **firewalled** (resolves, TCP timeout)
- License: registry data is restricted; exchange listed data is public (browser)
- Recommended ingestion path: **manual / browser** (MoC CR inquiry via Nafath;
  Tadawul for listed) — no open bulk/API

## Best source

The official company registry is the **Ministry of Commerce (MoC)** Commercial
Register (السجل التجاري, `mc.gov.sa`). It offers a **Commercial Register
inquiry/verification** e-service (company name, **CR number**, status, activities,
capital, managers), but the service requires **Nafath login** (national digital
identity), and the inquiry sub-hosts (`eservices.mc.gov.sa`, `businesscenter.gov.sa`,
`qaweem.mc.gov.sa`) were **NXDOMAIN / firewalled** from this environment. The
**Saudi Business Center** (unified company-establishment portal) was likewise not
reachable. There is **no open bulk register or open API**.

## Financial data

**Saudi Exchange (Tadawul)** (`saudiexchange.sa`) publishes **listed-company**
profiles, disclosures, and financial statements. It is **public via the browser** but
returned **HTTP 403 "Access Denied" (WAF)** for automated requests. **Private-company
financials** are not openly available. Currency **SAR**.

## Identifiers & tax

- **CR number (رقم السجل التجاري)** — 10-digit Commercial Registration number with a
  **region prefix** (1010 = Riyadh, 2050/2051 = Eastern Province, 4030 = Jeddah, …).
- **Unified National Number / "700 number" (الرقم الموحد)** — a `700…` unified
  company id linking the CR with government agencies.
- **VAT number** — 15-digit (ZATCA), starts and ends with `3`.
- Currency **SAR**. Languages: Arabic + English.

## Next action

Use the **MoC** Commercial Register inquiry (Nafath login) for company identity and
**Tadawul** (browser) for listed financials. There is **no open bulk register and no
open programmatic financials** (CR inquiry Nafath-gated; inquiry hosts firewalled;
Tadawul WAF-gated; open data firewalled). Managers/owners are personal data (PDPL,
Royal Decree M/19 of 1443H) — redact if obtained.
