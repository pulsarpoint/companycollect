# License & terms — Egypt

## Summary

EGX listed disclosures are **public** (browser); GAFI/Commercial Registry company
data is **restricted** (login / not openly searchable). No open-data portal was
reachable. Treat registry reuse/redistribution as **restricted**.

## Per source

### EGX (`egx.com.eg`)
- Listed-company profiles, disclosures, and financial statements are **public**
  (mandatory disclosure) — but the site is **WAF-gated** for automated requests
  ("Request Rejected"). Use a browser; do not bypass the WAF. Attribution to EGX /
  issuer. Listed companies only.

### GAFI (`gafi.gov.eg`)
- Company establishment / investor eServices are **login-gated**; no public company
  search/register, no open API. No stated bulk-reuse rights. Field model from public
  knowledge — **no real values copied**.

### Commercial Registry (السجل التجاري)
- Commercial registration data is **not openly searchable** online and is restricted.
  No open bulk/API.

### egypt.gov.eg / data.gov.eg
- Unreachable at investigation time; nothing to license.

## Personal data

GAFI / Commercial Registry company records include **directors and shareholders** —
personal data under **Egypt's Personal Data Protection Law (Law No. 151 of 2020)**.
These must be **redacted** in committed outputs. Because all registry sources are
gated, **no per-company registry values were captured**; the sample uses
**public-knowledge EGX-listed companies** with **null registry identifiers** (nothing
fabricated).

## Practical guidance

- Use **EGX** (browser) for listed companies + financials; **GAFI** eServices (login)
  for company establishment/registry per company.
- Do not bypass the EGX WAF or the GAFI login; do not assume registry reuse rights.
- Currency **EGP**; Arabic + English; dates dd/mm/yyyy.
