# License & terms — Nigeria

## Summary

NGX listed disclosures are **public**; CAC company documents are **paid**; the CAC BO
register is public via browser but **token-gated** for automation. Treat CAC
reuse/redistribution as **restricted**.

## Per source

### NGX (`ngxgroup.com`, `doclib.ngxgroup.com`)
- Listed-company market data + disclosures are **public** (mandatory disclosure). The
  equities JSON API is openly reachable. Attribution to NGX / issuer. Be polite with
  request volume. Listed companies only.

### CAC registry (`search.cac.gov.ng`, CAC portal)
- Official register. Public search is **Cloudflare-gated**; documents (status report,
  certified extract, annual returns, AFS) are **paid** via the CAC portal. No open
  bulk/API; no stated bulk-reuse rights. Do not bypass Cloudflare or the paywall.
  Field model from public knowledge — **no real values copied**.

### CAC BO Register (`bor.cac.gov.ng`)
- Public beneficial ownership register (PSC). Public via the browser; the API requires
  an access token. **Do not bypass** the token gate. **Security/privacy note:** a
  token-less endpoint (`/auth/access-token`) returned an individual user's personal
  profile (a broken-access-control misconfiguration) — this was **not used, not
  stored, and not pursued**; it is recorded only as a data-protection concern.

### data.gov.ng
- Unreachable at investigation time; nothing to license.

## Personal data

CAC company documents (directors, shareholders) and the **BO register** (beneficial
owners / persons with significant control) are personal data under the **Nigeria Data
Protection Act 2023 (NDPA)**. These must be **redacted** in committed outputs, and any
inadvertently-exposed personal data (e.g. the misconfigured endpoint above) must
**not** be stored or processed. The sample uses **NGX-verified + public-knowledge
listed companies** with **null CAC identifiers** (nothing fabricated, no PII).

## Practical guidance

- Use the **NGX equities API** for listed companies (open); buy **CAC** documents
  (paid) per company for the rest.
- Do not bypass the CAC Cloudflare/paywall or the BO token gate; do not store leaked PII.
- Currency **NGN**; English; dates dd-mm-yyyy.
