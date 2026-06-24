# License & terms — Indonesia

## Summary

No source publishes an open-data reuse license for company-level data. AHU profiles
are **paid (PNBP)**; OSS NIB lookup is for verification; IDX listed disclosures are
public but Cloudflare-gated. Treat reuse terms as **restricted/uncertain** and do
not bypass controls.

## Per source

### AHU Online (`ahu.go.id`, Ministry of Law)
- Authoritative legal-entity registry. Free profile **search**; full profiles and
  documents are **paid (PNBP)**. No open bulk/API; no stated bulk-reuse license. Host
  was firewalled from this environment (DNS resolves; TCP/HTTP timeout). Catalogued
  from public documentation; no raw values copied.

### OSS (`oss.go.id`, BKPM)
- Public per-company NIB search for verification. No open bulk; reuse terms not
  stated. Do not scrape the SPA aggressively.

### IDX (`idx.co.id`)
- Listed-company disclosures are public (mandatory disclosure) and viewable via the
  browser, but the site is **Cloudflare-gated** for automation (403). Do not bypass
  the Cloudflare challenge. Attribution to IDX/issuer applies.

### Satu Data Indonesia (`data.go.id`)
- Openly published datasets (statistics) reusable with attribution; does not host the
  register.

## Personal data

AHU exposes **pengurus (directors/commissioners)** and **pemegang saham
(shareholders)** — personal data when natural persons (Indonesia **PDP Law**, UU No.
27/2022 Perlindungan Data Pribadi). These must be **redacted** in committed outputs.
Because AHU/IDX were not reachable for automation, **no per-company values were
captured**; the sample uses **public-knowledge listed-company names** with **null
identifiers** (nothing fabricated).

## Practical guidance

- Use AHU profile (paid) + OSS NIB search for identity; IDX for listed financials
  (browser). Do not bypass Cloudflare or paid access.
- Currency **IDR**; Indonesian language; dates dd-mm-yyyy.
