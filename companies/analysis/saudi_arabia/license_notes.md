# License & terms — Saudi Arabia

## Summary

Tadawul listed disclosures are **public** (browser); the MoC Commercial Register
inquiry is **Nafath login-gated**; open.data.gov.sa was firewalled. Treat registry
reuse/redistribution as **restricted**.

## Per source

### MoC Commercial Register (`mc.gov.sa`)
- Official register. The CR inquiry/verification e-service requires **Nafath login**
  (national digital identity); the inquiry sub-hosts were NXDOMAIN/firewalled. No open
  bulk/API; no stated bulk-reuse rights. Do not bypass the login. Field model from
  public knowledge — **no real values copied**.

### Saudi Exchange (Tadawul) (`saudiexchange.sa`)
- Listed-company disclosures are public (mandatory disclosure) and viewable via the
  browser, but the site is **WAF-gated** ("Access Denied", 403) for automation. Do
  not bypass the WAF. Attribution to Tadawul / issuer. Listed companies only.

### open.data.gov.sa (SDAIA)
- National open-data portal under an open-government license where datasets exist
  (attribution to the publisher), but it was **firewalled** here and no company-
  register dataset was confirmed.

## Personal data

MoC Commercial Register records include **managers and owners/partners** — personal
data under Saudi Arabia's **Personal Data Protection Law (PDPL, Royal Decree M/19 of
1443H / 2021)**. These must be **redacted** in committed outputs. Because all registry
sources are gated, **no per-company registry values were captured**; the sample uses
**public-knowledge Tadawul-listed companies** with **null registry identifiers**
(nothing fabricated).

## Practical guidance

- Use the **MoC** CR inquiry (Nafath login) for identity and **Tadawul** (browser) for
  listed financials.
- Do not bypass the Nafath login, the Tadawul WAF, or any firewall; do not assume
  registry reuse rights.
- Currency **SAR**; Arabic + English; dates Hijri (and Gregorian).
