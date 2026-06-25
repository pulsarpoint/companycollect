# License & terms — United Arab Emirates

## Summary

DFM/ADX listed disclosures are **public** (browser); the NER, emirate DEDs, and
free-zone registers are **login/WAF-gated**; open-data portals were unreachable. Treat
registry reuse/redistribution as **restricted**.

## Per source

### National Economic Register (NER) — Ministry of Economy
- Unified company search; **login-gated** e-service. No open bulk/API; no stated
  bulk-reuse rights. Field model from public knowledge — **no real values copied**.

### Emirate DEDs (Dubai DET / Invest in Dubai, Abu Dhabi ADDED, etc.)
- Trade-license issuance and verification per emirate; WAF/login-gated. No open bulk;
  reuse terms not stated. Do not bypass the WAF/login.

### Free-zone registers (DIFC, ADGM)
- DIFC and ADGM operate **public registers** of free-zone entities (common-law
  jurisdictions), intended for public consultation, but the search apps were **WAF/
  rate-limited** for automation (DIFC 429; registration.adgm.com 403). Public via the
  browser; do not bypass the WAF. Confirm each registrar's reuse terms.

### DFM & ADX
- Listed-company disclosures are public (mandatory disclosure) and viewable via the
  browser, but the data feeds are **WAF/auth-gated** for automation (ADX 403; DFM
  feeds auth-gated). Attribution to DFM/ADX/issuer. Listed companies only.

### bayanat.ae / data.gov.ae
- Unreachable at investigation time; nothing to license.

## Personal data

Company **owners, managers, and directors** are personal data under the UAE's
**Personal Data Protection Law (PDPL, Federal Decree-Law No. 45 of 2021)** and the
free-zone data-protection regimes (DIFC DP Law 2020; ADGM Data Protection Regulations
2021). These must be **redacted** in committed outputs. Because all registry sources
are gated, **no per-company registry values were captured**; the sample uses
**public-knowledge listed companies** with **null registry identifiers** (nothing
fabricated).

## Practical guidance

- Use the **NER** (login) / **emirate DEDs** / **DIFC-ADGM** registers (browser) for
  identity, and **DFM/ADX** (browser) for listed financials.
- Do not bypass the WAF, rate limits, or logins; do not assume registry reuse rights.
- Currency **AED**; Arabic + English; dates dd/mm/yyyy.
