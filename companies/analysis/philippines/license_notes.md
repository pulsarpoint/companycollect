# License & terms — Philippines

## Summary

SEC company documents are **paid** (per document, via SEC Express); PSE EDGE listed
disclosures are **public**; data.gov.ph has no accessible company dataset. Treat SEC
reuse/redistribution as **restricted**.

## Per source

### SEC Express / eFAST / eSPARC (`secexpress.ph`, `efast.sec.gov.ph`, `esparc.sec.gov.ph`)
- Official SEC systems. **GIS / AFS / Articles / certificates** are **paid per
  document** via SEC Express; eFAST/eSPARC are login portals. No open bulk/API; no
  stated bulk-reuse rights. Do not bypass the paywall/login. Field lists here come
  from public product descriptions — **no real company values copied**.

### PSE EDGE (`edge.pse.com.ph`)
- Listed-company disclosures and financial reports are **public** (mandatory
  disclosure). Attribution to PSE / issuer. Be polite with the search endpoint; do
  not scrape aggressively. Listed companies only.

### DTI BNRS (`bnrs.dti.gov.ph`)
- Free business-name search/verification for sole proprietors; for verification use.
  No open bulk; reuse terms not stated.

### data.gov.ph
- Open-government datasets where present (attribution to the publisher), but the
  portal is a JS SPA with no accessible company dataset confirmed here.

## Personal data

The SEC **GIS** exposes **directors, officers, and stockholders** — personal data
when natural persons under the **Data Privacy Act of 2012 (RA 10173)**. These must be
**redacted** in committed outputs. Because SEC documents are paid, **no per-company
SEC values were captured**; the sample uses **PSE-verified + public-knowledge listed
companies** with **null SEC identifiers** (nothing fabricated).

## Practical guidance

- Use **PSE EDGE** for listed companies (open); buy **SEC Express** documents
  (GIS/AFS) per company for the rest; **DTI BNRS** for sole-prop name checks.
- Do not bypass the SEC paywall/login or the sec.gov.ph WAF.
- Currency **PHP**; English; dates Mon dd, yyyy.
