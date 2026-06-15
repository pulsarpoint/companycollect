# Vietnam Company Data — Investigation

## Conclusion

Vietnam is a **portal-gated, no-open-bulk** country: an authoritative national
register exists with a **free per-company public search**, but there is **no open
API or bulk download**, and **financial statements are open only for listed
companies**.

- **Register**: the **National Business Registration Portal (NBRP)**,
  `dangkykinhdoanh.gov.vn` (Business Registration Authority, Ministry of Planning
  and Investment), offers a free per-company search returning name, **enterprise
  code = tax code (mã số doanh nghiệp, 10–13 digits)**, head-office address,
  business lines (VSIC), legal representative, and legal status. **Vietnamese-only,
  CAPTCHA-gated on submit, no open API/bulk.** Bulk is sold via MOU.
- **Tax**: the General Department of Taxation lookup (`tracuunnt.gdt.gov.vn`)
  validates a tax code (status/name/address) per company, also CAPTCHA-gated.
- **Open data portal**: `data.gov.vn` / `open.data.gov.vn` exist but publish **no
  enterprise-registration dataset** (datasets observed: construction prices,
  cultural heritage, agro-chemicals). GSO's **Vietnam Enterprise Survey (VES)** is
  a **statistical, access-controlled** survey, not a per-company register.
- **Financials**: only **listed companies** disclose audited statements via
  **HOSE / HNX / SSC** (CISM/ECM disclosure). Non-listed accounts are filed to the
  tax authority but **not published**.

## Identifiers

- **Mã số doanh nghiệp (MSDN)** — enterprise code, **also the tax code (MST)**,
  10 digits (HQ) or 13 (with a 3-digit branch suffix). The company id.
- **VAT**: Vietnam has no separate VAT number — the **tax code** serves VAT.
- **VSIC** — Vietnam Standard Industrial Classification (business lines /
  ngành nghề), the activity code.

## Sources found

### 1. NBRP — National Business Registration Portal (official) — partial / gated
- `https://dangkykinhdoanh.gov.vn/` (reachable; 302→200). Free per-company search
  (no account) by enterprise code or name → name, MSDN/tax code, address, business
  lines, legal representative, legal status. **CAPTCHA on submit; no open API or
  bulk; Vietnamese-only.** OpenCorporates mirrors this register (register 277).
- **Paid bulk**: the national business-registration database is available via a
  **fee-based MOU** with the Business Registration Support Centre. → blocked_by_payment.

### 2. GDT taxpayer lookup (tracuunnt.gdt.gov.vn) — per-company, gated
- Tax-code validation/lookup (status, name, managing tax office). Reachable (200);
  CAPTCHA-gated. Useful to confirm a tax code/status for a known company.

### 3. HOSE / HNX / SSC — listed-company financials — listed-only
- Listed issuers disclose audited financial statements (balance sheet, P&L, cash
  flow, notes) via the exchanges' disclosure systems (HNX CISM, SSC ECM,
  congbothongtin) and IR pages. Per-company; **no clean open bulk API**. Covers
  only the listed population.

### 4. GSO — Vietnam Enterprise Survey (VES) — statistical, by request
- Firm-level survey (characteristics, financial accounts, output). **Aggregate /
  access-controlled**, not a per-company open register. gso.gov.vn.

### 5. Aggregators (masothue.com, infodoanhnghiep.com; vietstock/cafef/fireant) — license-uncertain
- Scrape/repackage NBRP identity and listed-company financials under their own
  terms. Cross-check only; not official; license-uncertain.

## What was NOT bypassed

- The NBRP and GDT **CAPTCHA** gates were **not** circumvented; no automated
  search was run against them. Only reachability and the public landing page were
  checked. No paid MOU data or aggregator scraping was performed.

## Recommended ingestion

There is **no lawful open bulk** for Vietnamese companies. Options: NBRP
per-company official lookup (respect the CAPTCHA), a **paid NBRP data MOU** for
coverage, and **HOSE/HNX/SSC** for listed-company financials. Identity keys on the
**enterprise code = tax code**. Redact the legal-representative name (personal
data).
