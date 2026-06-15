# Company data sources for Vietnam (VN)

## Status

- Official bulk data: **not found (open)** — the National Business Registration Portal has no open bulk/API; bulk is sold via MOU.
- Official API: **not found (open)** — the portal is per-company **search-only** and **CAPTCHA-gated**.
- Open data portal: **found but no company register** — data.gov.vn / open.data.gov.vn exist but publish no enterprise-registration dataset.
- License: **restricted/unclear** — no open re-use terms for the register; GSO enterprise survey is statistical and access-controlled.
- Recommended ingestion path: **manual review / licensed data** — no lawful open bulk; per-company official lookup or a paid MOU/aggregator.

## Best source

**National Business Registration Portal — NBRP** (`dangkykinhdoanh.gov.vn`), run by
the Business Registration Authority (Ministry of Planning and Investment). It is the
**authoritative** register and offers a **free per-company public search** (no
account): business name, **enterprise code = tax code (mã số doanh nghiệp, 10–13
digits)**, head-office address, business lines (VSIC), legal representative, and
legal status. But it is **Vietnamese-only**, **CAPTCHA-gated on submit**, and has
**no open API or bulk download** — comprehensive coverage requires a paid data MOU
with the agency or a (license-uncertain) aggregator.

## Financial data

**Not openly available for most companies.** Only **listed companies** disclose
audited financial statements (balance sheet, P&L, cash flow, notes) through the
**HOSE / HNX exchanges and the State Securities Commission (SSC)** disclosure
systems (CISM/ECM, congbothongtin) — per-company, no clean open bulk API.
Non-listed companies file accounts with the tax authority but these are **not
published**. Third-party aggregators (vietstock, cafef, fireant) repackage
listed-company financials under their own terms.

## Next action

For a lawful pipeline: (1) use the NBRP per-company official lookup for verified
identity (respecting the CAPTCHA — no bypass); (2) negotiate the **paid NBRP bulk
data MOU** for coverage; (3) pull **listed-company financials** from HOSE/HNX/SSC
per issuer. Treat aggregators as license-uncertain. Person data (legal
representative) is personal data — redact.
