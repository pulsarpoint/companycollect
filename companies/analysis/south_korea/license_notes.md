# South Korea — License Notes

## OpenDART (FSS) — public disclosure, free key

- OpenDART exposes Korea's statutory **corporate disclosure** (DART). The disclosed
  information is public and intended for reuse; access requires a **free API key
  (`crtfc_key`)** obtained by registering on opendart.fss.or.kr (no payment).
- Reuse of the disclosure data is generally permitted; redistribution of bulk
  financial data should follow the OpenDART terms of use and rate limits
  (~20,000 calls/day per key).
- Treatment here: **blocked_by_authentication** (free key); data is public
  disclosure and reusable. No records fetched without a key.

## NTS business-status API (data.go.kr) — free key, KOGL

- Provided through **data.go.kr** under the **Korea Open Government Licence
  (KOGL)** style terms; requires a **free service key**. KOGL Type 1 permits free
  use including commercial, with attribution.
- Treatment here: **blocked_by_authentication** (free key); reusable with
  attribution. Catalog only.

## data.go.kr datasets — KOGL

- Most data.go.kr company/tax datasets are licensed under **KOGL** (free reuse,
  attribution). Each dataset states its own KOGL type; verify per dataset.

## IROS court registry — paid

- The Supreme Court commercial registry provides registry extracts
  (등기사항증명서) on a **fee-per-issue** basis; no open bulk/API.
- Treatment here: **blocked_by_payment**. Cataloged from public docs only; no
  values copied.

## Personal data

- **CEO name** (`ceo_nm` in OpenDART) and **directors** (court registry) are
  **personal data** under Korea's **PIPA (Personal Information Protection Act)**.
  They must be handled lawfully and **redacted** in committed/shared samples. The
  committed sample redacts the CEO name.

## Tax identifiers

- The **사업자등록번호 (business registration number, 10-digit)** is the tax id and
  also serves as the **VAT number** — Korea has VAT (부가가치세) but **no separate
  VAT id**. It is a business identifier (corporate), not personal data, but should
  still be handled carefully.
