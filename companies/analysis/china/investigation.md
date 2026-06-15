# China Company Data — Investigation

## Conclusion

China is a **portal-gated, no-open-bulk** country: an authoritative national
register exists but is **real-name + CAPTCHA gated** with **no open API or bulk**,
and **financial statements are open only for listed companies**.

- **Register**: the **National Enterprise Credit Information Publicity System
  (GSXT)**, `gsxt.gov.cn` (State Administration for Market Regulation, **SAMR**),
  is the official register. A per-company search returns the **USCC (Unified
  Social Credit Code, 18-char)**, status, legal representative, registered
  address, and establishment date. **Real-name authentication required for queries
  (since Nov 2021)**, **CAPTCHA-gated**, Chinese-only, **no open bulk/API**, and
  frequently unreachable externally (verified **HTTP 521**).
- **Open data**: there is **no national open dataset** of the company register;
  Credit China (creditchina.gov.cn) publishes credit/penalty info (bot-protected,
  HTTP 412) but not the full register; provincial/municipal open-data portals have
  some company-related datasets, not the national register.
- **Financials**: listed companies disclose via **cninfo (巨潮资讯网,
  cninfo.com.cn)** — the CSRC-designated platform — and the **SSE/SZSE/BSE**
  exchanges (verified reachable, HTTP 200). Non-listed companies do **not** publicly
  disclose financials.

## Identifiers

- **USCC — Unified Social Credit Code (统一社会信用代码)** — **18-character**
  alphanumeric; the unified company id, which **doubles as the taxpayer
  identification number**. (Replaced the old 注册号 registration number + tax
  number since 2015.)
- China levies **VAT** but the taxpayer id is the **USCC** — there is no separate
  VAT number.
- Listed companies also have a **stock code** (e.g. 600519 SSE / 000001 SZSE).

## Sources found

### 1. GSXT — official register (gsxt.gov.cn) — gated
- SAMR's national register. Per-company search (USCC/name) → USCC, status
  (存续/在营/注销/吊销), legal representative, registered capital, address,
  establishment date, business scope. **Real-name auth + CAPTCHA; no open
  bulk/API; HTTP 521 externally.** Not bypassed.

### 2. Credit China (creditchina.gov.cn) — gated/secondary
- Credit and administrative-penalty information portal (NDRC/PBoC). Bot-protected
  (HTTP 412). Some downloadable penalty/redlist datasets, but **not** the full
  company register.

### 3. cninfo / SSE / SZSE / BSE — listed-company financials — listed-only
- **cninfo.com.cn** (巨潮资讯网) is the CSRC-designated information-disclosure
  platform; the **Shanghai (sse.com.cn)**, **Shenzhen (szse.cn)** and **Beijing**
  exchanges also publish issuer disclosures. Annual reports + financial statements
  for **listed** companies (A/B shares); **H-shares** via HKEX. Reachable
  (HTTP 200). The open route to Chinese financials — **listed issuers only**.

### 4. Commercial aggregators (Qichacha 企查查, Tianyancha 天眼查, Aiqicha 爱企查) — license-uncertain
- Resell GSXT identity + listed financials with enrichment. Anti-bot (HTTP 419)
  and **paid/restricted**. Not official; license-uncertain. Cross-check only.

## What was NOT bypassed

- GSXT's **real-name authentication + CAPTCHA** were **not** circumvented; no
  automated query was run. Only reachability was checked. Aggregator anti-bot was
  not bypassed; no paid data accessed.

## Recommended ingestion

There is **no lawful open bulk** for Chinese companies. Options: GSXT per-company
official lookup (respect real-name + CAPTCHA), a **licensed commercial provider**
for coverage, and **cninfo/SSE/SZSE** for listed-company financials. Identity keys
on the **USCC** (= taxpayer id). Redact the legal-representative name (personal
data).
