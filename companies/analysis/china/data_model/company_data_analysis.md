# Company Data Analysis For China

## Summary

China's company data is **portal-gated with no open bulk**. The authoritative
national register, **GSXT** (国家企业信用信息公示系统, run by SAMR), requires
**real-name authentication and a per-query CAPTCHA**, exposes **no open API or
bulk download**, and is frequently unreachable from outside China (HTTP 521).
There is therefore no lawful open path to download the register at scale.

What *can* be modeled:

- A clean, country-specific profile keyed on the **USCC** (统一社会信用代码,
  18-character Unified Social Credit Code), which is both the **company id** and
  the **taxpayer id**. China has **no separate VAT number**.
- **Listed-company financials** are genuinely open via **cninfo** (and SSE/SZSE)
  in CNY under Chinese Accounting Standards (ASBE) — but only for listed issuers.
- Everything else (identity at scale, shareholders/officers, non-listed
  financials) is either gated (GSXT), paid/license-uncertain (commercial
  aggregators), or simply not publicly disclosed.

The profile is consequently **planning-only** for identity and **listed-only**
for financials. The example record is **schematic** (placeholder USCC; legal
representative redacted), not copied from any gated source.

## Sources Analyzed

| Source slug | Source name | Status | Access | License | Role |
|---|---|---|---|---|---|
| gsxt_search | GSXT (SAMR national enterprise credit system) | blocked_by_authentication | real-name + CAPTCHA; no bulk/API (HTTP 521) | restricted/unclear | Authoritative identity (gated) |
| cninfo_disclosure | cninfo / SSE / SZSE disclosure | useful_secondary | public, listed issuers only | restricted/unclear | Listed financials |
| credit_china | Credit China (信用中国, NDRC) | blocked_by_authentication | public, bot-protected (HTTP 412) | restricted/unclear | Risk/compliance enrichment |
| cn_aggregators | Qichacha / Tianyancha / Aiqicha | blocked_by_license_uncertainty | paid / anti-bot (HTTP 419) | restricted/vendor | Identity + ownership at scale (paid) |

## What Each Source Contributes

- **gsxt_search (GSXT/SAMR)** — the authoritative register: USCC, legal name,
  company type, registration status, establishment date, registered (subscribed)
  capital, registered address, business scope (free text), and the legal
  representative (personal data). **Gated**: real-name login + CAPTCHA per query,
  no open bulk/API, often unreachable externally. Cataloged from public docs
  only; no records copied.
- **cninfo_disclosure** — official disclosure portal for **listed** companies:
  balance sheet (资产负债表), income statement (利润表), cash-flow statement
  (现金流量表) from annual/interim reports, keyed by stock code, in CNY under
  ASBE. Reachable (HTTP 200) but covers listed issuers only.
- **credit_china (NDRC)** — administrative penalties (行政处罚) and red/black-list
  status keyed on USCC. Compliance/risk enrichment only; **not** the company
  register and not financials. Bot-protected (HTTP 412).
- **cn_aggregators** — commercial vendors that repackage GSXT identity plus
  shareholders/officers and listed financials via paid APIs. The only realistic
  route to bulk identity + ownership, but **paid, license-uncertain, anti-bot**
  (HTTP 419), and **not authoritative**. Planning-only; verify against official
  sources; PIPL applies.

## Proposed Country Company Profile

A single object keyed on `registration.uscc`:

- `registration.uscc` — 18-char USCC (company id + join key).
- `tax_identifiers` — `tax_id` = USCC; `vat_id` = null (none in China).
- `legal_identity` — legal name, company type.
- `status` — raw 登记状态 + mapped active/deregistered/revoked.
- `incorporation` — establishment date.
- `capital` — registered (subscribed) capital, CNY.
- `registered_location` — registered address.
- `activity` — business scope (free text; no coded classification).
- `officers[]` — legal representative only (personal data, redacted).
- `financial_statements[]` — listed-only, CNY, ASBE (empty for non-listed).
- `source_provenance[]` — per-section access/license/retrieval provenance.

Identity sections are **gated/planning-only**; financials are **listed-only**.

## Join And Precedence Rules

- **Join key:** USCC (18-char) across every source; it is also the taxpayer id.
  Listed financials join via **stock_code → USCC**.
- **Precedence:** GSXT (authoritative identity) > cninfo (authoritative listed
  financials) > credit_china (secondary risk) > aggregators (paid, last resort,
  must be verified).
- **No VAT:** never synthesize a VAT id; taxpayer id = USCC.

## Missing Or Restricted Data

- **No open bulk register / API** — GSXT is real-name + CAPTCHA gated and often
  unreachable externally; no lawful open bulk path.
- **No separate VAT id.**
- **No coded activity classification** — 经营范围 is free text.
- **Non-listed financials** are not publicly disclosed.
- **Shareholders/officers** beyond the legal representative are not open (paid
  aggregators only).
- **PIPL** governs personal data (legal rep, shareholders); **cross-border
  data-export** rules apply to any transfer of China company/personal data
  abroad.

## Common Mapper Notes

- Map `company_id`, `registration_number`, and `tax_id` all to the **USCC**;
  mark `vat_id` as `not_available_in_open_sources`.
- Mark China identity mappings as **gated/planning-only** (contrast with
  fully-open registers like UK Companies House, RO EDR, UA EDR).
- Map `financials` only for listed issuers (cninfo/SSE/SZSE, CNY, ASBE) via
  stock_code → USCC; treat as planning-only at the country level.
- Treat `officers`/`owners` as personal data (PIPL) and redact in any committed
  sample.
