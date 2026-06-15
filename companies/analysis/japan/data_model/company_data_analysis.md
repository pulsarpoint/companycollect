# Company Data Analysis For Japan

## Summary

Japan supports a **strong company profile**: a **fully-open national identity
layer** plus **free-but-key-gated financial and enrichment sources**, all joined
on one clean key — the **13-digit Corporate Number (法人番号)**, which is also the
corporate taxpayer number (Japan has **no separate VAT number**).

- **Identity (open bulk):** NTA Corporate Number Publication — every registered
  corporation (~5M) with name (JP/EN/kana), address, corporate kind, and
  registry-closure status. National + per-prefecture bulk (CSV SJIS/Unicode/XML),
  monthly full + daily diff. Verified live (Tottori, 20,153 rows, 30 columns).
- **Financials (free key):** EDINET XBRL for listed/disclosure-obligated companies.
- **Enrichment (free token):** gBizINFO — establishment date, capital, employees,
  business summary/items, light financials, procurement, subsidies.
- **Officers/definitive registry (paid):** Legal Affairs Bureau registry —
  officers, capital, purpose, incorporation date. Planning-only.

## Sources Analyzed

| Source slug | Source name | Status | Access | License | Role |
|---|---|---|---|---|---|
| nta_houjin_bangou | NTA Corporate Number Publication | recommended | public, no auth | free use (public data) | Authoritative identity (open bulk) |
| edinet_xbrl | EDINET (FSA XBRL financials) | blocked_authentication | free Subscription-Key | public disclosure | Listed/obligated financials |
| gbizinfo | gBizINFO (METI) | blocked_authentication | free token | gov standard (≈ CC-BY) | Enrichment: capital, employees, est. date, financials |
| houki_toukibo | Legal Affairs Bureau registry | blocked_payment | paid per-record | restricted | Officers, definitive incorporation/capital (planning-only) |

## What Each Source Contributes

- **nta_houjin_bangou** — the authoritative open register: 13-digit corporate
  number (= tax id), legal name (+ English opt-in + furigana), corporate kind,
  full address (prefecture/city/street + codes + postal code), registry-closure
  date/cause (→ status), successor number (mergers), and assignment date. Real
  data verified. **No financials, capital, officers, or industry code.**
- **edinet_xbrl** — XBRL financial facts (net sales, operating/net income, total
  assets, net assets) from securities reports for listed and disclosure-obligated
  filers, joined via `JCN` (corporate number). Free Subscription-Key; v1 retired.
- **gbizinfo** — keyed on the corporate number, supplies what NTA lacks:
  **establishment date** (true founding date), **capital**, **employees**,
  business summary/items, and lighter financials, plus procurement/subsidy/
  certification context. Free token.
- **houki_toukibo** — the only source of **officers/directors** and the definitive
  incorporation date/capital/purpose. Pay-per-record; planning-only; officer data
  is APPI personal data.

## Proposed Country Company Profile

A single object keyed on `registration.corporate_number`:

- `registration` — corporate number (+ assignment date).
- `tax_identifiers` — tax_id = corporate number; vat_id = null.
- `legal_identity` — name (JP/EN/kana), corporate kind.
- `status` — active/closed (+ close date/cause, successor number).
- `registered_location` — prefecture/city/street, codes, postal code.
- `company_details` — gBizINFO: establishment date, capital, employees, business
  summary/items (free token; planning-only until fetched).
- `financial_statements[]` — EDINET XBRL per fiscal year, JPY (free key; listed/
  obligated only).
- `officers[]` — paid registry; APPI personal data (planning-only, redacted).
- `source_provenance[]` — per-section access/license/retrieval.

The example record uses **real NTA identity** for a Tottori SME; enrichment and
financial sections are clearly marked planning-only (a non-listed SME has no
EDINET filings, and those sources were not fetched).

## Join And Precedence Rules

- **Join key:** 13-digit corporate number across all sources (EDINET `JCN`,
  gBizINFO `corporate_number`, paid registry's 12-digit number is its base).
- **Precedence:** NTA (identity) > EDINET (financials) > gBizINFO (est. date,
  capital, employees, light financials) > paid registry (officers, definitive
  fields).
- **Founding date:** use gBizINFO/registry establishment date, **never** NTA
  assignment date.
- **Financials:** EDINET authoritative over gBizINFO finance[].

## Missing Or Restricted Data

- **Officers/directors** — paid registry only (APPI personal data).
- **Financials of non-listed companies** — not openly available.
- **Definitive incorporation date, capital, purpose** — gBizINFO (free, partial) or
  paid registry; not in NTA.
- **Industry/activity code** — no formal JSIC code in NTA; gBizINFO business_items
  is the closest.
- **Beneficial ownership** — no open register.
- **No VAT number** — taxpayer id = corporate number.

## Common Mapper Notes

- Map `company_id`, `registration_number`, `tax_id` all to the corporate number;
  mark `vat_id` as `not_available_in_open_sources`.
- Identity is open bulk; financials/enrichment are free-key/token; officers paid.
- Treat the corporate number as a string (leading digits significant) and the
  data join key for every Japan source.
