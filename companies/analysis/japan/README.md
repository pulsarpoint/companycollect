# Company data sources for Japan

## Status

- Official bulk data: **found** (NTA Corporate Number Publication — full national bulk CSV/XML, fully open)
- Official API: **found** (NTA Web-API; gBizINFO REST API; EDINET API v2 — last two need a free key/token)
- Open data portal: **found** (gBizINFO / METI; e-Stat; data.go.jp)
- License: **known** for NTA (freely usable public data); gBizINFO 利用規約 (gov standard terms ≈ CC-BY); EDINET public disclosure
- Recommended ingestion path: **bulk** (NTA for identity) + **API** (EDINET for financials, gBizINFO for enrichment — both behind a free key)

## Best source

**NTA Corporate Number Publication Site (法人番号公表サイト, houjin-bangou.nta.go.jp)**
— run by the National Tax Agency. Publishes the **13-digit Corporate Number
(法人番号)** for every registered corporation in Japan (~5M entities) as a fully
open national/per-prefecture bulk download (CSV Shift-JIS, CSV Unicode, XML) plus
a Web-API. The published basic information (corporate number, name, address) is
designated **freely usable by anyone** (no application needed for the data). This
is identity only — it has **no financials, no capital, no officers, no industry**.

Verified live: downloaded the Tottori prefecture file
(`31_tottori_all_20260529.zip`, 2026-05-29), 20,153 corporations, 30 columns.

## Financial data

- **EDINET (FSA, disclosure.edinet-fsa.go.jp / api.edinet-fsa.go.jp v2)** —
  XBRL financial statements from securities reports (有価証券報告書) for listed and
  disclosure-obligated companies. Public disclosure; the **API v2 requires a free
  Subscription-Key** (v1 retired, HTTP 403). This is the authoritative financial
  source for listed/large filers.
- **gBizINFO (METI, info.gbiz.go.jp)** — REST API aggregating basic corporate
  info **plus 財務情報 (financial info)**, procurement, subsidies, certifications,
  patents, keyed on the corporate number. **Requires a free API token** (利用申請).

Financials for **non-listed** companies are not openly published; the full
commercial register (officers, capital) is at the Legal Affairs Bureau
(登記情報提供サービス) on a **paid per-record** basis.

## Next action

Implement NTA bulk ingestion (identity, fully open), then add EDINET XBRL
(financials, free key) and gBizINFO (enrichment, free token), all joined on the
13-digit corporate number.
