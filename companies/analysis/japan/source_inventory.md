# Japan — Source Inventory

| Source | Org | Type | Access | Formats | License | Status |
|---|---|---|---|---|---|---|
| NTA Corporate Number Publication (法人番号公表サイト) | National Tax Agency | official registry | public, no auth | CSV (SJIS/Unicode), XML, ZIP; Web-API | free use (public data) | **recommended** |
| EDINET (XBRL financials) | Financial Services Agency | financial disclosure | free Subscription-Key | JSON, XBRL, ZIP, CSV, PDF | public disclosure | blocked_by_authentication (free key) |
| gBizINFO (Gビズインフォ) | METI | gov aggregator | free token | JSON, CSV | gov standard terms (≈ CC-BY) | blocked_by_authentication (free token) |
| Registry Information Service (登記情報提供サービス) | Ministry of Justice | official registry | paid per-record | PDF/view | restricted | blocked_by_payment |

## Roles

- **nta_houjin_bangou** — authoritative open **identity** layer: 13-digit
  corporate number, name (JP/EN/kana), address, corporate kind, registry-closure
  status, assignment date. Full national/per-prefecture bulk + daily diffs.
  Verified live (Tottori, 20,153 rows, 30 cols). No financials/capital/officers.
- **edinet_xbrl** — authoritative **financials** for listed & disclosure-obligated
  companies (XBRL securities reports), joined on the corporate number. Free key.
- **gbizinfo** — **enrichment**: capital, employees, business summary, financial
  info, procurement, subsidies, certifications, patents. Free token.
- **houki_toukibo** — paid full commercial register (officers, capital, purpose,
  history). Documentation only; not fetched.

## Join key

13-digit **corporate number (法人番号)** across NTA, EDINET (JCN), and gBizINFO.
It is also the corporate tax id; Japan has no separate VAT number.
