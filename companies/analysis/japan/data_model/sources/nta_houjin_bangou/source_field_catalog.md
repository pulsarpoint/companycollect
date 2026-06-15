# NTA Corporate Number Publication (法人番号公表サイト) Field Catalog

## Source Summary

- Country: Japan
- Source type: official_registry
- Organization: National Tax Agency (国税庁)
- URL: https://www.houjin-bangou.nta.go.jp/download/zenken/
- License: free use (public data) — corporate number/name/address freely usable
- Access: public (bulk download, no auth; Web-API needs a free application ID)
- Freshness: monthly full file + daily diff
- Record shape: flat CSV, 30 columns, one row per history entry (filter `latest=1`)
- Primary keys: `corporateNumber`
- Join keys: `corporateNumber`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| col2.corporateNumber | 法人番号 | 13-digit corporate number (= tax id) | string | identifier | 1010001119515 | Join key; keep as string |
| col3.process | 処理区分 | Record event type | string | metadata | 01, 12 | new/name/address/closure codes |
| col5.updateDate | 更新年月日 | System update date | date | date | 2024-02-28 | YYYY-MM-DD |
| col6.changeDate | 変更年月日 | Effective change date | date | date | 2024-02-21 | |
| col7.name | 商号又は名称 | Legal name (JP) | string | legal_name | 株式会社ウッドプラスチックテクノロジー | full-width chars |
| col9.kind | 法人種別 | Entity type | string | legal_form | 301 | 101/201/301/401/499 |
| col10–12 | 国内所在地 | Prefecture / city / street | string | address | 鳥取県 / 鳥取市 / 谷６０５－３３ | empty for overseas |
| col14.prefectureCode | 都道府県コード | JIS prefecture code | string | geography | 31 | |
| col15.cityCode | 市区町村コード | Municipality code | string | geography | 201 | |
| col16.postCode | 郵便番号 | 7-digit postal code | string | address | 6820954 | |
| col19.closeDate | 登記記録の閉鎖等年月日 | Registry closure date | date | date | (empty=active) | drives status |
| col20.closeCause | 登記記録の閉鎖等の事由 | Closure reason | string | status | | 01/11/21/31 |
| col21.successorCorporateNumber | 承継先法人番号 | Successor corp number | string | relationship | | mergers |
| col23.assignmentDate | 法人番号指定年月日 | Number-assignment date | date | date | 2015-10-05 | NOT incorporation date |
| col24.latest | 最新履歴 | Latest-history flag | string | metadata | 1 | filter latest=1 |
| col25.enName | 英語表記 | English name (opt-in) | string | legal_name | Tottori Summary Court | mostly empty |
| col29.furigana | フリガナ | Katakana reading | string | legal_name | サウンドエフ | phonetic search |
| col30.hihyoji | 検索対象除外 | Search-excluded flag | string | metadata | 0 | 0 searchable |

(Full 30-column list with every field is in `source_field_catalog.json`.)

## Interpretation Notes

- **Verified from real data**: Tottori prefecture Unicode CSV
  (`31_tottori_all_20260529.csv`, 20,153 rows, 30 columns), downloaded
  2026-06-15 via the public download form (no auth/payment).
- **Corporate number (法人番号)** is 13 digits (12-digit base + check digit) and is
  simultaneously the **company id** and the **corporate taxpayer number**. Japan
  has **no separate VAT number** (the invoice-system registration number is `T` +
  the 13-digit number). Keep as a string.
- **History rows**: a corporate number can appear multiple times (name/address
  changes). Keep `latest=1` for the current state; the others are history.
- **Status**: `closeDate` present ⇒ closed (use `closeCause` for the reason),
  otherwise active.
- **assignmentDate is not incorporation**: mass assignment began 2015-10-05, so
  that date dominates and must not be read as the founding date. True
  incorporation/establishment dates are not in this dataset (use gBizINFO or the
  paid registry).
- **No financials, capital, officers, or industry code** in this source — identity
  + address + status only.
- **Encoding**: Unicode file is UTF-8 (a parallel Shift-JIS file exists); each CSV
  ships with an `.asc` signature file. Addresses use full-width digits.
- **Corporate identity is not personal data** (entities, not individuals).
