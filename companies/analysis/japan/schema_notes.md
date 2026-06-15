# Japan — Schema Notes

## Identifiers

- **法人番号 (Corporate Number)** — **13 digits** (12-digit base + 1 leading check
  digit). Assigned by the NTA to every registered corporation and government body.
  It is simultaneously the **company id** and the **corporate taxpayer number**.
- **No separate VAT number.** The Qualified Invoice System (インボイス制度)
  registration number is simply `T` + the 13-digit corporate number.
- **EDINET code** — 6-char code (e.g. `E01234`) identifying a filer in EDINET;
  filer metadata also exposes the **JCN (corporate number)** and, for listed
  issuers, the **securities code (証券コード)**.
- Join key across all sources: the **13-digit corporate number**.

## NTA bulk CSV layout (30 columns, verified from Tottori 2026-05-29)

| # | Field | Meaning |
|---|---|---|
| 1 | sequenceNumber 一連番号 | Row sequence |
| 2 | corporateNumber 法人番号 | 13-digit corporate number (key) |
| 3 | process 処理区分 | 01 new / 11 name change / 12 address change / 21 closure / 71/81/99 etc. |
| 4 | correct 訂正区分 | Correction flag (0/1) |
| 5 | updateDate 更新年月日 | Record update date |
| 6 | changeDate 変更年月日 | Effective change date |
| 7 | name 商号又は名称 | Legal name (Japanese) |
| 8 | nameImageId | Name image id (for chars not encodable) |
| 9 | kind 法人種別 | 101 national agency / 201 local public body / 301 registered corporation / 401 foreign company etc. / 499 other |
| 10 | prefectureName 都道府県 | Address: prefecture |
| 11 | cityName 市区町村 | Address: city/ward/town |
| 12 | streetNumber 丁目番地等 | Address: street/block |
| 13 | addressImageId | Address image id |
| 14 | prefectureCode 都道府県コード | JIS prefecture code (2-digit) |
| 15 | cityCode 市区町村コード | Municipality code (3-digit) |
| 16 | postCode 郵便番号 | 7-digit postal code |
| 17 | addressOutside 国外所在地 | Overseas address (foreign cos) |
| 18 | addressOutsideImageId | Overseas address image id |
| 19 | closeDate 登記記録の閉鎖等年月日 | Registry closure date (→ status) |
| 20 | closeCause 登記記録の閉鎖等の事由 | 01 liquidation / 11 merger dissolution / 21 closed by registrar / 31 other |
| 21 | successorCorporateNumber 承継先法人番号 | Successor corp number (mergers) |
| 22 | changeCause 変更事由の詳細 | Change cause detail |
| 23 | assignmentDate 法人番号指定年月日 | Date the corporate number was assigned |
| 24 | latest 最新履歴 | Latest-history flag (1 = current) |
| 25 | enName 英語表記 | Legal name (English, if registered) |
| 26 | enPrefectureName | Prefecture (English) |
| 27 | enCityName | City + street (English) |
| 28 | enAddressOutside | Overseas address (English) |
| 29 | furigana フリガナ | Name reading (katakana) |
| 30 | hihyoji 検索対象除外 | Search-exclusion flag (0 = searchable) |

### Status derivation

- `status = closed` when `closeDate` is present (use `closeCause` for the
  reason); otherwise `active`.
- Use `latest = 1` to keep only the current record per corporate number (the file
  may contain historical rows per number).

### Notes

- Encoding: Unicode file is UTF-8; a parallel Shift-JIS file exists. Addresses use
  full-width digits (e.g. `２２３`). An `.asc` signature accompanies each CSV.
- Dates are `YYYY-MM-DD`.

## EDINET (financials) — fields

XBRL facts from securities reports: filer name, EDINET code, securities code, JCN
(corporate number), document type, fiscal period start/end, submit datetime, and
the financial facts themselves (net sales, operating income, net income, total
assets, net assets, etc.) under the Japanese GAAP / IFRS XBRL taxonomy. Currency
JPY. Access: free Subscription-Key.

## gBizINFO — fields

corporate_number, name, location, capital (資本金), employee_number (従業員数),
founding/establishment date, business_summary, business item codes, financial_info
(財務情報), procurement (調達), subsidy (補助金), certification, patent. Access:
free token. Currency JPY.

## Internal model mapping

```text
company_id          <- corporateNumber (NTA, 13-digit)
registration_number <- corporateNumber
tax_id              <- corporateNumber (same value)
vat_id              <- null (no separate VAT; invoice no = "T"+corporateNumber)
legal_name          <- name (NTA)
legal_name_en       <- enName (NTA, if present)
legal_name_kana     <- furigana (NTA)
company_type        <- kind (NTA code list)
status              <- closeDate present ? closed : active
registered_address  <- prefectureName + cityName + streetNumber (NTA)
incorporation_date  <- not in NTA open data (assignmentDate ≈ number-assignment, not incorporation); establishment date via gBizINFO/registry
capital             <- gBizINFO (resOblig) / paid registry
financials          <- EDINET XBRL (listed/obligated) / gBizINFO financial_info
officers            <- not open (paid Legal Affairs Bureau registry; APPI personal data)
```
