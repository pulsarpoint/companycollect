# Japan — Search Attempts

## Attempt 1

- Date/time: 2026-06-15
- Source: NTA Corporate Number Publication Site (法人番号公表サイト)
- Query / URL: https://www.houjin-bangou.nta.go.jp/ and /download/zenken/
- Language: Japanese
- Why: National corporate-number register is Japan's authoritative open company id source.
- Result: HTTP 200. Download page lists national + 47-prefecture + overseas files in three formats (CSV Shift-JIS, CSV Unicode, XML), each via a form POST (CSRF token + selDlFileNo).
- Decision: RECOMMENDED. Authoritative, fully open identity bulk.

## Attempt 2

- Date/time: 2026-06-15
- Source: NTA bulk download (Tottori prefecture, Unicode CSV, file no 27306)
- Query / URL: POST https://www.houjin-bangou.nta.go.jp/download/zenken/index.html
- Language: Japanese
- Why: Verify the download works and capture the real schema.
- Result: HTTP 200, `31_tottori_all_20260529.zip` (886 KB) → CSV 20,153 rows, 30 columns. Saved.
- Decision: Confirmed working. Used as the real sample.

## Attempt 3

- Date/time: 2026-06-15
- Source: EDINET (FSA) API v2
- Query / URL: https://api.edinet-fsa.go.jp/api/v2/documents.json?date=2024-01-04&type=1
- Language: Japanese/English
- Why: Authoritative XBRL financial filings for listed/disclosure-obligated companies.
- Result: HTTP 200 body `{"StatusCode":401,...invalid subscription key}`. v1 endpoint → HTTP 403 (retired).
- Decision: blocked_by_authentication (free Subscription-Key required). Catalog financials.

## Attempt 4

- Date/time: 2026-06-15
- Source: gBizINFO (METI) REST API
- Query / URL: https://info.gbiz.go.jp/hojin/v1/hojin?name=トヨタ
- Language: Japanese
- Why: Official aggregator with financial info + procurement + subsidies keyed on corporate number.
- Result: HTTP 500 without a token. API doc (content.info.gbiz.go.jp/api/index.html) confirms 利用申請 → APIトークン and lists 財務情報 among datasets.
- Decision: blocked_by_authentication (free token). Catalog as enrichment.

## Attempt 5

- Date/time: 2026-06-15
- Source: Legal Affairs Bureau Registry Information Service (登記情報提供サービス)
- Query / URL: https://www1.touki.or.jp/ (documentation only)
- Language: Japanese
- Why: Only source of officers, capital, purpose, full history.
- Result: Pay-per-record service; no open bulk/API. Not fetched.
- Decision: blocked_by_payment. Cataloged from public docs only.
