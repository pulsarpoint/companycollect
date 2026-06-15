# EDINET (FSA XBRL Financials) Field Catalog

> **PLANNING-ONLY for field values.** EDINET API v2 `documents.json` returns
> HTTP **401 "invalid subscription key"** without a free Subscription-Key
> (registration with the FSA); v1 is retired (403). The document/field structure
> below is from the public EDINET API specification — no records were fetched
> without a key. The disclosed content itself is public.

## Source Summary

- Country: Japan
- Source type: financial_disclosure
- Organization: Financial Services Agency (金融庁)
- URL: https://api.edinet-fsa.go.jp/api/v2/
- License: public disclosure (EDINET terms); API needs a free Subscription-Key
- Access: public with a free key
- Freshness: daily (filing-driven)
- Record shape: documents list (JSON) + per-document XBRL/ZIP
- Primary keys: `docID`
- Join keys: `JCN` (corporate number), `edinetCode`, `secCode`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| results[].docID | docID | Filing document id | string | identifier | fetch XBRL via /documents/{docID}?type=1 |
| results[].edinetCode | edinetCode | 6-char filer code | string | identifier | E##### |
| results[].secCode | secCode | Securities code | string | identifier | listed only |
| results[].JCN | JCN | 13-digit corporate number | string | identifier | **join key to NTA** |
| results[].filerName | filerName | Filer name | string | legal_name | |
| results[].docTypeCode | docTypeCode | Document type | string | document | 120 annual / 140 quarterly / 160 semi-annual |
| results[].periodStart/End | periodStart/End | Fiscal period | date | date | financial-year window |
| results[].submitDateTime | submitDateTime | Submission time | datetime | date | |
| xbrl.NetSales | 売上高 | Revenue (JPY) | decimal | financial | from XBRL instance |
| xbrl.OperatingIncome | 営業利益 | Operating income | decimal | financial | |
| xbrl.NetIncome | 当期純利益 | Net income | decimal | financial | |
| xbrl.TotalAssets | 総資産 | Total assets | decimal | financial | + NetAssets, equity |

## Interpretation Notes

- **Access**: API v2 requires `Subscription-Key=<freeKey>` on every request.
  Two-step flow: `GET /documents.json?date=YYYY-MM-DD&type=2` to list filings for
  a day, then `GET /documents/{docID}?type=1` to download the XBRL ZIP.
- **Coverage**: listed companies and other disclosure-obligated filers
  (有価証券報告書 issuers). **Non-listed SMEs are not in EDINET.**
- **Join**: the `JCN` field is the 13-digit corporate number → join to NTA and
  gBizINFO. `secCode` links to the stock ticker for listed issuers.
- **Financial facts**: live inside the XBRL instance, tagged with the EDINET/
  JP-GAAP or IFRS taxonomy. Element names differ between JP-GAAP and IFRS filers,
  so a taxonomy-aware extractor is needed. Currency is **JPY**.
- **Document types**: 120 = annual securities report (full financials); 140
  quarterly; 160 semi-annual. Pick 120 for annual statements.
- No raw sample record is included (key-gated source).
