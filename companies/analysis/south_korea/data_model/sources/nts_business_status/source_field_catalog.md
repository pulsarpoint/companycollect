# NTS Business Registration Status API Field Catalog

> **PLANNING-ONLY for field values.** The NTS business-status API (via data.go.kr /
> odcloud) returns HTTP **401** `인증키는 필수 항목 입니다` ("authentication key is
> required") without a free service key. Schema from the public data.go.kr docs —
> no records fetched without a key.

## Source Summary

- Country: South Korea
- Source type: tax_registry
- Organization: National Tax Service (국세청) via data.go.kr / odcloud
- URL: https://api.odcloud.kr/api/nts-businessman/v1/status
- License: KOGL (Korea Open Government Licence) / dataset terms
- Access: public with a free service key (data.go.kr)
- Freshness: live lookup
- Record shape: JSON `data[]` keyed by `b_no`
- Primary keys: `b_no`
- Join keys: `b_no`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| data[].b_no | b_no | 사업자등록번호 (10-digit) | string | identifier | tax id; join to OpenDART bizr_no |
| data[].b_stt | b_stt | Business status (text) | string | status | 계속/휴업/폐업 |
| data[].b_stt_cd | b_stt_cd | Status code | string | status | 01/02/03 |
| data[].tax_type | tax_type | VAT taxpayer type | string | license_or_terms | general/simplified/exempt |
| data[].end_dt | end_dt | Closure date | date | date | YYYYMMDD |
| data[].utcc_yn | utcc_yn | Unit-taxation flag | string | metadata | Y/N |

## Interpretation Notes

- A **lookup/validation** endpoint, not a company master: POST a list of business
  registration numbers (`b_no`) and get their **tax-registration status**
  (active/suspended/closed) and VAT taxpayer type.
- **Join**: `b_no` = OpenDART `bizr_no` = the business registration number (= tax
  id = VAT number). This refreshes the live operating status that OpenDART does not
  carry.
- Free **data.go.kr service key** required. The business registration number is a
  corporate identifier (not personal data), but handle carefully.
- No raw sample record (key-gated).
