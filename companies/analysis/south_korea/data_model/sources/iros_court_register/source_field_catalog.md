# IROS — Supreme Court Commercial Registry (등기) Field Catalog

> **PLANNING-ONLY / PAID.** The full commercial register run by the Supreme Court
> (대법원 인터넷등기소). Registry extracts (등기사항증명서) are **fee-based per
> issue**; no open bulk or API. Cataloged from public documentation only — no
> records fetched, no values copied. It is the only complete source of **unlisted
> companies, full legal form, capital, and directors**.

## Source Summary

- Country: South Korea
- Source type: official_registry
- Organization: Supreme Court of Korea (대법원 인터넷등기소)
- URL: https://www.iros.go.kr/
- License: restricted (paid)
- Access: paid per-document (fee per registry extract)
- Freshness: live register
- Record shape: per-company registry certificate (등기사항증명서)
- Primary keys: `corp_registration_number` (법인등록번호, 13-digit)
- Join keys: `corp_registration_number`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| registry.corp_registration_number | 법인등록번호 | 13-digit corp reg no | string | identifier | = OpenDART jurir_no |
| registry.company_name | 상호 | Trade name | string | legal_name | paid |
| registry.company_type | 종류 | Legal form | string | legal_form | 주식회사/유한회사/… |
| registry.head_office | 본점 | Head office | string | address | paid |
| registry.capital | 자본금의 액 | Registered capital (KRW) | integer | financial | paid |
| registry.directors | 임원에 관한 사항 | Directors/officers | array | person | **PERSONAL DATA (PIPA)** |
| registry.establishment_date | 회사성립연월일 | Incorporation date | date | date | authoritative |

## Interpretation Notes

- The **법인등록번호** (13-digit corporate registration number) is the join key
  (= OpenDART `jurir_no`).
- This register is the **only** complete source of **unlisted companies** and the
  authoritative **legal form, capital, and full director list** — none of which are
  fully in the open OpenDART data (OpenDART covers only DART-registered companies
  and exposes only the CEO).
- **Director records are personal data** under **PIPA** and must be redacted in
  any committed output.
- **Access**: fee per registry extract; no bulk/API. Keep planning-only; verify
  terms before any use. No raw sample record.
