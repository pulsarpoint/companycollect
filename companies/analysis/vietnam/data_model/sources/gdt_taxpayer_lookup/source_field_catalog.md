# GDT Taxpayer Lookup Field Catalog

> **DOCUMENTED-ONLY / GATED.** Per-company tax-code lookup, **CAPTCHA-gated**, no
> open API/bulk. Cataloged from the public UI; no values retrieved; CAPTCHA not
> bypassed. No `sample_record.json`.

## Source Summary

- Country: Vietnam
- Source type: official_tax
- Organization: Tổng cục Thuế (General Department of Taxation)
- URL: https://tracuunnt.gdt.gov.vn/tcnnt/mstdn.jsp
- License: restricted/unclear
- Access: public per-company (CAPTCHA-gated)
- Freshness: real-time
- Record shape: HTML per-company result
- Primary keys: `ma_so_thue` (= enterprise code)
- Join keys: `ma_so_thue`

## Fields

| Path (VI) | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| Mã số thuế | ma_so_thue | Tax code | string | identifier | = NBRP enterprise code |
| Tên người nộp thuế | ten | Taxpayer name | string | legal_name | |
| Địa chỉ | dia_chi | Address | string | address | |
| Cơ quan thuế | co_quan_thue | Managing tax office | string | metadata | |
| Tình trạng | tinh_trang | Tax status | string | status | active/closed/suspended |

## Interpretation Notes

- Confirms a **known company's tax registration + status** by tax code (which
  equals the NBRP enterprise code). **CAPTCHA-gated; no bulk/API** — use only for
  per-company verification; do not bypass the CAPTCHA. Complements NBRP identity.
