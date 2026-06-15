# NBRP — Per-Company Search Field Catalog

> **DOCUMENTED-ONLY / GATED.** The authoritative register, but the per-company
> search is **CAPTCHA-gated** and Vietnamese-only, with **no open API or bulk**.
> Fields are described from the public search UI; **no values were retrieved** and
> the CAPTCHA was not bypassed. No `sample_record.json`.

## Source Summary

- Country: Vietnam
- Source type: official_registry
- Organization: Business Registration Authority, Ministry of Planning and Investment (MPI)
- URL: https://dangkykinhdoanh.gov.vn/
- License: restricted/unclear (no open re-use terms)
- Access: public per-company search (CAPTCHA-gated on submit)
- Freshness: real-time
- Record shape: HTML per-company result
- Primary keys: `ma_so_doanh_nghiep` (enterprise code = tax code)
- Join keys: `ma_so_doanh_nghiep`

## Fields (from the public search UI)

| Path (VI) | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| Mã số doanh nghiệp | ma_so_doanh_nghiep | Enterprise code = tax code | string | identifier | id + join key (10–13 digits) |
| Tên doanh nghiệp | ten_doanh_nghiep | Name | string | legal_name | Vietnamese |
| Loại hình | loai_hinh | Legal form | string | legal_form | TNHH/CP/DNTN |
| Tình trạng hoạt động | tinh_trang | Status | string | status | active/suspended/dissolved |
| Ngày thành lập | ngay_thanh_lap | Establishment date | date | date | |
| Địa chỉ trụ sở | dia_chi_tru_so | Head-office address | string | address | |
| Ngành nghề kinh doanh | nganh_nghe | Business lines | array | activity | VSIC |
| Người đại diện theo pháp luật | nguoi_dai_dien | Legal representative | string | person | **PII — redact** |

## Interpretation Notes

- **The authoritative identity source**, but **gated**: free to view one company
  at a time after solving a CAPTCHA; **no open bulk/API**. Full coverage needs the
  paid MOU (see `nbrp_bulk_mou`).
- **Enterprise code = tax code** (one 10–13-digit number for register + tax + VAT)
  — the universal join key. Vietnam has **no separate VAT number**.
- **Legal representative** is the only open analogue to officers — **personal
  data**, redact. Shareholders/beneficial owners are **not** shown.
- Vietnamese-only; UTF-8 diacritics. Do **not** bypass the CAPTCHA or run
  automated searches.
