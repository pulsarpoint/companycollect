# Vietnam — Schema Notes

## Identifiers

- **Mã số doanh nghiệp (MSDN)** — enterprise code; **identical to the tax code
  (mã số thuế, MST)**. 10 digits for the head entity, or 13 (10 + `-` + 3-digit
  branch/unit suffix). The company id and universal join key.
- **VAT**: Vietnam has **no separate VAT number** — the tax code serves VAT.
- **VSIC** — Vietnam Standard Industrial Classification (ngành nghề kinh doanh),
  the business-line/activity code.

## NBRP per-company search (fields shown on the public result)

| Field (VI) | Meaning |
|---|---|
| Tên doanh nghiệp | Company name (Vietnamese; sometimes an English/abbrev name) |
| Mã số doanh nghiệp | Enterprise code = tax code (10–13 digits) |
| Địa chỉ trụ sở | Head-office address |
| Ngành nghề kinh doanh | Business lines (VSIC codes + text) |
| Người đại diện theo pháp luật | Legal representative (PERSONAL DATA) |
| Tình trạng hoạt động | Legal status (Đang hoạt động = active; Đã giải thể = dissolved; Tạm ngừng = suspended) |
| Ngày cấp / thành lập | Issue / establishment date |
| Loại hình | Company type (TNHH = LLC, CP = joint-stock, DNTN = sole proprietorship, …) |

No open API/bulk — these are read from the gated per-company search.

## Listed-company financials (HOSE/HNX/SSC)

Per issuer: balance sheet (bảng cân đối kế toán), income statement (kết quả kinh
doanh), cash flow (lưu chuyển tiền tệ), notes. PDF/XLS; VND. Listed companies only.

## Mapping to internal model

| Internal | Vietnam source |
|---|---|
| company_id | MSDN (= tax code) |
| registration_number | MSDN |
| tax_id | MSDN (same) |
| vat_id | not_available (no separate VAT number; tax code serves VAT) |
| legal_name | NBRP Tên doanh nghiệp |
| company_type / legal_form | NBRP Loại hình (TNHH/CP/DNTN/…) |
| status | NBRP Tình trạng hoạt động (map active/suspended/dissolved) |
| incorporation_date | NBRP Ngày thành lập |
| dissolution_date | implied by status (Đã giải thể) |
| registered_address | NBRP Địa chỉ trụ sở |
| activity_code | NBRP Ngành nghề (VSIC) |
| financials | HOSE/HNX/SSC (listed only); non-listed not_available |
| officers / legal representative | NBRP Người đại diện (PII; redact) |
| owners | not_available openly |

## Gotchas

- **Vietnamese-only**, UTF-8 with diacritics. **No open bulk/API** — register and
  tax lookup are CAPTCHA-gated; financials listed-only.
- Tax code = enterprise code (one number for register + tax + VAT).
- Legal-representative name is personal data — redact.
- The committed normalized sample is **schematic** (no real per-company record
  lawfully bulk-downloadable).
