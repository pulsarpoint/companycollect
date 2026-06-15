# HOSE / HNX / SSC — Listed-Company Financials Field Catalog

> **DOCUMENTED-ONLY / LISTED ISSUERS ONLY.** The only open route to Vietnamese
> financials — but only the **listed** population. Cataloged from public docs; no
> records retrieved.

## Source Summary

- Country: Vietnam
- Source type: stock_exchange
- Organization: HOSE, HNX, State Securities Commission (SSC)
- URL: https://congbothongtin.ssc.gov.vn/ ; https://www.hsx.vn/ ; https://www.hnx.vn/
- License: issuer disclosure (open to view; redistribution per exchange/SSC terms)
- Access: public (per-issuer)
- Freshness: annual/quarterly
- Record shape: per-issuer disclosure documents (PDF/XLS)
- Primary keys: `ma_so_thue` + `ticker`
- Join keys: `ma_so_thue`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| ticker | mã chứng khoán | Stock ticker | string | identifier | listed only |
| bang_can_doi_ke_toan | Bảng cân đối kế toán | Balance sheet | object | financial | VND; VAS |
| ket_qua_kinh_doanh | Kết quả kinh doanh | Income statement | object | financial | VND |
| luu_chuyen_tien_te | Lưu chuyển tiền tệ | Cash flow | object | financial | VND |
| bao_cao_thuong_nien | Báo cáo thường niên | Annual report + disclosures | array | document | listed only |

## Interpretation Notes

- The **only open financials** in Vietnam, but **listed issuers only** (a few
  hundred on HOSE/HNX/UPCoM). Per-issuer PDFs/XLS to the **VAS** (Vietnamese
  Accounting Standards), in **VND**; **no clean open bulk API**. Link the
  **ticker** to the **enterprise code (= tax code)** by issuer to join with NBRP.
- **Non-listed companies' financials are not published** — a structural gap for
  the vast majority of Vietnamese companies.
