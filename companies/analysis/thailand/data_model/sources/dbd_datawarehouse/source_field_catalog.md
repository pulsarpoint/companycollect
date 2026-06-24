# DBD DataWarehouse — Financial Statements Field Catalog (PLANNING-ONLY)

## Source Summary

- Country: Thailand
- Source type: financial_statements
- Organization: Department of Business Development (DBD)
- URL: https://datawarehouse.dbd.go.th/
- License: not stated (login required)
- Access: **login/session-gated** (302/403 for automation)
- Freshness: annual
- Record shape: per-company, per-year financial statement (login-gated)
- Primary keys: juristic_id + financial_year
- Join keys: juristic_id

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| juristic_id | เลขทะเบียนนิติบุคคล | 13-digit juristic id | string | identifier |  | join key |
| financial_year | ปีงบการเงิน | Financial year | string | date |  | planning-only |
| total_assets | สินทรัพย์รวม | Total assets | decimal | financial |  | THB |
| total_liabilities | หนี้สินรวม | Total liabilities | decimal | financial |  | THB |
| shareholder_equity | ส่วนของผู้ถือหุ้น | Shareholders' equity | decimal | financial |  | THB |
| total_revenue | รายได้รวม | Total revenue | decimal | financial |  | THB |
| net_profit | กำไร(ขาดทุน)สุทธิ | Net profit (loss) | decimal | financial |  | THB |
| financial_ratios | อัตราส่วนทางการเงิน | Financial ratios | object | financial |  | ROA/ROE/etc. |

## Interpretation Notes

- The **DBD DataWarehouse** is the richest **financial** source: all juristic
  persons file annual **financial statements** (balance sheet + income statement,
  THB), and DBD exposes them per company per year, plus computed **ratios**.
- **Access is login/session-gated** — automated/API requests returned **302/403**.
  So all fields here are **PLANNING-ONLY**, documented from public descriptions of
  the standard Thai financial-statement layout; **no raw values copied**.
- **Join key** is the **13-digit juristic ID**, attaching financials to the DBD
  OpenAPI identity. Currency **THB**.
- Implementation is blocked on **authentication** (login); treat as a future gated
  enrichment.
