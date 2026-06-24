# Central Registry — Registry of Annual Accounts Field Catalog (PLANNING-ONLY)

## Source Summary

- Country: North Macedonia
- Source type: financial_statements
- Organization: Централен регистар на РСМ (Central Registry, CRM)
- URL: https://www.crm.com.mk/
- License: commercial distribution by the registry
- Access: **paid** (CRM distribution)
- Freshness: annual
- Record shape: per-company paid annual account (planning-only)
- Primary keys: ЕМБС
- Join keys: ЕМБС

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| embs | ЕМБС | Entity registration number | string | identifier |  | join key |
| delovna_godina | Деловна година | Fiscal year | string | date |  | planning-only |
| bilans_sostojba.aktiva | Биланс на состојба — Актива | Total assets | decimal | financial |  | MKD |
| bilans_sostojba.kapital | Биланс на состојба — Капитал | Equity | decimal | financial |  | MKD |
| bilans_uspeh.vkupni_prihodi | Биланс на успех — Вкупни приходи | Total revenue | decimal | financial |  | MKD |
| bilans_uspeh.neto_dobivka | Биланс на успех — Нето добивка/загуба | Net result | decimal | financial |  | MKD |
| broj_vraboteni | Број на вработени | Employees | integer | employment |  | planning-only |

## Interpretation Notes

- All companies file **annual accounts (годишна сметка)** with the CRM's **Registry
  of Annual Accounts**: **Биланс на состојба** (balance sheet) and **Биланс на
  успех** (income statement), in **MKD**.
- Access is via the CRM's **paid distribution** service — there is **no open bulk
  financial dataset**, so all fields here are **PLANNING-ONLY**, derived from public
  descriptions of the standard MK financial-statement forms; **no raw values copied**.
- **Join key** is **ЕМБС**, attaching financials to the Trade Registry identity.
- Implementation is **blocked on payment**; treat as a future paid enrichment.
- Currency **MKD**.
