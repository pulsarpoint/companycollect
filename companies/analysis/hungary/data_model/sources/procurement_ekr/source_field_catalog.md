# EKR / Közbeszerzés — public procurement (suppliers) Field Catalog

## Source Summary

- Country: Hungary
- Source type: public_procurement
- Organization: Közbeszerzési Hatóság / Elektronikus Közbeszerzési Rendszer (EKR)
- URL: https://ekr.gov.hu/
- License: public (per terms)
- Access: public
- Freshness: continuous
- Record shape: procurement notices and awards
- Primary keys: none
- Join keys: `adoszam`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| supplier.adoszam | nyertes adószáma | Supplier tax id | string | identifier | — | cross-ref key |
| supplier.name | nyertes neve | Supplier name | string | legal_name | — | cross-check |
| award.amount | szerződés értéke | Contract amount (HUF) | decimal | financial | — | contract-level |

## Interpretation Notes

- **Open cross-reference, not a company master.** EKR and the Közbeszerzési Értesítő publish tenders/awards with
  the supplier's **adószám + name**. Useful to corroborate **adószám↔name** and flag entities active in public
  procurement. Join on adószám. Contract amounts are not company financials.
