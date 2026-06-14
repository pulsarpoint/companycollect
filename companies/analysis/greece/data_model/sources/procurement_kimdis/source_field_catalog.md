# KIMDIS / Promitheus — public procurement (ΚΗΜΔΗΣ/ΕΣΗΔΗΣ) Field Catalog

## Source Summary

- Country: Greece
- Source type: public_procurement
- Organization: Greek Government — Εθνικό Σύστημα Ηλεκτρονικών Δημοσίων Συμβάσεων
- URL: https://www.eprocurement.gov.gr/
- License: open (per terms)
- Access: public
- Freshness: continuous
- Record shape: procurement notices and awards
- Primary keys: none
- Join keys: `afm`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| supplier.afm | ΑΦΜ αναδόχου | Supplier tax id | string | identifier | — | cross-ref key |
| supplier.name | επωνυμία αναδόχου | Supplier name | string | legal_name | — | cross-check |
| award.amount | αξία σύμβασης | Contract amount (EUR) | decimal | financial | — | contract-level |

## Interpretation Notes

- **Open cross-reference, not a company master.** ΚΗΜΔΗΣ/ΕΣΗΔΗΣ publishes contract notices/awards with the
  supplier's **ΑΦΜ + name**. Useful to corroborate **ΑΦΜ↔name** and flag entities active in public procurement.
  Join on ΑΦΜ. Contract amounts are not company financials.
