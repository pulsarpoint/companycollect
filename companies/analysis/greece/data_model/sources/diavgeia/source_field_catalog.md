# Diavgeia (Δι@ύγεια) — government transparency programme Field Catalog

## Source Summary

- Country: Greece
- Source type: transparency_register
- Organization: Greek Government (Υπουργείο Ψηφιακής Διακυβέρνησης)
- URL: https://diavgeia.gov.gr/opendata/ (REST)
- License: open
- Access: public
- Freshness: continuous
- Record shape: REST API of decision acts
- Primary keys: `ada` (act id)
- Join keys: `afm`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| afm | afm | Payee/contractor ΑΦΜ | string | identifier | — | cross-ref key |
| subject / organization | subject | Act subject + issuer | string | relationship | — | public-sector link |
| amount | amount | Amount (EUR) | decimal | financial | — | act-level |

## Interpretation Notes

- **Open cross-reference, not a company master.** Diavgeia mandates publication of government decisions/spending;
  its open REST API exposes acts that often reference a company **ΑΦΜ + name** (e.g. expenditure/award
  decisions). Useful to corroborate **ΑΦΜ↔name** and public-sector relationships for entities that transact with
  the state. Open under the programme's terms. Join on ΑΦΜ.
