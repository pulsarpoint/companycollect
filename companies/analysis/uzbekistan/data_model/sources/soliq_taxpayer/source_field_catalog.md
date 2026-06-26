# State Tax Committee (soliq.uz) — taxpayer search Field Catalog

## Source Summary

- Country: Uzbekistan
- Source type: tax_register
- Organization: State Tax Committee of the Republic of Uzbekistan (soliq.uz)
- URL: https://soliq.uz/
- License: restricted
- Access: **firewalled from this environment** (soliq.uz timed out)
- Freshness: live
- Record shape: per-STIR search result (planning-only)
- Primary keys: stir_inn
- Join keys: stir_inn, taxpayer_name

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| stir_inn | СТИР/ИНН | 9-digit taxpayer id | string | identifier |  | **join key to EGRPO** |
| taxpayer_name | Солик туловчи номи | Taxpayer name | string | legal_name |  | individuals = personal data |
| vat_status | ҚҚС рўйхати | VAT (QQS) status | string | status |  | ҚҚС = VAT |
| taxpayer_status | Солик туловчи холати | Taxpayer status | string | status |  | tax status, not registration |

## Interpretation Notes

- The **State Tax Committee** (`soliq.uz`) provides **taxpayer search by STIR/INN** and a
  **VAT-payers registry**. From this environment `soliq.uz` **timed out (firewalled)** — not
  reachable. All fields here are **planning-only**, documented from public knowledge — **no
  values captured**. The firewall is environmental; re-check from an unblocked network.
- **Identifier**: the **STIR/INN (9-digit)** joins to the EGRPO register. **`taxpayer_status`**
  is **tax status** (active/inactive), not company registration status — keep distinct from the
  EGRPO `status`. **`vat_status`** indicates VAT (QQS) registration. Covers **individuals** too
  (personal data — redact). **Language**: Uzbek + Russian.
- No `sample_record.json`: source firewalled / not captured.
