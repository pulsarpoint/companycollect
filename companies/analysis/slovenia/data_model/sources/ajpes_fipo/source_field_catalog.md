# AJPES Fi=Po / S.BON Field Catalog

> **PLANNING-ONLY / PAID.** Fi=Po (structured financial statements + indicators)
> and S.BON (credit ratings) are commercial AJPES products. Cataloged from public
> product descriptions; no records retrieved.

## Source Summary

- Country: Slovenia
- Source type: official_registry
- Organization: AJPES
- URL: https://www.ajpes.si/FinancialData
- License: paid / contract
- Access: paid (login)
- Freshness: annual
- Record shape: planning-only
- Primary keys: `Matična številka`
- Join keys: `Matična številka`, `Davčna številka`

## Fields (from public docs)

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| financial_statements[] | complete financial statements | Structured multi-year financials | array | financial | planning-only; EUR |
| indicators | financial indicators | Ratios/indicators | object | financial | planning-only |
| sbon_rating | S.BON | Credit rating | string | raw_extension | planning-only; proprietary |

## Interpretation Notes

- The **structured/bulk** route to Slovenian financials (what JOLP shows view-only).
  **Paid** — keep planning-only. Join on Matična številka.
- S.BON is a proprietary credit-rating model — optional risk signal, paid.
