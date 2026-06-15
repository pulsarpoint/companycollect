# Companies House — REST API Field Catalog

> **FREE BUT KEY-GATED.** All endpoints need a free Companies House API key (HTTP
> Basic, key as username); 600 requests / 5 min. Officers and PSC carry **personal
> data** — redact. Cataloged from the developer docs; no records pulled.

## Source Summary

- Country: United Kingdom
- Source type: official_registry
- Organization: Companies House
- URL: https://developer.company-information.service.gov.uk/ (api.company-information.service.gov.uk)
- License: Open Government Licence (OGL)
- Access: public (free API key)
- Freshness: real-time
- Record shape: JSON endpoints per company
- Primary keys: `company_number`
- Join keys: `company_number`

## Fields (endpoints)

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| /company/{n}/officers | officers | Directors/secretaries | array | person | **the route to officers**; PII |
| /company/{n}/filing-history | filing-history | Filing events + documents | array | filing | document API → PDFs/iXBRL |
| /company/{n}/charges | charges | Charge detail | array | filing | vs counts in bulk |
| /company/{n} | profile | Live company profile | object | identifier | real-time; ISO dates |
| /company/{n}/persons-with-significant-control | PSC | Beneficial owners | array | ownership | PII; see ch_psc_snapshot |

## Interpretation Notes

- The **per-company detail** layer and the **only route to officers** (no officers
  bulk product). Also filing history + the **document API** (fetch the actual
  accounts/filing documents), charges, and real-time profile/PSC.
- **Free API key** required — register at the developer portal; not a payment, not
  bypassable. Rate limit 600 req / 5 min. Officers/PSC are **personal data** —
  redact.
- For bulk identity/financials/ownership prefer the bulk products (basic data,
  accounts, PSC snapshot); use the API for officers, filing history, and documents.
