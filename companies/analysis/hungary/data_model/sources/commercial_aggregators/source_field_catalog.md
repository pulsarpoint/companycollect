# Commercial aggregators (OPTEN, Bisnode, Céginfo, companyapi.hu, ...) Field Catalog

> **Planning-only.** Proprietary, paid, per-vendor contract. Fields described from public product documentation;
> no records/values copied. No `sample_record.json`.

## Source Summary

- Country: Hungary
- Source type: commercial_api
- Organization: various private vendors (OPTEN, Bisnode/Dun & Bradstreet, Céginfo, companyapi.hu, …)
- URL: https://www.opten.hu/ ; https://companyapi.hu/
- License: commercial / paid (per-vendor contract)
- Access: paid
- Freshness: from cégjegyzék / e-beszámoló
- Record shape: vendor JSON or PDF (full register + parsed financials + officers/owners + credit)
- Primary keys: `adoszam`
- Join keys: `adoszam`, `cegjegyzekszam`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| company.financials[] | financials | Parsed structured financials | array | financial | (paid) | HUF/EUR; from e-beszámoló |
| company.officers[] | képviselők | Officers | array | person | (paid) | PII |
| company.owners[] | tulajdonosok | Owners | array | ownership | (paid) | PII |
| company.credit_score | minősítés | Credit/risk rating | string | raw_extension | (paid) | proprietary |

## Interpretation Notes

- **The realistic path to full register + structured financials.** Because e-beszámoló is **reCAPTCHA-gated**
  and full cégjegyzék data (officers/owners/history) is **paid**, vendors that pre-parse cégjegyzék + e-beszámoló
  are the practical way to get **structured** financials and **ownership/officers** at scale. companyapi.hu
  advertises ~31 fields direct from the Ministry of Justice Company Information Service, no contract.
- **Proprietary & paid.** Redistribution/persistence are governed by the vendor contract — keep entirely
  **planning-only** until a contract is in place. Join on **adószám** / cégjegyzékszám.
