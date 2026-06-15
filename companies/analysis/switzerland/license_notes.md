# Switzerland — License Notes

## Zefix (LINDAS SPARQL + REST API)

- **License: OGD / "Open use"** (the Swiss open-government-data terms). Free reuse,
  including commercial, with **mandatory source attribution** — credit
  "Eidgenössisches Amt für das Handelsregister (EHRA) / Zefix" and retain the
  source URL + retrieval date.
- **LINDAS SPARQL** (`https://lindas.admin.ch/query`) is **open, no authentication**.
  Query politely (paged SELECT, modest LIMITs); it is a shared public endpoint.
- **Zefix REST API** is the same open data but **gated by free HTTP Basic
  credentials** (`Zefix-Credentials`). Obtaining credentials is a registration
  step, not a payment; do not attempt to bypass the 401.

## SOGC / SHAB

- Official gazette publications are public (OGD/Open use). Accessed via the Zefix
  REST `/sogc` endpoints (same credential gate) or shab.ch. SOGC entries can name
  **officers/signatories** — treat person data per data-protection (FADP/GDPR).

## SIX Swiss Exchange (listed financials)

- Issuer financial reports are **public to view** but governed by SIX/issuer
  publication terms — not a blanket open-data licence. Use for listed companies;
  confirm redistribution terms before republishing.

## Handelsregisterauszug / cantonal extracts

- **Paid** per extract; certified documents are sold by the cantonal registers.
  Planning-only; no raw values copied.

## Financial-data availability (important)

- **Private companies (AG/GmbH) have no public financial-filing obligation** in
  Switzerland (Art. 958 CO — accounts are prepared but not disclosed). Therefore
  **no lawful open source of private-company financials exists**; only listed
  (SIX) and regulated (FINMA banks/insurers) financials are public.

## Summary

- **Open & usable (attribute)**: Zefix LINDAS SPARQL.
- **Free but credentialed**: Zefix REST API + SOGC.
- **Public but issuer-termed**: SIX listed financials.
- **Paid**: cantonal register extracts.
- **Unavailable**: private-company financials (no public filing).
