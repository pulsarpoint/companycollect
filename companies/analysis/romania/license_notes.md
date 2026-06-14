# Romania — License Notes

## data.gov.ro (ONRC open datasets — OD_FIRME and companions)

- Published on the **national open-data portal** `data.gov.ro` with **ONRC** as
  the publishing institution. data.gov.ro is Romania's open-government data
  portal (aligned with the EU Open Data / PSI directive).
- **The exact per-dataset license text was not displayed** on the dataset page at
  retrieval time. data.gov.ro datasets are generally provided under open terms
  (attribution-style). **Confirm the precise license/attribution** before
  redistribution; do not assume CC0.
- Practical guidance: attribute "Oficiul Național al Registrului Comerțului
  (ONRC) — data.gov.ro" and retain the source URL + retrieval date.

## PERSONAL DATA (GDPR) — important

- `OD_REPREZENTANTI_LEGALI.CSV` and `OD_REPREZENTANTI_IF.CSV` contain **personal
  data**: representative/officer **name**, **date of birth**, **place of birth**,
  locality. Even though published openly, processing is subject to **GDPR**.
  - Redact or minimise person-level fields (especially birth date/place) in any
    published profile/sample.
  - Have a lawful basis and purpose limitation for storing officer data.
- Company-level fields (name, CUI, address, status, activities, financials) are
  not personal data for legal persons, but a registered office that is a natural
  person's home address can still be personal data.

## ANAF web services (bilant, ws/tva)

- Financial statements of economic operators are **public information published
  by law**; ANAF exposes them via free web services.
- Usage limits (must respect): **max 1 request/second**; ws/tva max **500 CUIs
  per request**. The bilant service applies an **F5 WAF** that returns empty data
  to non-browser User-Agents — sending a normal browser UA is required for a
  valid response and is **not** a control bypass (no auth/payment/CAPTCHA is
  circumvented). The ToS warns against server-overload attempts.

## Beneficial Ownership Register (RBR)

- **Restricted**: requires registration, an administrative **fee**, and a
  **qualified electronic signature**; access narrowed to **legitimate interest**
  after CJEU C-37/20. Do not treat as open; planning-only. Personal data.

## ONRC portal / RECOM (portal.onrc.ro)

- Full extracts (share capital, shareholders, history, filings) are **paid** per
  document and partly account/CAPTCHA-gated. Not redistributable from the portal;
  the open bulk CSV covers the master data instead.

## Summary

- **Open & usable**: OD_FIRME + companion CSVs (attribute ONRC/data.gov.ro;
  confirm exact license), ANAF bilant + ws/tva (respect rate limits).
- **Personal-data caution**: representatives/officers (GDPR) — redact.
- **Restricted/paid**: RBR (beneficial owners), ONRC portal full extracts.
