# Bolagsverket Värdefulla datamängder — /organisationer (company base data) Field Catalog

## Source Summary

- Country: Sweden
- Source type: official_registry_api (OAuth2, WSO2 gateway)
- Organization: Bolagsverket (Swedish Companies Registration Office)
- URL: https://gw.api.bolagsverket.se/vardefulla-datamangder/v1/organisationer
- License: Free of charge, no contract — EU Open Data Directive high-value datasets (2019/1024 / EU 2023/138). Confirm exact reuse string on dataportal.se/datasets/612_5428.
- Access: public, OAuth2 client_credentials gated (free `client_id`/`client_secret` via self-service *Kundanmälan*)
- Freshness: real-time / matches the official register
- Record shape: `POST /organisationer` with an organisationsnummer in the body → one JSON company-base-data object
- Primary keys: organisationsnummer
- Join keys: organisationsnummer

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| organisationsnummer | organisationsnummer | 10-digit Swedish org number; entity primary key (personnummer for sole traders) | string | identifier | 556012-5790 | Canonicalize digits-only + display NNNNNN-NNNN |
| organisationsnamn | organisationsnamn | Registered legal name | string | legal_name | — | Exact key unverified |
| juridisk_form | juridisk_form / organisationsform | Legal form (AB, HB, KB, Enskild firma, Ekonomisk förening) | string | legal_form | Aktiebolag | Code vs label unconfirmed |
| status | status | Registration status (registered / deregistered / konkurs / likvidation) | string | status | — | Enumeration unconfirmed |
| naringsgrenskod | naringsgrenskod / SNI | Industry code (SNI 2025) | string | activity | — | Repeatable; SCB is fuller |
| postadress_organisation.gatuadress | postadress_organisation (gatuadress) | Registered street address | string | address | — | Sub-structure unconfirmed |
| postadress_organisation.postnummer | postadress_organisation (postnummer) | Postal code | string | address | 111 22 | NNN NN vs digits-only |
| postadress_organisation.postort | postadress_organisation (postort) | Postal town | string | address | Stockholm | kommun/län from SCB |
| registreringsdatum | registreringsdatum | Registration date | date | date | — | ISO expected; presence unconfirmed |
| avregistreringsdatum | avregistreringsdatum | Deregistration date | date | date | — | Only for deregistered entities |
| momsregistreringsnummer | momsregistreringsnummer (VAT) | VAT number SE + orgnr + 01 | string | identifier | SE556012579001 | May be derived; prefer SCB register flag |

## Interpretation Notes

- **No authenticated record was observed.** Every probe to this gateway returns HTTP 401
  `{"code":"900902","message":"Missing Credentials"}` (WSO2 API Manager). The field set above is
  reconstructed from the official Bolagsverket landing/API pages, `schema_notes.md`, and the
  community Elixir client (`bolagsverket_ex`) docs. **Field keys, casing, nesting, and code lists
  must be confirmed against a real credentialed response** before the parser hardcodes them.
- **Identifiers.** `organisationsnummer` is the universal Swedish join key. For sole traders
  (enskild firma) it equals a personnummer and is personal data — handle privacy accordingly.
- **Address.** The Bolagsverket payload gives the registered postal address (postort). Municipality
  (kommun) and county (län) are more reliably obtained from SCB; do not assume Bolagsverket breaks
  them out.
- **VAT.** A Swedish VAT number is structurally `SE` + the 10 orgnr digits + `01`. It can be
  *derived*, but only an authoritative VAT-register flag (SCB) confirms the entity is actually
  VAT-registered. Mark derived VAT values as derived.
- **Financials are NOT here.** Annual-report figures come from the separate document endpoints —
  see the `bolagsverket_annual_reports` catalog.
