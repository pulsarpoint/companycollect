# Poland — Common Field Mapping Suggestions

> **Suggestion only.** Proposes how Poland's country-specific profile *could* map onto a future
> cross-country company schema. It does **not** constrain `country_company_profile.schema.json`. The
> country-specific model is authoritative.

| Common field | Poland source path | Notes |
|---|---|---|
| company_id | registration.krs (companies) else registration.nip | KRS for companies; NIP for sole traders. |
| registration_number | registration.krs | 10-digit; null for sole traders (use NIP). |
| tax_id | registration.nip | 10-digit NIP. |
| vat_id | registration.vat_id ("PL" + NIP) | white list gives VAT status. |
| legal_name | legal_identity.nazwa | open (KRS). |
| status | status.derived (+ vat_status) | KRS dzial6 / CEIDG status / white list. |
| legal_form | legal_identity.forma_prawna | open (KRS). |
| incorporation_date | (KRS dataRejestracjiWKRS / CEIDG dataRozpoczecia) | open. |
| dissolution_date | (KRS dzial6 wykreślenie) | open. |
| registered_address | registered_location.* | open (KRS + white list). |
| activity_code | activity.pkd_primary | **PKD — open and clean**. |
| financials | financial_statements[] | **OPEN structured XML** (RDF e-Sprawozdania) — free, no paid tier. |
| officers | officers[] (KRS, anonymized) | board; PII anonymized by source. |
| owners | beneficial_owners[] (CRBR) | **OPEN beneficial ownership** (sensitive PII — minimize). |
| source_provenance | source_provenance[] | per-source + access flag. |

## Cross-country notes for a future mapper

- **Poland is a top-tier open case** (with Norway/France): a cross-country mapper gets identity,
  **financials**, **beneficial ownership**, VAT status, and **bank accounts** — all for free. Few countries
  offer open beneficial ownership; Poland does (CRBR).
- **Multi-id**: KRS (companies) + NIP (universal) + REGON — the **white list** bridges all three in one
  call. A cross-country `company_id` should prefer KRS for companies, NIP for sole traders; normalize REGON.
- **Activity code (PKD) is open and clean** — not `not_available_in_open_sources`.
- **Financials**: open structured XML; a `financials` mapper must handle **versioned MF schemas**,
  entity-type/P&L variants, and **unit scaling** (whole vs thousands); currency PLN.
- **Two populations**: KRS companies vs CEIDG sole traders — carry `entity_kind`.
- **PII** is the real constraint (CEIDG names, CRBR PESEL) — GDPR minimization, not availability.
