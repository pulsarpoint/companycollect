# Switzerland Company Profile — Mapping Report

Switzerland has **fully-open identity** (Zefix LINDAS SPARQL, no auth) but a
**structural financial-data gap**: private companies have no public filing
obligation, so financials are open only for listed issuers (SIX). Everything keys
on the **UID** (CHE-xxx.xxx.xxx); `vat_id = UID + ' MWST'/'TVA'/'IVA'`.

## Mapping Table

| Profile path | Source | Source path | Join key | Precedence | Notes |
|---|---|---|---|---|---|
| registration.uid | zefix_lindas | schema:identifier[CompanyUID] | UID | LINDAS | company id + join key |
| registration.chid | zefix_lindas | schema:identifier[CHID] | UID | LINDAS | cantonal id |
| registration.ehraid | zefix_lindas | schema:identifier[EHRAID] | UID | LINDAS | Zefix id |
| tax_identifiers.vat_id | zefix_lindas | derived: UID + ' MWST'/'TVA'/'IVA' | UID | derived | confirm VAT registration |
| legal_identity.legal_name | zefix_lindas | schema:legalName | UID | LINDAS | multilingual |
| legal_identity.legal_form | zefix_lindas | schema:additionalType | UID | LINDAS | eCH-0097 |
| legal_identity.purpose / website | zefix_lindas | schema:description / schema:url | UID | LINDAS | |
| status.status | zefix_rest_api | status | UID | free credentialed | LINDAS ≈ active set |
| registered_location.* | zefix_lindas | schema:address / admin:municipality | UID | LINDAS | structured |
| share_capital | zefix_rest_api | capitalNominal/Currency | UID | free credentialed / paid extract | registered capital |
| officers[] | sogc_shab / handelsregister_extract | persons[] | UID | PLANNING-ONLY | credentialed/paid; PII |
| register_events[] | sogc_shab | publicationDate/mutationType | UID | free credentialed | incorporation/dissolution |
| financial_statements[] | six_listed_financials | balance_sheet/income_statement | UID/ISIN | LISTED ONLY | structural gap for private |

## Source Precedence

1. **Zefix LINDAS** — authoritative open identity (UID, name, legal form, address,
   purpose, website). OGD/Open use.
2. **Zefix REST API** (free credentialed) — status, registered capital, SOGC links.
3. **SOGC/SHAB** (free credentialed) — register events + officers → planning-only.
4. **SIX** — listed-company financials only → planning-only.
5. **Cantonal extract** — paid officers/capital/journal → planning-only.

## Join Keys

- **UID** is the single universal key (CHID/EHRAID alternates). `vat_id = UID +
  ' MWST'/'TVA'/'IVA'`. ISIN links listed financials to the issuer/UID by name.

## Missing / Restricted

- **Financials** — not available for private companies (no public filing); listed
  only (SIX). The defining gap.
- **Activity code** — no per-company NOGA in the open set.
- **Status / capital / events / officers** — credentialed (free) or paid; officers
  are personal data.
- **Beneficial owners** — no public register.
