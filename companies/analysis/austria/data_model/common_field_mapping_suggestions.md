# Austria — Common Field Mapping Suggestions

> **Suggestion only.** Proposes how Austria's country-specific profile *could* map onto a future
> cross-country company schema. It does **not** constrain `country_company_profile.schema.json`. The
> country-specific model is authoritative.

| Common field | Austria source path | Notes |
|---|---|---|
| company_id | registration.firmenbuchnummer (else registration.gisa_zahl) | FN (paid) for companies; GISA-Zahl (open) for trade-licence holders. |
| registration_number | registration.firmenbuchnummer | FN###### + check letter (paid). |
| tax_id | registration.uid | = VAT id (no separate tax id public). |
| vat_id | registration.uid ("ATU########") | paid (Firmenbuch) or VIES validation. |
| legal_name | legal_identity.name | open via GISA name; authoritative via Firmenbuch (paid). |
| status | status.derived | Firmenbuch aufrecht/gelöscht (paid) + Ediktsdatei insolvency (free web). |
| legal_form | legal_identity.rechtsform | paid (Firmenbuch). |
| incorporation_date | (Firmenbuch Eintragungsdatum) | paid. |
| dissolution_date | (Firmenbuch gelöscht) | paid. |
| registered_address | registered_location.* | open (GISA Standort) / paid (Firmenbuch). |
| activity_code | activity.gewerbeschluessel (GISA, OPEN) | No ÖNACE in the register; GISA trade code is the open proxy (trade-licence holders only). |
| financials | financial_statements[] | **PAID** (Jahresabschluss / aggregator); revenue null for small companies. |
| officers | officers[] (Firmenbuch, paid) | PII. |
| owners | not_available_in_open_sources | WiEReG beneficial ownership is restricted. |
| source_provenance | source_provenance[] | per-source + access flag. |

## Cross-country notes for a future mapper

- **Austria is a paid-register case** (with Germany/Italy): a cross-country mapper gets only an **open
  subset** for free (GISA trade authorizations + insolvency gazette); identity, financials, capital, and
  officers are **paid** (Firmenbuch / aggregator).
- **Two id systems**: **Firmenbuchnummer** (companies, paid) vs **GISA-Zahl** (open). They do **not** share
  a key — a cross-country `company_id` mapping must treat the open GISA layer as **fuzzy-linked** (name+
  location) to the company spine, or rely on UID where available.
- **Activity**: `activity_code` is **open only via GISA Gewerbeschlüssel** (and only for trade-licence
  holders); the register itself has **no coded ÖNACE** (free-text Geschäftszweig).
- **Financials**: a `financials` mapper must tolerate Austria's `financial_statements[]` being **paid/
  empty** and `revenue` null for small companies (UGB size classes).
- **Ownership**: `owners` = `not_available_in_open_sources` (WiEReG restricted).
- **Currency** EUR; **identifiers** keep the FN check letter and the UID `ATU` prefix.
