# Serbia — Common Field Mapping Suggestions

> Suggestions only for a future cross-country mapper. This does **not** constrain
> the country-specific Serbia profile, which is authoritative.

| Common field | Serbia source | Serbia path | Notes |
|---|---|---|---|
| company_id | apr_companies | registration.maticni_broj | 8-digit MB |
| registration_number | apr_companies | registration.maticni_broj | same as company_id |
| tax_id | apr_webservice | tax_identifiers.pib | not_available_in_open_sources (paid) |
| vat_id | apr_webservice | tax_identifiers.pib | PIB doubles as VAT; paid |
| legal_name | apr_companies | legal_identity.legal_name | Latin, Cyrillic, or mixed script |
| status | apr_companies | status.status | map Cyrillic → enum |
| legal_form | apr_companies | legal_identity.legal_form | Cyrillic |
| incorporation_date | apr_companies | incorporation.incorporation_date | ISO |
| dissolution_date | not_available_in_open_sources | — | only status signals it |
| registered_address | apr_companies | registered_location.municipality_name | municipality only (no street) |
| activity_code | apr_companies | activity.kd2010_code | KD2010 ≈ NACE Rev.2 |
| financials | apr_financial_statements | financial_statements[] | thousands RSD; latest year only |
| officers | apr_webservice | officers.records[] | not_available_in_open_sources; SP3/SP4 paid |
| officers_availability | mapper envelope | officers.availability | distinguish not acquired/restricted from a confirmed empty result |
| owners | apr_beneficial_owners | beneficial_owners.records[] | separate restricted CEV source; never infer from members |
| owners_availability | mapper envelope | beneficial_owners.availability | distinguish not acquired/restricted from a confirmed empty result |
| source_provenance | (all) | source_provenance[] | per-section provenance |

## Cross-country notes

- **Single-key country.** Everything joins on **matični broj**; there is **no
  PIB/VAT in open data**, so a cross-country mapper cannot derive `vat_id`/`tax_id`
  for Serbia from open sources (unlike RO/PT where VAT = prefix+id).
- **Financials are open but shallow**: one year, summary figures, in **thousands
  of RSD**. Map into `financials[]` but record the unit and the single-year limit.
- **Cyrillic normalisation**: status, legal form, and municipality are Cyrillic;
  the business name is Latin. A mapper should transliterate/normalise for
  matching.
- **People-source boundary**: SP3/SP4 representatives and CEV beneficial owners
  are separate products. Model independent acquisition status for each.
- **Empty is not absent**: an empty `records` array is meaningful only with
  `availability=complete`, `partial`, or `not_applicable`. Current examples use
  `not_acquired`.
- **Members are not beneficial owners**: do not promote the APR public-search
  `Чланови` section into CEV ownership.
- **Sensitive identifiers**: never expose raw JMBG/passport/card values. If an
  approved deterministic linkage is necessary, use a secret-keyed HMAC.
