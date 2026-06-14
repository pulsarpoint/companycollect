# Portugal — Common Field Mapping Suggestions

> Suggestions only for a future cross-country mapper. This does **not** constrain
> the country-specific Portugal profile, which is the authoritative model.

Portugal is a **closed-data** country for company information: most common fields
are available only from paid/restricted sources, so they are marked
`paid_or_planning_only` rather than `not_available_in_open_sources` (the data
exists, just not openly).

| Common field | Portugal source | Portugal path | Notes |
|---|---|---|---|
| company_id | registo_comercial | registration.nipc | 9-digit NIPC (paid/aggregator/VAT-derived) |
| registration_number | registo_comercial | registration.nipc | NIPC doubles as the registration number |
| tax_id | registo_comercial | tax_identifiers.nif | = NIPC |
| vat_id | vies_vat | tax_identifiers.vat_id | PT + NIPC (free validation) |
| legal_name | registo_comercial | legal_identity.legal_name | paid; VIES may return it free |
| status | registo_comercial | status.estado | paid |
| legal_form | registo_comercial | legal_identity.legal_form | Lda./S.A./Unipessoal/... |
| incorporation_date | registo_comercial | (register) | paid; not modeled as open |
| dissolution_date | registo_comercial | status.estado | paid (status reflects dissolution) |
| registered_address | registo_comercial | registered_location.sede | paid |
| activity_code | registo_comercial | activity.cae_main | CAE Rev.3; paid |
| financials | ies_financials / commercial_aggregators | financial_statements[] | IES not openly published; via vendor |
| officers | registo_comercial | officers[] | PII; paid |
| owners | registo_comercial / rcbe_register | shareholders[] / beneficial_owners[] | PII; paid / restricted |
| source_provenance | (all) | source_provenance[] | per-section provenance |

## Cross-country notes

- **NIPC = NIF = VAT base.** Like other PT identifiers, one number drives all
  tax/company joins: `vat_id = "PT" + nipc`. A cross-country mapper can derive
  `vat_id` and `tax_id` from `company_id` for Portugal.
- **No open register.** Unlike EE/LV/IE/NL, Portugal exposes **no** per-company
  open dataset; a generic "fetch the open bulk register" strategy does not apply.
  The mapper should treat PT as **paid/aggregator-first** for everything beyond
  free VAT validation.
- **Statistics ≠ companies.** dados.justica.gov.pt looks like open company data
  but is statistical; do not map it to per-company common fields.
