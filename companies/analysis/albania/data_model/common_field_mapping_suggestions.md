# Albania — Common Field Mapping Suggestions

> Suggestion layer only for a future cross-country mapper. It does **not**
> constrain the Albania-specific profile.

| Common field | Albania mapping | Status |
|---|---|---|
| company_id | registration.nipt (NIPT/NUIS) | open (Open Data Albania) |
| registration_number | registration.nipt | open |
| tax_id | registration.nipt | open (= NIPT) |
| vat_id | registration.nipt | open (NIPT serves as the VAT id) |
| legal_name | legal_identity.legal_name (Emri) | open |
| status | status.status (Aktiv/Pasiv/Çregjistruar) | open |
| legal_form | legal_identity.legal_form (Sh.p.k./Sh.a./Person Fizik) | open |
| incorporation_date | incorporation.registration_date | QKB extract (per-company) |
| dissolution_date | not_available_in_open_sources | status flag (Çregjistruar) |
| registered_address | not_available_in_open_sources (open mirror) | QKB extract has the address |
| activity_code | activity.activity_text (free text; NACE-aligned) | open |
| financials | financial_statements[] (QKB bilanci) | per-company; ALL |
| officers | officers[] (administrator/ortakë) | open but personal data (Law 9887) |
| owners | officers[] (ortakë/aksionarë) | open but personal data |
| source_provenance | source_provenance[] | available |

## Notes

- **Single anchor**: the **NIPT/NUIS** is `company_id`, `registration_number`,
  `tax_id`, AND `vat_id` — Albania has VAT and the NIPT serves as the VAT number.
- **Identity is open** via Open Data Albania (verified 4,459 companies); **financial
  statements** (bilanci) are filed with QKB per-company (ALL), not clean open bulk.
- **Personal data**: administrator/owners are personal data (Law 9887 / GDPR-
  aligned) — redact.
