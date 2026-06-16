# Singapore Company Profile — Source Mapping

> Keyed on the **UEN** = company id = registration number = tax reference
> (Singapore has GST not VAT; GST reg is generally the UEN, so no separate VAT
> number). Identity + status + activity + address + former names + auditors +
> officer **count** come openly from **ACRA** on data.gov.sg. Officer/shareholder
> **names**, share capital, and private financials are **paid** (BizFile); listed
> financials are open via SGX.

## Field mapping

| Profile path | Source | Source path | Join key | Freshness | License/Access | Precedence / Notes |
|---|---|---|---|---|---|---|
| registration.uen | acra_entities | uen | uen | monthly-ish | open | Authoritative id + tax ref. |
| tax_identifiers.tax_id | acra_entities | uen | — | monthly-ish | open | = UEN. |
| tax_identifiers.vat_id | — | — | — | — | not available | GST country; GST reg = UEN. |
| legal_identity.legal_name | acra_entities | entity_name | — | monthly-ish | open | Primary name. |
| legal_identity.former_names | acra_entities | former_entity_name1..15 | — | monthly-ish | open | Name history. |
| legal_identity.entity_type / company_type | acra_entities | entity_type_description / company_type_description | — | monthly-ish | open | Company/sole prop/partnership/LLP. |
| status.* | acra_entities | entity_status_description | — | monthly-ish | open | Live -> active. |
| incorporation.registration_incorporation_date | acra_entities | registration_incorporation_date | — | monthly-ish | open | |
| activity.* | acra_entities | primary_ssic_code / secondary_ssic_code | — | monthly-ish | open | SSIC. |
| registered_location.* | acra_entities | block/street/unit/building/postal_code | — | monthly-ish | open | |
| officers_summary.no_of_officers | acra_entities | no_of_officers | — | monthly-ish | open | COUNT only. |
| auditors[] | acra_entities | name_of_audit_firm1..5 (+ uen) | uen | monthly-ish | open | Each audit-firm UEN joins back. |
| officers[] | acra_bizfile_financials | profile.officers | uen | filing | paid | PLANNING-ONLY; names; PDPA — redact. |
| financial_statements[] | acra_bizfile_financials / sgx_listed_financials | financials.* / results.* | uen / ticker | filing/quarterly | paid / exchange terms | PLANNING-ONLY; SGD. |

## Source precedence

1. **acra_entities** — authoritative open registry (identity, status, activity,
   address, former names, auditors, officer count). Primary open source.
2. **sgx_listed_financials** — open financials for **listed** companies.
3. **acra_bizfile_financials** — officer/shareholder names, share capital, and
   private-company financials; **paid**, planning-only.

Conflict rules:
- **Officers**: the open dataset has only the count; names come from the paid
  BizFile profile (personal data).
- **Financials**: SGX (open) for listed; BizFile (paid) for private. No open
  financials for the broad population.

## Join keys

- **UEN** is the universal key across all sources (and audit-firm UENs join back to
  the entities dataset). SGX joins by UEN / ticker / name. The UEN is also the
  entity tax reference; no separate VAT id.

## Missing / restricted data

- **Officer / shareholder NAMES** and **share capital** — paid BizFile (PDPA).
- **Private-company financials** — paid BizFile (XBRL); listed via SGX.
- **No separate VAT id** — GST registration is generally the UEN.
