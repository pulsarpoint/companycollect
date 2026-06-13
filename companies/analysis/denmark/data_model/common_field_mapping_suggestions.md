# Denmark — Common Field Mapping Suggestions

> **Suggestion only.** This file proposes how Denmark's country-specific profile *could* map to
> a future cross-country company schema. It does **not** constrain the Denmark profile
> (`country_company_profile.schema.json`), which stays country-first. Use these mappings only in
> a later cross-country mapper.

| Common field | Denmark source field | Notes |
|---|---|---|
| `company_id` | `registration.cvr_nummer` (`cvr_permanent.cvrNummer`) | 8-digit; universal join key. |
| `registration_number` | `registration.cvr_nummer` | Same as company_id in Denmark. |
| `tax_id` | `registration.cvr_nummer` | CVR is the base for the SE/tax number. |
| `vat_id` | `registration.vat_id` | Derived: `'DK'+cvrNummer`. Only meaningful if VAT-registered (CVR does not always expose a VAT flag in the open layer). |
| `legal_name` | `legal_identity.navn` | Current CVR name; XBRL `gsd:NameOfReportingEntity` is a fallback. |
| `status` | `status.current_status` | Map NORMAL→active; UNDER KONKURS→bankrupt; OPLØST→dissolved; UNDER TVANGSOPLØSNING→compulsory_liquidation. |
| `legal_form` | `legal_identity.legal_form.kode` (+tekst) | Danish virksomhedsform codes (e.g. 60=A/S, 80=ApS, 10=Enkeltmandsvirksomhed). |
| `incorporation_date` | `registration.incorporation_date` (`stiftelsesDato`) | |
| `dissolution_date` | `status.dissolution_date` | Derived from livsforloeb. |
| `registered_address` | `registered_location.*` | Structured CVR address; `kommune_kode` gives municipality geography. |
| `activity_code` | `activity.primary.branchekode` | DB07 ≈ NACE Rev. 2; map DB07→NACE for cross-country comparability. |
| `financials` | `financial_statements[].line_items[]` | From parsed DCCA XBRL; attach period + currency + consolidated/solo. Map `fsa:` concept QNames to a common chart of accounts. |
| `officers` | `financial_statements[].board[]` | Executive (direktion) + supervisory (bestyrelse) members from XBRL; richer/current officer data is in CVR `deltager`. PERSONAL DATA. |
| `owners` | `participants[]` (role=owner / reel ejer) | From CVR `deltagerRelation` + `deltager`; beneficial owners (*reelle ejere*). PERSONAL DATA, credentials required. |
| `source_provenance` | `source_provenance[]` | Already structured per source. |

## Not available / caveats

- `lei` — only common as a global field for listed/IFRS filers; Denmark exposes it in XBRL
  (`gsd:LegalEntityIdentifierOfReportingEntity`) but most small companies have none.
- `owners` / `officers` — only fully available behind **free CVR credentials**
  (`cvr_permanent` / `deltager`); a partial officer view comes openly from XBRL board tags.
- A standalone `tax_id` distinct from the company number — `not_available_in_open_sources`
  (Denmark uses CVR as the single identifier; the VAT number is the only derived variant).
- Employee counts are **interval bands**, not exact numbers — flag as approximate when mapping
  to any common `employee_count` field.
