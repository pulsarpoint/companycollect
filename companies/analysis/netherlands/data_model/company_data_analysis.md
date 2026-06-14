# Company Data Analysis For Netherlands

## Summary

The Netherlands is a **split-access** country. The **KvK (Kamer van Koophandel)** publishes two genuinely **open
(CC-BY 4.0, EU High-Value DataSets)** datasets — **basic company data** and **structured annual accounts
(jaarrekeningen)** — but **both are anonymised in bulk** (no KvK number, name, address or directors). So the open
data supports rich **statistics** (legal form, activity, age, region) and **structured financial benchmarks**
(balance-sheet figures), but cannot identify a company. **Identified** data keys on the **KvK-nummer** (8 digits):
the **free HVDS open-data API** returns a company's basic + financial data **by KvK number** (with a free API
key), while names, addresses and officers require the **paid KvK Handelsregister API** (or a commercial
provider). **RSIN** (9 digits) is the VAT base; **UBO** (beneficial owners) is restricted to AML-obliged
entities.

## Sources Analyzed

| Source slug | Source name | Status | Access | License | Role |
|---|---|---|---|---|---|
| kvk_open_basis | KvK Open Data — basis bedrijfsgegevens | recommended | public | CC-BY 4.0 | **anonymised identity/stats** |
| kvk_open_jaarrekeningen | KvK Open Data — jaarrekeningen | recommended | public | CC-BY 4.0 | **structured financials** (anonymised) |
| kvk_handelsregister_api | KvK Handelsregister API | blocked_by_payment | paid | commercial | identified data (name/address/officers) |
| ubo_register | UBO-register | blocked_by_authentication | restricted | restricted | beneficial owners |
| vies_vat | Belastingdienst / VIES | useful_secondary | public | validation | VAT (= NL+RSIN+B+2) |
| data_overheid_nl | data.overheid.nl | useful_secondary | public | per dataset | catalog (CC-BY 4.0) |
| commercial_aggregators | Company.info, Graydon, Kyckr | useful_secondary | paid | commercial | identified bulk + financials |

## What Each Source Contributes

- **kvk_open_basis** — anonymised company attributes (verified: **1,891,639 records**): registration date,
  active/insolvency, **legal form**, postcode region, **SBI** activity codes. CC-BY 4.0; no identifier.
- **kvk_open_jaarrekeningen** — **structured** deposited annual accounts (XBRL-derived XML): balance sheet
  (assets current/non-current, equity, liabilities by maturity, provisions, called-up share capital) + financial
  year + SBI. CC-BY 4.0; anonymised; EUR. Verified by downloading a 200 MB ZIP and parsing a real report.
- **kvk_handelsregister_api** — the **paid** identified route: KvK-nummer, RSIN, name, addresses, officers
  (Basisprofiel), establishments (Vestigingsprofiel), search (Zoeken).
- **ubo_register** — beneficial owners; **restricted** (AML-obliged entities, post-CJEU). Planning-only.
- **vies_vat** — VAT = `NL` + RSIN + `B` + 2 (derivable from RSIN; VIES validates).
- **data_overheid_nl** — CKAN catalog of the KvK open datasets (CC-BY 4.0).
- **commercial_aggregators** — Company.info/Graydon/Kyckr resell identified KvK data + parsed financials + group
  + credit; the route to identified financials at scale.

## Proposed Country Company Profile

`country_company_profile.schema.json` is keyed on `registration.kvk_nummer` (with `rsin`). It groups
`tax_identifiers` (VAT derived), `legal_identity` (legal_form open; legal_name paid), `status` (active/
insolvency, open), `activity` (SBI, open), `incorporation` (open), `registered_location` (postcode region open;
full address paid), planning-only/paid `officers[]`, planning-only/restricted `beneficial_owners[]`, and
**`financial_statements[]`** (open structured balance-sheet figures, EUR — anonymised in bulk, identified via the
HVDS API by KvK number). Every section carries `source_provenance`. The example combines a **real anonymised
basis row** with a **real jaarrekening** (Assets 428763, Equity 275698, Liabilities 153065); identity fields are
null because the bulk is anonymised.

## Join And Precedence Rules

- **Join key:** **KvK-nummer** (8 digits) — not in the open bulk; via the HVDS API (by number) or the paid API.
  RSIN (9, VAT base); vestigingsnummer (12).
- **Precedence:** open KvK datasets authoritative for legal form/status/activity/financials (anonymised);
  identity (name/address/officers) from the paid KvK API; financials linked to a named company via the HVDS API
  by KvK number or a vendor; UBO restricted.

## Missing Or Restricted Data

- **No join key in the open bulk** (anonymised) — identification needs the paid/HVDS API.
- **Names/addresses/officers are paid**; **beneficial ownership restricted** (AML).
- **Income-statement detail** limited in the open jaarrekeningen (micro/small abridged).
- **License**: open datasets are **CC-BY 4.0** (attribute KvK); the paid API has its own terms.
- **GDPR**: officers and beneficial owners are personal data.

## Common Mapper Notes

A cross-country mapper can map company_id/registration_number ← KvK-nummer, tax_id ← RSIN, vat_id ← NL+RSIN+B+2,
legal_form/status/incorporation_date/activity_code ← open basis, financials ← open jaarrekeningen (XBRL, EUR).
Map `legal_name`/`registered_address`/`officers` to the **paid KvK API**, and `owners` to the **restricted UBO**.
Treat the open bulk as anonymised statistics/financial benchmarks; use the **HVDS API by KvK number** or a vendor
for identified per-company data + financials. Mark `dissolution_date` and income-statement detail as
`not_available_in_open_sources`.
