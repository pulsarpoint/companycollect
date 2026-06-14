# Company Data Analysis For Malta

## Summary

Malta is a **partial-open / automation-blocked** country. The authoritative register, the **MBR (Malta Business
Registry)**, is **publicly searchable for free** (English) and holds rich data — basic info, **officers**,
**shareholders** (name, share type, degree of control), and **financial information** (annual accounts, annual
return). But the deeper data and documents are **paid** (EUR 5–25), the official **MBR API** is a **paid
subscription**, the registry portals are **WAF-blocked (HTTP 403)** for automation, there is **no open bulk
export**, and **data.gov.mt** is WAF-blocked and does not publish the register. Everything joins on the
**registration number** (e.g. `C 12345`); **VAT** (`MT`+8) is separate; **UBO** is restricted to legitimate
interest. So the open profile is **free identity + status**, with officers/shareholders/financials behind a fee
and beneficial ownership restricted.

## Sources Analyzed

| Source slug | Source name | Status | Access | License | Role |
|---|---|---|---|---|---|
| mbr_register | MBR company search | blocked_by_authentication (automation); recommended (manual) | public | public (terms unclear) | **identity spine** (manual) |
| mbr_annual_accounts | MBR annual accounts / return | blocked_by_payment | paid | public/paid | **financials** (paid PDF) |
| mbr_api | MBR API packages | blocked_by_payment | paid | subscription | sanctioned automation (structured) |
| rbe_register | MBR UBO register | blocked_by_authentication | restricted | restricted | beneficial owners |
| vies_vat | CFR / VIES (MT VAT) | useful_secondary | public | validation | VAT (not in register) |
| data_gov_mt | data.gov.mt | useful_secondary | public | per dataset | not the register (WAF-blocked) |
| commercial_aggregators | Kyckr/Creditinfo/… | useful_secondary | paid | commercial | bulk + structured financials |

## What Each Source Contributes

- **mbr_register** — the authoritative identity: registration number, name, type, status, registration date,
  registered office (free); plus **officers**, **shareholders** and **financial info** (paid). Registry portals
  WAF-blocked for automation.
- **mbr_annual_accounts** — annual accounts (IFRS/GAPSME) + annual return as **paid documents** (EUR 5–25, PDF).
  Document-based; no open structured bulk.
- **mbr_api** — the MBR's **paid subscription API** packages (Company Search API, etc.) — the sanctioned
  automation path, returning structured company data (and financial information where the package includes it).
- **rbe_register** — beneficial owners; **restricted** (legitimate interest, post-CJEU). Planning-only.
- **vies_vat** — validates the MT VAT number (not in the register; no open crosswalk).
- **data_gov_mt** — WAF-blocked + not the register.
- **commercial_aggregators** — vendors reselling the MBR + parsed accounts; bulk + structured financials at
  scale. Paid, planning-only.

## Proposed Country Company Profile

`country_company_profile.schema.json` is keyed on `registration.registration_number` and groups
`tax_identifiers` (VAT external), `legal_identity`, `status`, `activity` (explicitly `activity_code =
not_available`), `incorporation`, `registered_location`, planning-only/paid `officers[]` and `shareholders[]`
(registered owners — a Malta distinctive), planning-only/restricted `beneficial_owners[]` (UBO), and
planning-only/paid `financial_statements[]` (annual accounts, EUR). Every section carries `source_provenance`.
The example record is **schematic** (placeholder values; financials/owners empty, officers/shareholders redacted)
because no per-company open record was lawfully downloadable in bulk.

## Join And Precedence Rules

- **Key:** registration number (prefix = entity class); VAT = MT+8 (separate, no open crosswalk).
- **Precedence:** MBR authoritative for identity/status (free) and the deeper register data (paid); financials
  from the paid annual accounts / paid API / a vendor; UBO restricted.
- **Automation:** MBR registry portals WAF-blocked — manual, paid MBR API, or commercial provider only.

## Missing Or Restricted Data

- **No open bulk** export; **no free API** (registry portals WAF-blocked).
- **Officers/shareholders/financials are paid** (documents or the paid API).
- **Beneficial ownership (UBO)** restricted (legitimate interest).
- **No NACE/activity code, no VAT, no employee count** in the free register data.
- **License**: MBR reuse/redistribution terms unclear — confirm before redistribution.
- **GDPR**: officers, shareholders and beneficial owners are personal data.

## Common Mapper Notes

A cross-country mapper can map company_id/registration_number ← registration number, vat_id ← MT+8 (VIES),
legal_name/status/legal_form/incorporation_date/registered_address ← MBR. Map `financials` to the paid MBR
annual accounts / paid API / a vendor; map `owners` to **registered shareholders** (paid) **and** beneficial
owners (restricted) as distinct sub-concepts. Mark `tax_id`, `activity_code`, `dissolution_date` (derive from
status) and UBO as paid/restricted/`not_available_in_open_sources`. Treat Malta as requiring manual/paid/
commercial access rather than open bulk.
