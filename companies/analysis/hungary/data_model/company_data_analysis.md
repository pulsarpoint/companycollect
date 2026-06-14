# Company Data Analysis For Hungary

## Summary

Hungary is a **partial-open / automation-blocked** country. **Basic company identity** is free to search
(e-cégjegyzék) and **annual financial statements** are **free to view** (e-beszámoló) with structured key
figures — but there is **no open bulk export**, the e-beszámoló search is **reCAPTCHA-protected** (so automation
is blocked), and **full register data** (officers, owners, history) is **paid**. Everything joins on the
**cégjegyzékszám** (registration number, `NN-NN-NNNNNN`) and the **adószám** (tax number; the **8-digit base** is
the universal cross-source stem; EU VAT = `HU` + base; the statistical code embeds it too). A lawful automated
profile needs a **commercial provider** (full register + structured financials) or manual lookups, with **NAV**
and **VIES** for tax/VAT validation and **KSH** for the canonical TEÁOR.

## Sources Analyzed

| Source slug | Source name | Status | Access | License | Role |
|---|---|---|---|---|---|
| ebeszamolo | e-beszámoló financial reports | blocked_by_authentication | public | public (terms unclear) | **financials** (free view, gated) |
| ecegjegyzek | e-cégjegyzék / Cégszolgálat | blocked_by_payment (full) | public | free basic / paid full | **identity spine** |
| nav_afaalany | NAV VAT-subjects DB | useful_secondary | public | public | VAT status + cancellation flag (daily) |
| vies_vat | VIES (HU VAT) | useful_secondary | public | validation | VAT validation |
| ksh_register | KSH business register | useful_secondary | public | open | statistical code + TEÁOR |
| procurement_ekr | EKR / Közbeszerzés | useful_secondary | public | public | supplier adószám cross-ref |
| commercial_aggregators | OPTEN/Bisnode/Céginfo/companyapi.hu | useful_secondary | paid | commercial | full register + structured financials |

## What Each Source Contributes

- **ebeszamolo** — the financial spine: structured key figures (net sales revenue, profit after tax, total
  assets, equity, liabilities) + full statements as **PDF/XML**, free to view, high coverage (mandatory
  e-filing). Search is **reCAPTCHA-gated** → automation blocked.
- **ecegjegyzek** — register identity: cégjegyzékszám, name, legal form, status, seat, main TEÁOR (free basic);
  **officers/owners/history are paid**.
- **nav_afaalany** — daily VAT-subject status by adószám + the **cancelled-tax-number** distress flag.
- **vies_vat** — validates HU EU VAT (HU + 8-digit base).
- **ksh_register** — canonical **TEÁOR** + the 17-digit statistical code (embeds the 8-digit base).
- **procurement_ekr** — open adószám↔name cross-reference for public-contract suppliers.
- **commercial_aggregators** — OPTEN/Bisnode/Céginfo/companyapi.hu resell the **full cégjegyzék** + **parsed
  financials** + credit; the realistic route to ownership/officers and structured financials at scale. Paid.

## Proposed Country Company Profile

`country_company_profile.schema.json` is keyed on `registration.cegjegyzekszam` with `tax_identifiers.adoszam`
(8-digit base) as the universal cross-source stem. It groups `tax_identifiers` (adószám/VAT/statistical code +
NAV VAT status + cancellation flag), `legal_identity`, `status`, `activity` (TEÁOR), `registered_location`,
`officers[]` (paid, planning-only), `owners[]` (paid, planning-only), `financial_statements[]` (e-beszámoló key
figures, free-view but gated → planning-only for automation; HUF/EUR), and `public_sector_links[]` (procurement
adószám cross-refs). Every section carries `source_provenance`. The example record is **schematic** (placeholder
values; financials/owners empty, officers redacted) because no per-company open record was lawfully downloadable.

## Join And Precedence Rules

- **Keys:** cégjegyzékszám (register) + **adószám** (8-digit base = universal stem); EU VAT = HU + base.
- **Precedence:** e-cégjegyzék authoritative for free basic identity; officers/owners paid; NAV adds VAT status +
  cancellation; KSH the canonical TEÁOR; financials from e-beszámoló (gated) or a vendor.
- **Automation:** e-beszámoló blocked (reCAPTCHA); full register paid — manual / vendor only.

## Missing Or Restricted Data

- **No open bulk** export; **no open automation** of e-beszámoló (reCAPTCHA).
- **Officers/owners are paid** (full cégjegyzék / vendor).
- **Beneficial ownership** (UBO register) is access-restricted — `not_available_in_open_sources`.
- **License**: register/financial reuse terms unclear — confirm before redistribution.
- **GDPR**: officers/owners are personal data.

## Common Mapper Notes

A cross-country mapper can map company_id/registration_number ← cégjegyzékszám, tax_id ← adószám, vat_id ←
HU+base, legal_name/status/legal_form/registered_address ← e-cégjegyzék, activity_code ← TEÁOR. Map `financials`
to e-beszámoló (gated) or a vendor (parsed), and `officers`/`owners` to the **paid** full register / vendor. Mark
`dissolution_date` (derive from status / NAV cancellation) and **beneficial ownership** as
`not_available_in_open_sources`. Treat Hungary as requiring manual/paid/commercial access rather than open bulk.
