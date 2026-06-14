# Company Data Analysis For Luxembourg

## Summary

Luxembourg is a **partial-open / automation-blocked but document-rich** country. The authoritative register,
the **RCS (Registre de Commerce et des Sociétés)** run by **Luxembourg Business Registers (LBR)**, is **public**:
**free** basic search and **free download of filed documents** — including **annual accounts (comptes annuels)**
— but the search UI is **captcha-gated** with **no open bulk/API**, certified extracts are paid, and
**data.public.lu** carries only STATEC **statistical** aggregates (not the register). Everything joins on the
**RCS number** (prefix = entity class) and the **matricule** (13-digit national id); **VAT** (`LU`+8) is separate.
So the open profile is strong on **identity + free documents (incl. financials)** per company, but there is **no
lawful open bulk/automation** — that needs a commercial provider.

## Sources Analyzed

| Source slug | Source name | Status | Access | License | Role |
|---|---|---|---|---|---|
| rcs_register | RCS public search (LBR) | blocked_by_authentication (automation); recommended (manual) | public | public (terms unclear) | **identity spine** (manual) + free documents |
| rcs_annual_accounts | RCS annual accounts (comptes annuels) | blocked_by_authentication | public | public docs | **financials** (free PDF/eCDF) |
| resa_gazette | RESA legal gazette | useful_secondary | public | public | events/history |
| rbe_register | RBE beneficial owners | blocked_by_authentication | restricted | restricted | beneficial owners |
| vies_vat | AED / VIES (LU VAT) | useful_secondary | public | validation | VAT (not in RCS data) |
| data_public_lu | data.public.lu / STATEC | useful_secondary | public | CC0/open | statistics (not register) |
| commercial_aggregators | Kyckr/Creditreform/… | useful_secondary | paid | commercial | bulk + structured financials |

## What Each Source Contributes

- **rcs_register** — the authoritative identity: RCS number, matricule, dénomination, forme juridique, siège
  social, statut, date de constitution, **plus free filed documents** (statutes, **annual accounts**,
  resolutions). Free manual search; automated access blocked (captcha; no API).
- **rcs_annual_accounts** — the comptes annuels (bilan + compte de profits et pertes + annexes), filed via the
  structured **eCDF** format, **free to download** per company as PDF. Document-based; no open structured bulk.
- **resa_gazette** — RESA legal publications (incorporation, amendments, accounts deposits, dissolutions) keyed
  on the RCS number — events/history.
- **rbe_register** — beneficial owners; **restricted** (post-CJEU). Planning-only.
- **vies_vat** — validates the LU VAT number (`LU`+8; not in the RCS data; no open crosswalk).
- **data_public_lu** — STATEC statistical enterprise aggregates; **not** the register.
- **commercial_aggregators** — vendors that index the RCS + parse the accounts; the realistic route to **bulk**
  and **structured financials** at scale. Paid, planning-only.

## Proposed Country Company Profile

`country_company_profile.schema.json` is keyed on `registration.rcs_number` with `registration.matricule` as the
cross-source key. It groups `tax_identifiers` (VAT external), `legal_identity`, `status`, `activity` (explicitly
`activity_code = not_available`), `incorporation`, `registered_location`, planning-only `officers[]` (from free
documents, PII), planning-only `financial_statements[]` (free PDF/eCDF, EUR), planning-only `beneficial_owners[]`
(restricted RBE), and `lifecycle_events[]` (RESA). Every section carries `source_provenance`. The example record
is **schematic** (placeholder values; officers/financials/owners empty/redacted) because no per-company open
record was lawfully downloadable in bulk.

## Join And Precedence Rules

- **Keys:** RCS number (prefix = entity class) + **matricule** (13-digit national id); VAT = LU+8 (separate).
- **Precedence:** RCS authoritative for identity/status + free documents (incl. annual accounts); financials from
  the free comptes annuels (PDF/eCDF) or a vendor; VAT external; RBE restricted; RESA events.
- **Automation:** RCS blocked (captcha; no open bulk/API) — manual or commercial provider only.

## Missing Or Restricted Data

- **No open bulk** export; **no open automation** of the RCS (captcha).
- **No NACE/activity code** in the free RCS data; **VAT/employee count** not in the RCS data.
- **Beneficial ownership (RBE)** is restricted (post-CJEU).
- **Financials/officers** are free per company but document-based (PDF/eCDF) and gated for automation.
- **License**: RCS reuse/redistribution terms unclear — confirm before redistribution.
- **GDPR**: officers (in documents) and beneficial owners are personal data.

## Common Mapper Notes

A cross-country mapper can map company_id/registration_number ← RCS number, tax_id ← matricule, vat_id ← LU+8
(VIES), legal_name/status/legal_form/incorporation_date/registered_address ← RCS, officers ← RCS documents. Map
`financials` to the free RCS comptes annuels (PDF/eCDF, OCR/parse) or a vendor (parsed, bulk). Mark
`activity_code`, `dissolution_date` (derive from status), and `owners` (RBE restricted) as
`not_available_in_open_sources`. Treat Luxembourg as requiring manual/document or commercial access rather than
open bulk.
