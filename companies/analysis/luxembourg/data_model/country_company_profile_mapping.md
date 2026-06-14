# Luxembourg Company Profile — Mapping Report

Join on the **RCS number** and the **matricule** across sources. The RCS is free (search + document downloads,
incl. annual accounts) but captcha-gated with no open bulk/API; VAT and beneficial ownership are separate/
restricted.

| Profile path | Source | Source path | Join key | Freshness | Access/License | Precedence | Notes |
|---|---|---|---|---|---|---|---|
| registration.rcs_number | rcs_register | rcs_number | self | real-time | public (manual) | authoritative | prefix = entity class |
| registration.matricule | rcs_register | matricule | self | real-time | public (manual) | authoritative | 13-digit national id |
| tax_identifiers.vat_id | vies_vat | vatNumber | (name match) | real-time | public / validation | external | LU+8; not in RCS data |
| legal_identity.legal_name | rcs_register | denomination | rcs_number | real-time | public (manual) | authoritative | |
| legal_identity.legal_form | rcs_register | forme_juridique | rcs_number | real-time | public (manual) | authoritative | S.A./S.à r.l./SCSp |
| status.value | rcs_register | statut | rcs_number | real-time | public (manual) | authoritative | inscrite/en liquidation/radiée |
| activity.activity_code | — | not_available | — | — | — | none | no public NACE in free RCS data |
| incorporation.incorporation_date | rcs_register | date_constitution | rcs_number | real-time | public (manual) | authoritative | |
| registered_location.* | rcs_register | siege_social | rcs_number | real-time | public (manual) | authoritative | parse commune |
| officers[] | rcs_register (documents) / commercial_aggregators | documents[] | rcs_number | real-time | public docs / paid | planning-only (automation) | **PII (GDPR)**; free per company |
| financial_statements[] | rcs_annual_accounts | bilan / compte de profits et pertes (PDF/eCDF) | rcs_number/matricule | annual | public docs (free) | planning-only (automation) | OCR/eCDF-parse; EUR |
| financial_statements[] (alt) | commercial_aggregators | company.financials[] | rcs_number | vendor | paid | planning-only | scalable structured + bulk |
| beneficial_owners[] | rbe_register | beneficial_owners[] | rcs_number | continuous | restricted | planning-only | **PII (GDPR)** |
| lifecycle_events[] | resa_gazette | publication_type/date | rcs_number | continuous | public | cross-reference | events/history |

## Precedence Rules

1. **RCS is authoritative** for identity, legal form, status, registered office, incorporation — and for the free
   filed **documents** (statutes, **annual accounts**) — but **manual-only** (captcha-gated; no open bulk/API; do
   not bypass).
2. **Two keys**: RCS number (prefix = entity class) + **matricule** (13-digit national id). **VAT** = `LU` + 8
   digits, separate (VIES; no open crosswalk).
3. **Officers + financials live in the free RCS documents** (PDF/eCDF) — free per company, but **planning-only
   for automation** (captcha) and document-based; a **commercial provider** is the scalable structured + bulk
   route.
4. **Beneficial ownership (RBE)** is **restricted** (post-CJEU) — planning-only.
5. **RESA** is an events/history cross-reference; **data.public.lu** is not the register (statistics only).

## Missing-Data Notes

- **No open bulk** company/financials export; **RCS automation blocked** (captcha).
- **No NACE/activity code** in the free RCS data (`activity_code` = not_available).
- **VAT** not in the RCS data (no open RCS↔VAT crosswalk); **employee count** not available.
- **Officers/financials** are free per company but document-based (PDF/eCDF) and gated for automation.
- **GDPR**: officers (in documents) and beneficial owners are personal data; RBE restricted.
- **License**: RCS reuse/redistribution terms unclear — confirm before redistribution.
