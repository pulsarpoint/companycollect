# Greece Company Profile — Mapping Report

Join on **ΑΦΜ** (AFM) across sources and the **GEMI number** on the register side. GEMI is authoritative but
manual-only (API reCAPTCHA-protected + rate-limited); financials are PDF; AADE needs credentials.

| Profile path | Source | Source path | Join key | Freshness | Access/License | Precedence | Notes |
|---|---|---|---|---|---|---|---|
| registration.gemi_number | gemi_portal | gemi_number | self | real-time | public (manual) | authoritative | register id |
| tax_identifiers.afm | gemi_portal | afm | self | real-time | public (manual) | authoritative | cross-source key |
| tax_identifiers.vat_id | vies_vat | vatNumber | afm | real-time | public / validation | authoritative | EL + ΑΦΜ |
| tax_identifiers.vat_valid | vies_vat | valid | afm | real-time | public / validation | enrichment | point-in-time |
| legal_identity.legal_name | gemi_portal | επωνυμία | gemi_number | real-time | public (manual) | authoritative | Greek + Latin |
| legal_identity.legal_form | gemi_portal | νομική μορφή | gemi_number | real-time | public (manual) | authoritative | ΑΕ/ΕΠΕ/ΙΚΕ/ΟΕ/ΕΕ |
| status.value | gemi_portal | κατάσταση | gemi_number | real-time | public (manual) | authoritative | ΕΝΕΡΓΗ/ΛΥΘΕΙΣΑ/… |
| status.tax_status | aade_rgwspublic | deactivationFlag | afm | real-time | restricted | planning-only | tax-side active/ceased |
| activity.kad | gemi_portal | ΚΑΔ | gemi_number | real-time | public (manual) | authoritative | unordered |
| activity.kad_primary | aade_rgwspublic | firmActivities[].kad (κύρια) | afm | real-time | restricted | planning-only | AADE flags primary |
| incorporation.incorporation_date | gemi_portal | ημερομηνία σύστασης | gemi_number | real-time | public (manual) | authoritative | |
| registered_location.* | gemi_portal | έδρα | gemi_number | real-time | public (manual) | authoritative | parse δήμος/περιφέρεια |
| officers[] | gemi_portal | representatives | gemi_number | real-time | public (manual) | authoritative | **PII (GDPR)** |
| financial_statements[] | gemi_financial_statements | isologismos/oikonomikes (PDF) | gemi_number/afm | annual | public / PDF | planning-only | OCR/parse; EUR |
| financial_statements[] (alt) | commercial_aggregators | company.financials[] | afm | vendor | paid | planning-only | scalable structured |
| public_sector_links[] | diavgeia / procurement_kimdis | afm references | afm | continuous | open | cross-reference | ΑΦΜ↔name corroboration |

## Precedence Rules

1. **GEMI is authoritative** for identity, legal form, status, seat, ΚΑΔ, incorporation, directors — but
   **manual-only** (its `/api` is reCAPTCHA-protected + rate-limited; no open bulk; do not bypass).
2. **ΑΦΜ is the cross-source key**; VAT = EL + ΑΦΜ (VIES validates).
3. **AADE RgWsPublic** (credentialed) is the tax-side complement — adds the **primary ΚΑΔ** and tax status;
   planning-only.
4. **Financials** come from GEMI as **PDF** (planning-only, OCR) or a **commercial provider** (parsed, paid).
5. **Diavgeia / procurement** are open ΑΦΜ↔name cross-references, not a company master.
6. **data.gov.gr** is not the company register (statistical).

## Missing-Data Notes

- **No open bulk** company export; **no structured open financials** (PDF only).
- **Automated GEMI access blocked** (reCAPTCHA + rate limits) — manual lookups or a commercial provider.
- **GEMI reuse/redistribution terms unclear** — confirm before redistribution.
- **Beneficial ownership** (Greek UBO register, Μητρώο Πραγματικών Δικαιούχων) is access-controlled — not
  included; `not_available_in_open_sources` for open use.
- **GDPR**: directors/representatives are personal data.
