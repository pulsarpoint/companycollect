# GEMI — General Commercial Registry publicity portal Field Catalog

> Field model documented from the GEMI portal. **No `sample_record.json`**: the `/api` is reCAPTCHA-protected
> and rate-limited, so no per-company open record was lawfully downloadable; no real values are copied.

## Source Summary

- Country: Greece
- Source type: official_registry
- Organization: Central Union of Chambers of Greece (KEEE) / Ministry of Development — Γενική Γραμματεία Εμπορίου
- URL: https://publicity.businessportal.gr/ (EN: https://www.businessportal.gr/en/home-en/)
- License: public register (free to view); reuse terms unclear; no bulk redistribution implied
- Access: public (manual); `/api` reCAPTCHA-protected + rate-limited
- Freshness: real-time (filings)
- Record shape: company page (HTML/JSON via undocumented `/api`)
- Primary keys: `gemi_number`
- Join keys: `gemi_number`, `afm`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| gemi_number | Αριθμός ΓΕΜΗ | Registry id | string | identifier | (not copied) | register-side key |
| afm | ΑΦΜ | Tax id (9-digit) | string | identifier | (not copied) | cross-source join key |
| name | επωνυμία | Legal name | string | legal_name | (not copied) | Greek + Latin |
| legal_form | νομική μορφή | Legal form | string | legal_form | (not copied) | ΑΕ/ΕΠΕ/ΙΚΕ/ΟΕ/ΕΕ |
| status | κατάσταση | Status | string | status | (not copied) | ΕΝΕΡΓΗ/ΛΥΘΕΙΣΑ/… |
| registered_address | έδρα | Registered seat | string | address | (not copied) | |
| kad | ΚΑΔ | Activity code | string | activity | (not copied) | NACE-aligned |
| incorporation_date | ημερομηνία σύστασης | Incorporation date | date | date | (not copied) | |
| representatives[] | νόμιμοι εκπρόσωποι | Directors/representatives | array | person | (not copied) | **PII** |

## Interpretation Notes

- **Authoritative register, manual-only.** GEMI holds the full company identity (name EL/EN, legal form, status,
  seat, ΚΑΔ, incorporation, directors) and filed financial statements. It is free to **search manually**, but
  the underlying `/api` is **undocumented, rate-limited (HTTP 429) and reCAPTCHA-protected** — automated/bulk
  access is **blocked and must not be bypassed**, and there is no sanctioned open bulk endpoint.
- **Two identifiers.** GEMI number (register) and **ΑΦΜ** (tax id; the universal cross-source join key).
  VAT = `EL` + ΑΦΜ.
- **GDPR.** Directors/representatives are personal data — lawful basis + retention; no direct marketing.
- **License.** Reuse/redistribution terms are not clearly stated — confirm before redistribution.
