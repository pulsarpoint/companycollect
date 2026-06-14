# Greece — Schema Notes

No per-company open record was lawfully downloadable (GEMI API reCAPTCHA-protected + rate-limited; AADE service
needs credentials). Fields below are documented from the GEMI portal data model and the AADE RgWsPublic service.
Join on the **ΑΦΜ** (AFM) across sources and the **GEMI number** on the register side.

## Identifiers
- **Αριθμός ΓΕΜΗ (GEMI number)** — General Commercial Registry id (register-side key).
- **ΑΦΜ (AFM)** — 9-digit tax identification number (AADE); the **universal cross-source join key**.
- **VAT** — `EL` + ΑΦΜ (for VIES validation).
- **ΚΑΔ** — Κωδικός Αριθμός Δραστηριότητας (Greek activity classification, NACE-aligned).
- Names exist in **Greek** and often a **Latin/English** transliteration.

## GEMI company record — documented fields
```
gemi_number          - Αριθμός ΓΕΜΗ (register id)
afm                  - ΑΦΜ (tax id; join key)
name (EL / EN)       - επωνυμία (legal name; Greek + Latin)
legal_form           - νομική μορφή (ΑΕ/SA, ΕΠΕ/LLC, ΙΚΕ, ΟΕ, ΕΕ, branch, sole trader)
status               - κατάσταση (ΕΝΕΡΓΗ active / ΛΥΘΕΙΣΑ dissolved / ΥΠΟ ΕΚΚΑΘΑΡΙΣΗ liquidation / ...)
registered_address   - έδρα (registered seat)
kad                  - ΚΑΔ activity code(s)
incorporation_date   - ημερομηνία σύστασης
chamber              - επιμελητήριο (chamber of registration)
representatives      - νόμιμοι εκπρόσωποι / διοίκηση (directors/representatives) [PII]
filings              - ανακοινώσεις/καταχωρίσεις (announcements incl. financial statements)
```

## AADE RgWsPublic — documented fields (per ΑΦΜ, credentialed)
```
afm, name, registered_address, kad (activity codes incl. primary), doy (tax office ΔΟΥ),
firm/individual flag, activity status (active/ceased), start/stop dates
```

## Financial statements (ισολογισμοί / οικονομικές καταστάσεις) — document-based
```
balance sheet (ισολογισμός): assets / equity / liabilities
income statement: revenue, results
notes; auditor info; GA resolution approving the accounts
standard: ΕΛΠ (Greek GAAP) or IFRS ; currency EUR
```
- Published on the company's GEMI page as **PDF documents** — NOT structured open data. Require OCR/parsing or a
  commercial provider. Join on GEMI number / ΑΦΜ.

## Mapping to internal company model
```
company_id          <- gemi_number (register) ; cross-key afm
registration_number <- gemi_number
gemi_number         <- Αριθμός ΓΕΜΗ
tax_id              <- ΑΦΜ (AFM)
vat_id              <- EL + ΑΦΜ (validate via VIES)
legal_name          <- επωνυμία (keep Greek + Latin)
company_type        <- νομική μορφή (ΑΕ/ΕΠΕ/ΙΚΕ/ΟΕ/ΕΕ/...)
status              <- κατάσταση (active/dissolved/liquidation)
incorporation_date  <- ημερομηνία σύστασης
registered_address  <- έδρα
municipality/region <- from address (δήμος/περιφέρεια)
activity_code       <- ΚΑΔ (+ scheme)
officers[]          <- representatives/directors [PII]
financials[]        <- GEMI financial-statement PDFs (parse) | commercial provider [EUR]
country             <- "Greece"
source_url/name/at, raw_record
```
See `companies/data/greece/normalized/companies.sample.jsonl` (schematic — no per-company open record was
lawfully downloadable here).
