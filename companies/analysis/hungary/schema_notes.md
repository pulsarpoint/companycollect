# Hungary — Schema Notes

No per-company open record was lawfully downloadable (e-beszámoló search reCAPTCHA-gated; full cégjegyzék data
paid). Fields below are documented from the e-beszámoló / cégjegyzék data model and NAV. Join on the
**adószám** (8-digit base) across sources and the **cégjegyzékszám** on the register side.

## Identifiers
- **Cégjegyzékszám** — company registration number, format `NN-NN-NNNNNN`
  (court code `NN` – legal-form code `NN` – 6-digit serial). Register-side join key.
- **Adószám** — 11-digit tax number `XXXXXXXX-Y-ZZ`: 8-digit **törzsszám** (base) + 1 VAT code + 2 county code.
  The **8-digit base** is the universal stem across adószám, EU VAT and statistical code.
- **Közösségi adószám** (EU VAT) = `HU` + the 8-digit base (VIES).
- **Statisztikai számjel** — 17 digits: 8-digit base + 4-digit TEÁOR + 3-digit legal form + 2-digit county (KSH).
- **TEÁOR** — Hungarian activity classification (NACE-aligned).
- Names are Hungarian; legal-form suffixes: Kft (Ltd), Zrt (private plc), Nyrt (public plc), Bt, Kkt, Ev.

## e-beszámoló financial record — documented fields
```
cegjegyzekszam, adoszam, name, report_year
mérleg (balance sheet): eszközök/assets, saját tőke/equity, kötelezettségek/liabilities
eredménykimutatás (income statement): értékesítés nettó árbevétele/sales revenue, adózott eredmény/profit after tax
report documents: PDF + electronic form (XML)
currency: HUF (some entities EUR)
```
- Structured key figures are exposed in the free portal; full statements as PDF/XML. Search reCAPTCHA-gated.

## Cégjegyzék (register) record — documented fields
```
cegjegyzekszam, name, legal_form (Kft/Zrt/Nyrt/Bt/...), status (bejegyezve/törölve/...),
registered seat (székhely), main activity (TEÁOR), incorporation date,
[PAID/full:] officers/representatives (képviselők), owners (tulajdonosok), share capital, history
```
- Free basic info via e-cégjegyzék; officers/owners/history are paid.

## NAV áfaalany record — documented fields
```
adoszam, name, VAT status (áfaalany / áfás), validity dates, tax-number cancellation flag
```

## Mapping to internal company model
```
company_id          <- cegjegyzekszam (register) ; cross-key adoszam (8-digit base)
registration_number <- cegjegyzekszam
cegjegyzekszam      <- cégjegyzékszám (NN-NN-NNNNNN)
tax_id              <- adószám (11-digit)
tax_base_number     <- 8-digit törzsszám
vat_id              <- HU + 8-digit base (közösségi adószám; validate via VIES/NAV)
statistical_code    <- statisztikai számjel (17-digit)
legal_name          <- name
company_type        <- legal_form (Kft/Zrt/Nyrt/Bt/...)
status              <- status (bejegyezve active / törölve deleted)
incorporation_date  <- bejegyzés dátuma
registered_address  <- székhely
municipality/region <- from address (település/megye)
activity_code       <- TEÁOR (+ scheme)
officers[]          <- képviselők/tulajdonosok [PAID; PII]
financials[]        <- e-beszámoló key figures + statements (HUF) [reCAPTCHA-gated/manual | commercial provider]
country             <- "Hungary"
source_url/name/at, raw_record
```
See `companies/data/hungary/normalized/companies.sample.jsonl` (schematic — no per-company open record was
lawfully downloadable here).
