# Estonia — Schema Notes

One authoritative open source (e-Business Register / RIK) with many datasets, all keyed on **registrikood**.
Company data, beneficial owners, shareholders and **structured financial statements** are all open (CC-BY 4.0).

## Identifiers
- **registrikood / ariregistri_kood** — 8-digit registry code; the universal join key. Also the bridge from the
  financial `report_id` (via the report metadata table).
- **KMKR** (kmkr_nr) — VAT number, `EE` + 9 digits (e.g. `EE101335276`); in the basic data.
- **EHAK** (asukoha_ehak_kood) — administrative-unit code for the address (maakond/vald/linn).
- **report_id** — financial report id; join key across the three financial layers.

## Company basic data (lihtandmed CSV) — observed fields (`;`-delimited, UTF-8 BOM)
```
nimi                                - legal name
ariregistri_kood                    - registrikood (company id)
ettevotja_oiguslik_vorm             - legal form (e.g. Osaühing = OÜ, Aktsiaselts = AS)
ettevotja_oigusliku_vormi_alaliik   - legal-form subtype
kmkr_nr                             - VAT number (EE...)
ettevotja_staatus                   - status code (R = Registrisse kantud / registered)
ettevotja_staatus_tekstina          - status text
ettevotja_esmakande_kpv             - first registration date (dd.mm.yyyy)
ettevotja_aadress                   - address (raw)
asukoha_ehak_kood / _tekstina       - EHAK admin code + text (maakond/linn)
ads_normaliseeritud_taisaadress     - normalized full address (ADS)
teabesysteemi_link                  - link to the public register card
```
General data (`yldandmed` JSON, 225 MB) carries deeper structured fields (capital, activities, contacts, etc.).

## Financial statements (majandusaasta aruanne) — STRUCTURED, three layers
### 1. Report metadata — `1.aruannete_yldandmed` (CSV `;`)
```
report_id, taidetud_aruanne_report_id, registrikood, õiguslik vorm, staatus,
aruandeaasta (report year), kas konsolideeritud? (consolidated Jah/Ei), period_start, period_end,
esitatud_kpv (submitted), kas auditeeritud? (audited Jah/Ei), valitud aruanne kategooria,
auditi töövõtu liik, audiitori otsuse tüüp (audit opinion), modifikatsioon..., Audiitorettevõtja (audit firm)
```
### 2. Financial line items — `4.{year}_aruannete_elemendid` (CSV `;`)
```
report_id, tabel (statement table, e.g. Lõppbilanss = closing balance sheet,
                   Kasumiaruanne = income statement),
elemendi_label (Estonian label, e.g. Käibevarad, Omakapital),
elemendi_nimetus (XBRL-like English element, e.g. CurrentAssets, Equity, TotalAnnualPeriodProfitLoss),
vaartus (numeric value, dot-decimal string; EUR)
```
### 3. Revenue breakdowns
```
2.EMTAK_myygitulu          - report_id, emtak (activity code), Jaotatud müügitulu, põhitegevusala, emtak_version
3.myygitulu_geograafiline  - report_id, geography, revenue
```
Join: `aruannete_elemendid.report_id` = `aruannete_yldandmed.report_id`; then `.registrikood` → company.
Years 2019–2025. Updated monthly. Currency EUR.

## Ownership & people (all open, CC-BY 4.0; PII / GDPR)
```
kasusaajad (beneficial owners)        - beneficial owner, control type, registrikood
osanikud (shareholders)               - shareholder name/id, share/contribution, registrikood
kaardile_kantud_isikud (persons/officers on the registry card) - board members etc., registrikood
```

## Other datasets
```
registrikaardid (registry cards), kommertspandid (commercial pledges), maarused (court rulings),
kandevalised_isikud, + Parquet versions of lihtandmed/yldandmed
```

## Mapping to internal company model
```
company_id          <- ariregistri_kood (registrikood)
registration_number <- ariregistri_kood
tax_id              <- (KMKR doubles as tax/VAT; no separate open tax id)
vat_id              <- kmkr_nr (EE...)
legal_name          <- nimi
company_type        <- ettevotja_oiguslik_vorm (+ alaliik)
status              <- ettevotja_staatus_tekstina (code ettevotja_staatus)
incorporation_date  <- ettevotja_esmakande_kpv (dd.mm.yyyy)
dissolution_date    <- (derived from status: liquidation/bankruptcy/deleted)
registered_address  <- ads_normaliseeritud_taisaadress
municipality/region <- asukoha_ehak_tekstina / EHAK
activity_code        <- EMTAK (from financial EMTAK_myygitulu / general data)
financials[]        <- aruannete_yldandmed + {year}_aruannete_elemendid (join via report_id) [EUR]
officers[]          <- kaardile_kantud_isikud [PII]
shareholders[]      <- osanikud [PII]
beneficial_owners[] <- kasusaajad [PII]
country             <- "Estonia"
source_url/name/at, raw_record
```
See `companies/data/estonia/normalized/companies.sample.jsonl` (real record: 007 Autohaus osaühing, 11694365).
