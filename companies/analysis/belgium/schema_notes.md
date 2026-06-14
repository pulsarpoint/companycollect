# Belgium — Schema Notes

No per-company open record was downloadable here (free sources behind registration/API key); fields below
are from the documented KBO Open Data CSV structure and the NBB XBRL (Belgian GAAP) schemas. Join on the
Ondernemingsnummer.

## Identifiers
- **Ondernemingsnummer / Numéro d'entreprise** — 10-digit **enterprise number**; the primary key. **VAT id
  = `BE` + the 10 digits** (e.g. `BE0202239951`). Often formatted `0202.239.951`.
- **Vestigingseenheidsnummer / Numéro d'unité d'établissement** — 10-digit **establishment unit** number
  (starts with `2`). One enterprise has 1..N establishment units.
- Multilingual data: names/addresses carry a **language** (NL/FR/DE) and a **type**.

## KBO/BCE Open Data — CSV file set (documented)
```
enterprise.csv      - EnterpriseNumber, Status, JuridicalSituation, TypeOfEnterprise,
                      JuridicalForm, JuridicalFormCAC, StartDate
establishment.csv   - EstablishmentNumber, StartDate, EnterpriseNumber
denomination.csv    - EntityNumber, Language, TypeOfDenomination, Denomination
address.csv         - EntityNumber, TypeOfAddress, CountryNL/FR, Zipcode, MunicipalityNL/FR,
                      StreetNL/FR, HouseNumber, Box, ...
activity.csv        - EntityNumber, ActivityGroup, NaceVersion (2003/2008/2025), NaceCode, Classification
contact.csv         - EntityNumber, ContactType (TEL/EMAIL/WEB), Value
branch.csv          - Id, StartDate, EnterpriseNumber
code.csv            - Category, Code, Language, Description  (code -> label lookups)
meta.csv            - snapshot metadata (SnapshotDate, ExtractTimestamp, ExtractType, Version)
```
- `EntityNumber` is either an EnterpriseNumber or an EstablishmentNumber (so denomination/address/activity/
  contact attach to both enterprises and establishments).
- Codes (JuridicalForm, Status, NaceCode, ...) are resolved via `code.csv`.

## NBB Central Balance Sheet Office — annual accounts (XBRL) (documented)
Belgian GAAP filing schemas: **micro / abbreviated (verkort/abrégé) / full (volledig/complet)**; plus
consolidated. Structured XBRL concepts to expect (per boekjaar / exercice):
```
balans / bilan:
  totaal van de activa / total de l'actif    - total assets (balanstotaal)
  vaste activa / actifs immobilisés          - fixed assets
  vlottende activa / actifs circulants       - current assets
  eigen vermogen / capitaux propres          - equity
  schulden / dettes                          - liabilities
resultatenrekening / compte de résultats:
  omzet / chiffre d'affaires                 - revenue (full schema; often absent for micro/abbreviated)
  bedrijfswinst/-verlies / résultat d'exploitation - operating result
  winst/verlies van het boekjaar / bénéfice/perte de l'exercice - net result
toelichting / annexe:
  gemiddeld personeelsbestand / effectif moyen du personnel - average employees
```
- Identified by the **Ondernemingsnummer** + the **boekjaar/exercice** (period) + schema type.
- `micro`/`abbreviated` companies file reduced schemas → **revenue/operating result often absent**; `full`
  companies file the income statement. Currency EUR. XBRL is versioned yearly (taxonomy).
- Free **Authentic Data** = as filed; **Improved Data** (paid) = NBB-rectified.

## Mapping to internal company model
```
company_id          <- EnterpriseNumber (Ondernemingsnummer)
registration_number <- EnterpriseNumber
tax_id / vat_id     <- "BE" + EnterpriseNumber
legal_name          <- denomination.csv (TypeOfDenomination=social name; prefer one language consistently)
company_type        <- enterprise.JuridicalForm (resolve via code.csv)
status              <- enterprise.Status / JuridicalSituation (active/stopped/...) (+ Moniteur for events)
incorporation_date  <- enterprise.StartDate
dissolution_date    <- (from JuridicalSituation / Moniteur)
registered_address  <- address.csv (registered office)
municipality        <- address.MunicipalityNL/FR
region              <- derive from Zipcode/Municipality (Vlaanderen/Wallonie/Bruxelles)
activity_code       <- activity.NaceCode (NACE-BEL; keep NaceVersion)
establishments[]    <- establishment.csv (vestigingseenheidsnummer)
financials[]        <- NBB XBRL annual accounts, keyed by boekjaar
country             <- "Belgium"
source_url/name/at, raw_record
```
See `normalized/companies.sample.jsonl` (schematic — no per-company open record was downloadable here).
