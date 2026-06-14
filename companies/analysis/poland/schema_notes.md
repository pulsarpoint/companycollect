# Poland — Schema Notes

Observed from the real downloaded samples in `data/poland/raw/api/`. Three identifiers; join on KRS/NIP/REGON.

## Identifiers
- **KRS** — 10-digit National Court Register number (companies/NGOs). Primary key in KRS.
- **NIP** — 10-digit tax identifier. **VAT id = `PL` + NIP**. Primary tax key.
- **REGON** — statistical id: **9-digit** (legal unit) or **14-digit** (with 5-digit local-unit suffix).
  KRS returns 14-digit; the white list returns 9-digit — normalize to the 9-digit core.
- The **white list** bridges NIP ↔ REGON ↔ KRS in one record.

## KRS API — OdpisAktualny (verified) — `raw/api/krs_odpisaktualny_0000026438.json`
```
odpis.rodzaj                         - "Aktualny" (current) | "Pelny" (full/historical)
odpis.naglowekA                      - rejestr, numerKRS, dataRejestracjiWKRS, dates, numerOstatniegoWpisu
odpis.dane.dzial1:
  danePodmiotu.nazwa                 - legal name
  danePodmiotu.formaPrawna           - legal form (SPÓŁKA AKCYJNA, SPÓŁKA Z OGRANICZONĄ ODPOWIEDZIALNOŚCIĄ, ...)
  danePodmiotu.identyfikatory        - { regon (14), nip }
  siedzibaIAdres.siedziba            - { kraj, wojewodztwo, powiat, gmina, miejscowosc }
  siedzibaIAdres.adres               - { ulica, nrDomu, nrLokalu, kodPocztowy, miejscowosc, poczta }
  siedzibaIAdres.adresStronyInternetowej - website
  kapital                            - share capital (kapitalZakladowy etc.)
  umowaStatut                        - articles of association
odpis.dane.dzial2                    - reprezentacja/organy (board), wspolnicy (partners/shareholders) [anonymized PII]
odpis.dane.dzial3:
  przedmiotDzialalnosci.przedmiotPrzewazajacejDzialalnosci - PKD main: { kodDzial, kodKlasa, kodPodklasa, opis }
  przedmiotDzialalnosci.przedmiotPozostalejDzialalnosci    - PKD secondary[]
  wzmiankiOZlozonychDokumentach      - mentions of FILED financial docs (rocznego sprawozdania finansowego,
                                       opinii bieglego rewidenta, uchwaly o zatwierdzeniu, sprawozdania z dzialalnosci)
  informacjaODniuKonczacymRokObrotowy - fiscal year end (rok obrotowy)
odpis.dane.dzial4                    - zaleglosci (arrears), zabezpieczenia, wierzyciele
odpis.dane.dzial5                    - kurator
odpis.dane.dzial6                    - likwidacja, rozwiazanie, upadlosc, restrukturyzacja (-> status)
```

## VAT white list (verified) — `raw/api/whitelist_nip_5250007738.json`
```
result.subject:
  name, nip, regon, krs               - identity + the NIP<->REGON<->KRS bridge
  statusVat                           - "Czynny" (active) | "Zwolniony" (exempt) | not registered
  workingAddress / residenceAddress   - addresses
  accountNumbers[]                    - registered bank accounts (NRB/IBAN core)
  representatives[], partners[]        - representatives / partners
  registrationLegalDate               - VAT registration date
  removalDate / restorationDate        - VAT removal/restoration
```

## Financial statements (RDF e-Sprawozdania) — MF logical XML schema (documented)
```
Naglowek                              - okres (period), data od/do, typ jednostki (mikro/male/inne/banki/ubezp.)
Bilans (balance sheet):
  Aktywa: A (aktywa trwale), B (aktywa obrotowe), suma bilansowa (total assets)
  Pasywa: A (kapital wlasny / equity), B (zobowiazania i rezerwy / liabilities)
RachunekZyskowIStrat (income statement):
  przychody netto ze sprzedazy (revenue), koszty, zysk/strata z dzialalnosci operacyjnej,
  zysk/strata brutto, zysk/strata netto (net income)
InformacjaDodatkowa                   - notes (zatrudnienie / employees often here)
```
- Variant schemas by entity type + versioned yearly (MF publishes XSD). Some entities file PDF-only;
  listed/consolidated may use XBRL. Currency PLN (some report in thousands — check jednostka).

## Mapping to internal company model
```
company_id          <- KRS numerKRS (else NIP for non-KRS / sole traders)
registration_number <- KRS numerKRS
tax_id              <- NIP
vat_id              <- "PL" + NIP
regon               <- REGON (normalize to 9-digit core)
legal_name          <- danePodmiotu.nazwa
company_type        <- danePodmiotu.formaPrawna
status              <- derive from dzial6 (likwidacja/upadlosc) + white list statusVat ; else aktywny
incorporation_date  <- naglowekA.dataRejestracjiWKRS
dissolution_date    <- dzial6 (wykreslenie/rozwiazanie)
registered_address  <- siedzibaIAdres.adres (ulica+nrDomu+kodPocztowy+miejscowosc)
municipality        <- siedziba.miejscowosc
region              <- siedziba.wojewodztwo
activity_code       <- dzial3 PKD (kodDzial.kodKlasa.kodPodklasa)
website             <- siedzibaIAdres.adresStronyInternetowej
bank_accounts[]     <- white list accountNumbers
financials[]        <- RDF e-Sprawozdania (bilans + rachunek zyskow i strat), keyed by rok obrotowy
beneficial_owners[] <- CRBR (by NIP)
country             <- "Poland"
source_url/name/at, raw_record
```
See `normalized/companies.sample.jsonl` (real, built from the KRS + white-list samples).
