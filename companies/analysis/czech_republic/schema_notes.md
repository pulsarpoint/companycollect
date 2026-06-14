# Czech Republic — Schema Notes

Two official open sources, both keyed on **IČO**. ARES API gives a clean aggregated record; the Justice VR bulk
gives the deepest structured register. Financial statements are separate PDF documents in the Sbírka listin.

## Identifiers
- **IČO** (Identifikační číslo osoby) — 8-digit company id and the universal join key.
  - Watch **zero-padding**: ARES returns padded (`00006947`); Justice XML may store it unpadded (`3431509` =
    `03431509`). Normalize to 8 digits.
- **DIČ** — tax/VAT id, normally `CZ` + IČO (e.g. `CZ27082440`). Validate via Registr DPH / VIES.
- **spisová značka** — court file mark: soud (court) + oddíl + vložka, e.g. `B 20032/MSPH`.

## ARES API record (JSON) — observed fields
```
ico                  - 8-digit company id (padded)
obchodniJmeno        - business/legal name
dic                  - VAT/tax id (CZ + IČO)
pravniForma          - legal-form code (e.g. 121 = a.s., 112 = s.r.o., 325 = org. složka státu)
sidlo                - structured registered office:
                         textovaAdresa, kodKraje/nazevKraje, kodObce/nazevObce, kodOkresu, nazevUlice,
                         cisloDomovni/cisloOrientacni, psc, kodAdresnihoMista (RUIAN)
czNace2008[]         - CZ-NACE activity codes
seznamRegistraci     - per-register status (stavZdrojeRos/Res/Vr/Rzp/Dph = AKTIVNI / NEEXISTUJICI / HISTORICKY)
datumVzniku          - incorporation date
datumZaniku          - dissolution date (if any)
datumAktualizace     - last update
primarniZdroj        - primary source register (ros/res/vr/rzp)
```
Search: `POST /vyhledat {start, pocet, obchodniJmeno|ico|sidlo...}` → `{pocetCelkem, ekonomickeSubjekty[]}`.

## Justice VR bulk (XML) — record shape
Each company is a `<Subjekt>`; typed data items are `<Udaj>` keyed by `udajTyp/kod`.
```
Subjekt: nazev, ico, zapisDatum (registration date)
Udaj[] by udajTyp/kod:
  NAZEV                - name
  ICO                  - IČO
  PRAVNI_FORMA         - legal form (kod/nazev/zkratka, e.g. as / s.r.o.)
  SIDLO                - registered office (adresa: obec, castObce, ulice, cisloPo/cisloOr, psc, okres)
  ZAKLADNI_KAPITAL     - share/registered capital (vklad: typ=KORUNY + textValue; splaceni % paid)
  PREDMET_PODNIKANI    - scope of business (free text)   | PREDMET_CINNOSTI - activity
  STATUTARNI_ORGAN(_CLEN) - board members (osoba: jmeno, prijmeni, narozDatum=DOB, funkce, clenstviOd) [PII]
  DOZORCI_RADA(_CLEN)  - supervisory board [PII]
  AKCIE / AKCIONAR     - shares / SHAREHOLDERS (present for a.s.)
  POCET_CLENU          - number of board members
  SPIS_ZN              - court file mark (soud kod/nazev, oddil, vlozka)
  LIKVIDATOR(_OSOBA)   - liquidator
  INSOLVENCE / INSOLVENCNI_ZAPIS - insolvency records
  ZPUSOB_JEDNANI       - manner of acting on behalf of the company
```
Packages: `{sro|as|pobspolek|sf|...}-{full|actual}-{court}-{year}`. `full` = all historical records;
`actual` (Platný výpis) = current valid extract. Resources: `.xml`, `.csv`, `.xml.gz`, `.csv.gz`.

## Sbírka listin — financial statements (account documents)
```
účetní závěrka:   rozvaha (balance sheet), výkaz zisku a ztráty (income statement), příloha (notes)
výroční zpráva:   annual report
zpráva auditora:  auditor report (where audit required)
```
- **Document-based PDF** (native or scanned). Free to view at `or.justice.cz` (vypis-sl-firma?subjektId=…).
- **NOT structured open data** — no official XBRL/CSV. Currency **CZK**. Structured figures need OCR/parsing
  or a commercial provider. Join to the company via subjektId ↔ IČO.

## Mapping to internal company model
```
company_id          <- IČO (normalize to 8 digits)
registration_number <- IČO
tax_id              <- DIČ (CZ + IČO)
vat_id              <- DIČ (same; validate via Registr DPH/VIES)
legal_name          <- obchodniJmeno (ARES) | nazev/NAZEV (Justice)
company_type        <- pravniForma (ARES code) | PRAVNI_FORMA (Justice nazev/zkratka)
status              <- seznamRegistraci (ARES) | derived (active unless ZANIK/likvidace/insolvence)
incorporation_date  <- datumVzniku (ARES) | zapisDatum (Justice)
dissolution_date    <- datumZaniku (ARES)
registered_address  <- sidlo.textovaAdresa (ARES) | SIDLO/adresa (Justice)
municipality        <- sidlo.nazevObce / adresa.obec
region              <- sidlo.nazevKraje (NUTS3) / adresa.okres (district)
activity_code       <- czNace2008 (ARES) | PREDMET_PODNIKANI (Justice, free text)
share_capital       <- ZAKLADNI_KAPITAL (Justice; CZK)
officers[]          <- STATUTARNI_ORGAN_CLEN / DOZORCI_RADA_CLEN (Justice) [PII: includes DOB]
shareholders[]      <- AKCIONAR (Justice, a.s.) [PII]
financials[]        <- účetní závěrka (Sbírka listin PDF; parse) | commercial provider
court_file_mark     <- SPIS_ZN (soud/oddil/vlozka)
country             <- "Czech Republic"
source_url/name/at, raw_record
```
See `companies/data/czech_republic/normalized/companies.sample.jsonl` (real record: CR Holding a.s., IČO 3431509).
