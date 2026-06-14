# Croatia — Schema Notes

No per-company open record was downloadable here (sudreg API key; FINA RGFI login); fields below are from the
documented Sudski registar OpenAPI and the RGFI standard forms. Join on OIB / MBS.

## Identifiers
- **OIB (Osobni identifikacijski broj)** — 11-digit personal/tax identification number; the company tax id
  and **the universal join key**. **VAT id = `HR` + OIB** (e.g. `HR12345678901`).
- **MBS (Matični broj subjekta)** — court-register entry number (Sudski registar key).
- **MB (matični broj)** — old 8-digit statistical number.

## Sudski registar (Court Register API) — documented fields
```
mbs                  - Matični broj subjekta (court register number)
oib                  - OIB (tax id) — VAT root
nadlezni_sud         - competent commercial court
tvrtka / naziv       - company name (full firm)
skraceni_naziv       - short name
pravni_oblik         - legal form (d.o.o., j.d.o.o., d.d., obrt, ...)
status               - status (active / in liquidation / deleted)
sjediste             - registered seat (mjesto/grad)
adresa               - registered address (ulica, kucni broj, ...)
temeljni_kapital     - share/registered capital (HRK historically; EUR since 2023)
predmet_poslovanja   - object of business / activities (+ NKD codes where present)
osobe                - persons (members, board, directors, signatories) [PII]
datum_osnivanja      - incorporation/registration date
```
- Query the API by **`tipIdentifikatora`** (`oib`|`mbs`) + **`identifikator`**; `subjekt` list endpoints
  support paging; `subjekt` detail endpoints (e.g. `subjekt_GetSubjectJavni`) return the full record.
- Legal forms: **d.o.o.** (LLC), **j.d.o.o.** (simple LLC), **d.d.** (JSC), **obrt** (sole trader/craft).

## FINA RGFI — Annual Financial Statements (CSV) — documented concepts
Structured CSV (balance sheet + income statement, abbreviated; notes), per razdoblje (fiscal year):
```
bilanca (balance sheet):
  ukupna aktiva (total assets), dugotrajna imovina (fixed assets),
  kratkotrajna imovina (current assets), kapital i rezerve (equity), obveze (liabilities)
racun dobiti i gubitka (income statement):
  ukupni prihodi / prihodi od prodaje (revenue), poslovni rezultat (operating result),
  dobit/gubitak razdoblja (net profit/loss for the period)
ostalo:
  prosjecan broj zaposlenih (average employees), velicina (mikro/mali/srednji/veliki)
```
- Keyed on **OIB** + **razdoblje** (year). **micro/small** disclose abbreviated forms (often no detailed
  P&L). Currency: **EUR** (Croatia adopted the euro on 2023-01-01; older years HRK). Open CSV (Otvorena
  dozvola); fuller/large data may need the paid FINA product.

## Mapping to internal company model
```
company_id          <- oib
registration_number <- mbs (court register)  [oib also]
tax_id / vat_id     <- "HR" + oib
legal_name          <- tvrtka / naziv
company_type        <- pravni_oblik (d.o.o./j.d.o.o./d.d./obrt)
status              <- status (aktivan / u likvidaciji / brisan)
incorporation_date  <- datum_osnivanja
registered_address  <- adresa + sjediste
municipality        <- sjediste (mjesto/grad)
region              <- županija (derive)
activity_code       <- NKD (Croatian NACE) from predmet_poslovanja where coded; else derive
share_capital       <- temeljni_kapital
officers/owners[]   <- osobe (members/board) [PII]
financials[]        <- FINA RGFI (bilanca + RDG), keyed by razdoblje
country             <- "Croatia"
source_url/name/at, raw_record
```
See `normalized/companies.sample.jsonl` (schematic — no per-company open record was downloadable here).
