# Norway — schema notes

## Identifiers

- **organisasjonsnummer** — 9-digit Norwegian organisation number. Primary key for both
  Enhetsregisteret and Regnskapsregisteret. (Validated with mod-11 check digit.)
- **VAT number** — derived as `NO{orgnr}MVA` *only when* `registrertIMvaregisteret = true`.
- **overordnetEnhet** — parent organisation number (links sub-entity → parent, or subsidiary
  context). Group membership flagged by `erIKonsern`.

## Encodings / formats

- Encoding: UTF-8.
- Dates: ISO 8601 `YYYY-MM-DD` (e.g. `stiftelsesdato`, `registreringsdatoEnhetsregisteret`).
  Update-feed timestamps are full ISO 8601 with millis + `Z`.
- Numbers: financial figures are decimal in the account's reporting currency (`valuta`,
  often NOK but can be USD/EUR — Equinor reports in USD). Always store the currency.
- API JSON uses HAL (`_links`). Bulk CSV flattens nested fields with dot notation
  (e.g. `organisasjonsform.kode`, `forretningsadresse.kommune`).

## Enhetsregisteret — entity fields (base record)

| Field | Meaning |
|---|---|
| `organisasjonsnummer` | 9-digit org number (PK) |
| `navn` | Legal name |
| `organisasjonsform.kode` / `.beskrivelse` | Legal form (AS, ASA, ENK, NUF, ANS, …) |
| `naeringskode1/2/3.kode` / `.beskrivelse` | Industry codes (NACE/SN2007), up to 3 |
| `institusjonellSektorkode` | Institutional sector code |
| `antallAnsatte` / `harRegistrertAntallAnsatte` | Employee count |
| `forretningsadresse` | Business address (adresse[], poststed, postnummer, kommune, kommunenummer, land) |
| `postadresse` | Postal address (same shape) |
| `hjemmeside`, `telefon`, `mobil`, `epostadresse` | Contact (epost only in bulk) |
| `stiftelsesdato` | Incorporation/foundation date |
| `registreringsdatoEnhetsregisteret` | Date registered in CCR |
| `registrertIMvaregisteret` (+ dates) | VAT-registered flag |
| `registrertIForetaksregisteret` (+ date) | Registered in Business Enterprise register |
| `registrertIStiftelsesregisteret` / `…Frivillighetsregisteret` / `…Partiregisteret` | Other register flags |
| `sisteInnsendteAarsregnskap` | Year of last filed annual accounts (use to trigger financial fetch) |
| `konkurs`, `underAvvikling`, `underTvangsavviklingEllerTvangsopplosning` (+ dates) | Status flags |
| `erIKonsern`, `overordnetEnhet` | Group / parent linkage |
| `kapital` | Share capital: belop, antallAksjer, type, valuta, innfortDato |
| `maalform` | Language form (Bokmål/Nynorsk) |
| `vedtektsfestetFormaal`, `aktivitet` | Articles purpose / activity text |

## Regnskapsregisteret — financial record (per orgnr, array of accounts)

| Field path | Meaning |
|---|---|
| `regnskapsperiode.fraDato` / `.tilDato` | Accounting period start/end |
| `valuta` | Reporting currency (NOK/USD/EUR/…) |
| `regnskapstype` | SELSKAP (company) / KONSERN (group) |
| `oppstillingsplan` | Layout (store/små) |
| `regnkapsprinsipper.smaaForetak` / `.regnskapsregler` | Small-company flag, accounting rules |
| `revisjon.ikkeRevidertAarsregnskap` / `.fravalgRevisjon` | Audit flags |
| `resultatregnskapResultat.driftsresultat.driftsinntekter.sumDriftsinntekter` | Operating revenue |
| `resultatregnskapResultat.driftsresultat.driftsresultat` | Operating result |
| `resultatregnskapResultat.finansresultat.nettoFinans` | Net financial items |
| `resultatregnskapResultat.ordinaertResultatFoerSkattekostnad` | Pre-tax result |
| `resultatregnskapResultat.aarsresultat` | Net result (profit/loss) |
| `eiendeler.sumEiendeler` | Total assets |
| `eiendeler.omloepsmidler.sumOmloepsmidler` | Current assets |
| `eiendeler.anleggsmidler.sumAnleggsmidler` | Fixed assets |
| `egenkapitalGjeld.egenkapital.sumEgenkapital` | Total equity |
| `egenkapitalGjeld.gjeldOversikt.sumGjeld` | Total debt |
| `egenkapitalGjeld.gjeldOversikt.kortsiktigGjeld.sumKortsiktigGjeld` | Current liabilities |
| `egenkapitalGjeld.gjeldOversikt.langsiktigGjeld.sumLangsiktigGjeld` | Long-term liabilities |

## Mapping to internal company model

```
company_id            <- organisasjonsnummer
registration_number   <- organisasjonsnummer
tax_id / vat_id       <- "NO"+organisasjonsnummer+"MVA"  (if registrertIMvaregisteret)
legal_name            <- navn
normalized_name       <- normalize(navn)
company_type          <- organisasjonsform.kode (+ .beskrivelse)
status                <- derive: konkurs/underAvvikling/tvangs* -> dissolved|liquidating else active
incorporation_date    <- stiftelsesdato (fallback registreringsdatoEnhetsregisteret)
dissolution_date      <- konkursdato / underAvviklingDato (when set)
registered_address    <- forretningsadresse.adresse joined
municipality          <- forretningsadresse.kommune (+ kommunenummer)
region                <- derive from kommunenummer (county)
postal_code           <- forretningsadresse.postnummer
country               <- "Norway"
nace_code             <- naeringskode1.kode
website               <- hjemmeside
employees             <- antallAnsatte
parent_org            <- overordnetEnhet
source_name           <- "Brønnøysundregistrene"
source_url            <- entity self link
source_retrieved_at   <- fetch timestamp
raw_record            <- full JSON

-- financials (1:N by period, from Regnskapsregisteret)
fin_period_start/end  <- regnskapsperiode.fraDato/tilDato
fin_currency          <- valuta
revenue               <- ...driftsinntekter.sumDriftsinntekter
operating_result      <- ...driftsresultat.driftsresultat
net_result            <- resultatregnskapResultat.aarsresultat
total_assets          <- eiendeler.sumEiendeler
equity                <- egenkapitalGjeld.egenkapital.sumEgenkapital
total_debt            <- egenkapitalGjeld.gjeldOversikt.sumGjeld
```

## Gotchas

- Bulk CSV `enheter_alle` includes dissolved entities (1,458,299 rows) vs. 1,164,396 active via
  API search — filter on status if you only want live companies.
- Note typos in the *source* JSON keys, preserved as-is by Brreg:
  `sumInnskuttEgenkaptial`, `regnkapsprinsipper` — match exactly when parsing.
- Currency is per-account, not always NOK. Never assume NOK.
- `sisteInnsendteAarsregnskap` (in the base record) is the cheap signal for "new accounts
  available" — only call Regnskapsregisteret when it changes.
