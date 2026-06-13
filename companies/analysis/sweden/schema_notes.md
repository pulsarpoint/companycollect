# Sweden — schema notes & mapping to internal company model

## Identifiers

- **organisationsnummer** — 10-digit Swedish org number (often written `NNNNNN-NNNN`). Primary key for
  companies/legal entities. For sole traders this is the **personnummer**.
- **CFAR-nummer** (SCB) — 8-digit **workplace / local-unit (arbetsställe)** identifier. One company can
  have many CFAR workplaces. Use for site/establishment-level coverage.
- **momsregistreringsnummer / VAT** — `SE` + 12 digits (orgnr + `01`), where applicable.

## Source 1 — Bolagsverket Värdefulla datamängder (base data + financials)

`POST /organisationer` (by organisationsnummer) → JSON. Observed/expected fields:

```
organisationsnummer
organisationsnamn            (legal name)
juridisk_form / organisationsform   (Aktiebolag (AB), Handelsbolag (HB), Kommanditbolag (KB),
                                     Enskild firma, Ekonomisk förening, ...)
status                       (registered / deregistered / bankruptcy / liquidation ...)
postadress_organisation      (postal address: street, postnr, postort)
naringsgrenskod / SNI        (industry code, SNI 2025)
registrering / avregistrering datum
```

`POST /dokumentlista` (by org) → list of available **annual-report documents** (document ids + metadata
such as filing year, type). `GET /dokument/{id}` → **ZIP** containing the report in **iXBRL**.

### Financial data (iXBRL annual report)

iXBRL = inline XBRL; tags map to the official **Swedish K2/K3 taxonomies** at `taxonomier.se`.
Parse to obtain at least:

```
# Income statement (resultaträkning)
Nettoomsättning                 (net revenue)
Rörelseresultat                 (operating profit/EBIT)
Resultat efter finansiella poster
Årets resultat                  (net profit for the year)

# Balance sheet (balansräkning)
Summa tillgångar                (total assets)
Summa eget kapital              (total equity)
Summa skulder                   (total liabilities)
Omsättningstillgångar / Anläggningstillgångar
Kortfristiga / Långfristiga skulder

# Meta
Räkenskapsår (financial year), valuta (currency, usually SEK), revisor/audit, K2-vs-K3 regelverk
```

Notes: free coverage = companies that filed **digitally**; quantify historical gaps. No pre-computed
ratios — derive (e.g. soliditet = equity/assets) downstream.

## Source 2 — SCB Företagsdatabasen (FDB) free API (register + workplaces)

REST JSON/XML. Field docs: `postbeskrivning-foretag.pdf`, `postbeskrivning-arbetsstalle.pdf`,
`variabelbeskrivning-api-sni-2025.pdf`. Observed/expected fields:

```
# Company (företag)
organisationsnummer / personnummer
företagsnamn
juridisk form
adress (postal/visit), postnr, postort, kommun, län
SNI-kod (industry)
register flags: moms (VAT), arbetsgivare (employer), F-skatt
storleksklass anställda (employee size-class)

# Workplace (arbetsställe / local unit)
CFAR-nummer
arbetsställets namn + adress, kommun, län
SNI-kod (workplace level)
storleksklass anställda
huvud-/del-arbetsställe (main vs subsidiary)
koppling till organisationsnummer (parent company)
```

No financial statements in SCB.

## Mapping to internal company model

```
company_id           <- organisationsnummer (Bolagsverket/SCB)
registration_number  <- organisationsnummer
tax_id / vat_id      <- VAT (SE + orgnr + 01) where present
legal_name           <- organisationsnamn / företagsnamn
normalized_name      <- normalized(legal_name)
company_type         <- juridisk_form / organisationsform (AB, HB, KB, EF, Enskild firma, ...)
status               <- status (registered/deregistered/bankruptcy/liquidation)
incorporation_date   <- registreringsdatum
dissolution_date     <- avregistreringsdatum
registered_address   <- postadress (street, postnr, postort)
municipality         <- kommun
region               <- län
country              <- "Sweden"
industry_code        <- SNI (company; + workplace-level from SCB)
employees            <- storleksklass anställda (SCB; size-class, not exact)
local_units[]        <- CFAR workplaces (SCB): {cfar, name, address, kommun, sni, size_class}
financials[]         <- per fiscal year from iXBRL annual report (Bolagsverket):
                        {year, currency, net_revenue, ebit, profit_after_fin, net_profit,
                         total_assets, total_equity, total_liabilities, audited, regelverk}
source_name          <- "Bolagsverket Värdefulla datamängder" / "SCB Företagsregistret"
source_url           <- gw.api.bolagsverket.se/... / SCB web service
source_retrieved_at  <- ISO-8601 UTC
raw_record           <- original JSON / parsed-iXBRL JSON
```

## Encoding / format notes

- Encoding: UTF-8; Swedish characters å/ä/ö.
- Dates: ISO `YYYY-MM-DD`.
- Currency: financials usually **SEK** (check `valuta` tag; some report in EUR/USD).
- orgnr canonical form: store digits-only (10) plus a display form `NNNNNN-NNNN`.
- iXBRL: parse with an XBRL/iXBRL parser keyed to the K2/K3 taxonomy concept names; keep raw + mapped.
