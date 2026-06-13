# Regnskapsregisteret (financial accounts) Field Catalog

## Source Summary

- Country: Norway
- Source type: official_registry_api (per-orgnr lookup)
- Organization: Brønnøysund Register Centre (Brreg)
- URL: https://data.brreg.no/regnskapsregisteret/regnskap/{orgnr}
- License: NLOD 2.0 (open API distribution; labelled "temporary/research")
- Access: public (no auth)
- Freshness: ~5 imports/week; annual filing cycle (deadline ~July 31)
- Record shape: **JSON array**, one element per filed period × accounts type (SELSKAP/KONSERN)
- Primary keys: `virksomhet.organisasjonsnummer` + `regnskapsperiode.tilDato` + `regnskapstype`
- Join keys: `virksomhet.organisasjonsnummer` (→ brregenhet)

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| [].id | id | Filing id | integer | filing | 5667197 | |
| [].journalnr | journalnr | Journal number | string | filing | 2025428073 | |
| [].regnskapstype | regnskapstype | SELSKAP / KONSERN | string | filing | SELSKAP | company vs consolidated |
| [].virksomhet.organisasjonsnummer | ... | Org number | string | identifier | 923609016 | join key |
| [].virksomhet.organisasjonsform | ... | Legal form | string | legal_form | ASA | |
| [].virksomhet.morselskap | ... | Is parent company | boolean | relationship | true | |
| [].regnskapsperiode.fraDato | fraDato | Period start | date | date | 2024-01-01 | |
| [].regnskapsperiode.tilDato | tilDato | Period end (FY end) | date | date | 2024-12-31 | natural key part |
| [].valuta | valuta | Currency | string | financial | USD | **never assume NOK** |
| [].avviklingsregnskap | avviklingsregnskap | Liquidation accounts | boolean | filing | false | |
| [].oppstillingsplan | oppstillingsplan | Layout (store/små) | string | filing | store | |
| [].revisjon.ikkeRevidertAarsregnskap | ... | Not audited | boolean | filing | false | |
| [].revisjon.fravalgRevisjon | ... | Opted out of audit | boolean | filing | false | |
| [].regnkapsprinsipper.smaaForetak | smaaForetak | Small enterprise | boolean | filing | false | **source key typo** |
| [].regnkapsprinsipper.regnskapsregler | regnskapsregler | Accounting framework | string | filing | forenkletAnvendelseIFRS | |
| ...driftsinntekter.sumDriftsinntekter | sumDriftsinntekter | Operating revenue | decimal | financial | 72543000000 | turnover |
| ...driftskostnad.sumDriftskostnad | sumDriftskostnad | Operating costs | decimal | financial | 62196000000 | |
| ...driftsresultat.driftsresultat | driftsresultat | Operating result | decimal | financial | 10347000000 | EBIT-like |
| ...finansresultat.nettoFinans | nettoFinans | Net financial items | decimal | financial | -2179000000 | |
| ...ordinaertResultatFoerSkattekostnad | ... | Pre-tax result | decimal | financial | 8168000000 | |
| ...aarsresultat | aarsresultat | Net result | decimal | financial | 8141000000 | bottom line |
| [].eiendeler.sumEiendeler | sumEiendeler | Total assets | decimal | financial | 109150000000 | |
| ...omloepsmidler.sumOmloepsmidler | ... | Current assets | decimal | financial | 45079000000 | anleggsmidler=fixed |
| [].egenkapitalGjeld.sumEgenkapitalGjeld | ... | Total equity+liabilities | decimal | financial | 109150000000 | = total assets |
| ...egenkapital.sumEgenkapital | sumEgenkapital | Total equity | decimal | financial | 41090000000 | |
| ...gjeldOversikt.sumGjeld | sumGjeld | Total debt | decimal | financial | 68060000000 | |
| ...kortsiktigGjeld.sumKortsiktigGjeld | ... | Current liabilities | decimal | financial | 42024000000 | langsiktig=long-term |

## Interpretation Notes

- **This is the financial data source** requested. One GET per org number returns an array of
  annual accounts — income statement (`resultatregnskapResultat`) and balance sheet
  (`eiendeler` + `egenkapitalGjeld`) with totals and sub-totals.
- **Array, not object**: an entity can have multiple elements per call — different years and/or
  `regnskapstype` (SELSKAP company-only vs KONSERN consolidated). Natural key =
  org number + `regnskapsperiode.tilDato` + `regnskapstype`.
- **Currency varies**: `valuta` can be NOK, USD (Equinor), EUR, etc. Persist currency with every
  figure; never assume NOK.
- **Source-side typos preserved**: `regnkapsprinsipper` (group key) and `sumInnskuttEgenkaptial`
  (under innskuttEgenkapital) are misspelled in the API — match the exact strings when parsing.
- **Coverage**: ~80% of accounting-liable entities (AS, ASA, NUF, savings banks). Banks/insurance
  excluded from standard figures. Sole proprietors generally don't file. Open API ~2018 onward;
  historical depth in the open API is shallow (recent year(s)).
- **Stability caveat**: Brreg labels the open API a "temporary/research" distribution. The
  guaranteed long-term channel for full history + scanned image copies is the paid Subscription
  Service (XML/TIF) — out of scope for open ingestion.
- **Refresh strategy**: use `sisteInnsendteAarsregnskap` from `brregenhet` as the cheap signal to
  decide when to re-fetch financials for an org number.
- All descriptive text is Norwegian; English names are helper metadata only.
