# Sweden — schema notes & mapping to internal company model

## Identifier model

- `organisationsnummer` / `PeOrgNr` is the primary company identifier.
- Store a normalized digits-only value and keep the raw source value.
- Bolagsverket raw values can include embedded suffixes, for example:

```text
8888006577$ORGNR-IDORG
Stiftelsen ...$FORETAGSNAMN-ORGNAM$1993-03-15
```

Split these into normalized value, source code/type, and any embedded date where relevant.

## Bolagsverket bulk file

File:

```text
data_model/bolagsverket_bulkfil.txt
```

Format:

- UTF-8.
- Semicolon CSV.
- 11 columns.

Observed columns and likely mapping:

| Source column | Meaning / mapping |
|---|---|
| `organisationsidentitet` | company id / registration number, with raw suffix |
| `namnskyddslopnummer` | name-protection sequence number |
| `registreringsland` | registration country |
| `organisationsnamn` | legal/company name, with raw suffix metadata |
| `organisationsform` | legal form code |
| `avregistreringsdatum` | deregistration/dissolution date |
| `avregistreringsorsak` | deregistration reason |
| `pagandeAvvecklingsEllerOmstruktureringsforfarande` | ongoing liquidation/restructuring indicator |
| `registreringsdatum` | incorporation/registration date |
| `verksamhetsbeskrivning` | business/activity description |
| `postadress` | postal address packed as delimited text |

## SCB bulk file

File:

```text
data_model/scb_bulkfil_JE_20260629T055245_80.txt
```

Format:

- Latin-1 / ISO-8859.
- Tab-separated.
- 35 columns, including a trailing empty header in the local sample.

Observed columns and likely mapping:

| Source column | Meaning / mapping |
|---|---|
| `PeOrgNr` | company/person organization identifier |
| `Namn` / `Foretagsnamn` | company/name fields |
| `JurForm` | legal form code |
| `FtgStat` / `JEStat` | status flags |
| `COAdress`, `Gatuadress`, `PostNr`, `PostOrt` | address fields |
| `Ng1`..`Ng5` | SNI/activity codes |
| `RegDatKtid` | registration date, observed as `YYYYMMDD` |
| `Reklamsparrtyp` | advertising/direct-marketing block type |
| `m*` columns | marker/change/metadata flags for corresponding fields |

## Annual-report archives

Files:

```text
data_model/01_1.zip
data_model/annual_reports_01_1/*.zip
```

Observed structure:

```text
01_1.zip
  5560187493_2025-06-30.zip
    <uuid>.xhtml
    <uuid>.xhtml
```

Nested ZIP filename gives:

```text
org_number = 5560187493
financial_period_end = 2025-06-30
```

XHTML files include inline XBRL concepts. Observed examples:

```text
se-cd-base:RakenskapsarForstaDag
se-cd-base:RakenskapsarSistaDag
se-cd-base:Organisationsnummer
se-cd-base:ForetagetsNamn
se-gen-base:Nettoomsattning
```

## Mapping to internal company model

```text
company_id              <- normalized organisationsnummer / PeOrgNr
registration_number     <- same as company_id
legal_name              <- Bolagsverket organisationsnamn, SCB fallback
company_type            <- Bolagsverket organisationsform, SCB JurForm fallback
status                  <- derived from avregistreringsdatum, avregistreringsorsak, FtgStat, JEStat
incorporation_date      <- Bolagsverket registreringsdatum, SCB RegDatKtid fallback
dissolution_date        <- Bolagsverket avregistreringsdatum
registered_address      <- Bolagsverket postadress, SCB address fallback
activity_description    <- Bolagsverket verksamhetsbeskrivning
industry_codes          <- SCB Ng1..Ng5
financials[]            <- parsed annual-report iXBRL facts
source_retrieved_at     <- raw ZIP retrieval time
raw_record              <- full raw row / raw iXBRL fact metadata
```

## Parser requirements

- Preserve raw rows.
- Normalize org numbers consistently.
- Decode SCB as Latin-1.
- Parse Bolagsverket packed fields without losing raw suffix metadata.
- Store raw annual-report archive path, nested ZIP path, XHTML filename, and concept QName for every
  financial fact.

## Authenticated API contract additions

The public `VärdefullaDatamängder` OpenAPI v1 contract was inspected on
2026-08-17. It adds important semantics even though the supplied OAuth client
was rejected:

- The registration key can be
  `(identitetsbeteckning, namnskyddslopnummer)`. Identity alone is not unique
  for sole traders with several protected business names/registrations.
- `organisationsdatum.registreringsdatum` and
  `organisationsdatum.infortHosScb` are distinct dates. Do not map the SCB date
  to incorporation without retaining its source meaning.
- `/dokumentlista` adds `dokumentId`, `filformat`,
  `rapporteringsperiodTom`, and `registreringstidpunkt`.
- Most response groups include both `dataproducent` and a nullable `fel`
  object. A producer error means unknown/unavailable, not a confirmed negative.

Detailed field catalog:

```text
data_model/sources/bolagsverket_vardefulla_datamangder_api/source_field_catalog.json
```

## Cross-country company-signal projection

The main list needs true tri-state values:

```text
has_financial_data       Nullable(UInt8)
has_government_contract  Nullable(UInt8)
is_publicly_traded       Nullable(UInt8)
```

Semantics:

```text
1    confirmed positive
0    source checked; no result inside its declared scope
NULL unknown, unsupported, unmatchable, or stale
```

Do not use `0` for gray. Store shared country/source scope in a separate
coverage table rather than repeating caveat strings on every company:

```text
company_signal_coverage
  country_code
  signal_name
  coverage_status
  coverage_from
  coverage_to
  source_slugs
  source_updated_at
  caveat
  resolved_at
```

Useful main-list summaries:

```text
financial_latest_year
public_award_count
public_award_last_date
listing_venue_count
listing_markets
signals_resolved_at
```

## Public-procurement source model

Keep source rows before deriving the company signal:

```text
se_procurement_awards
  source_slug
  source_procurement_id
  source_lot_id
  source_advertising_database
  publication_date
  title
  agreement_type
  contracted
  buyer_name
  buyer_id_raw
  buyer_id_normalized
  supplier_name
  supplier_id_raw
  supplier_id_normalized
  cpv_code
  source_record_id
  source_payload_hash
  resolved_at
```

The existing generic TED tables remain separate. Build
`company_public_procurement_summary` from the union so original provenance and
cross-source deduplication remain possible.

## Public-listing source model

Use one row per equity instrument/listing:

```text
company_listings
  country_code
  company_id
  issuer_lei
  isin
  ticker
  instrument_name
  instrument_type
  venue_name
  mic
  market_type
  segment
  admission_date
  termination_date
  trading_status
  is_current
  source_slug
  source_record_id
  source_retrieved_at
```

Resolve Swedish companies through authoritative identifiers:

```text
EODHD symbol
  -> EODHD ISIN
  -> GLEIF daily ISIN-to-LEI relationship
  -> GLEIF registered_as
  -> 10-digit company_id

FIRDS ISIN + issuer LEI + MIC
  -> GLEIF registered_as
  -> 10-digit company_id
```

Name-only matching is a manual-review fallback, not an automatic production
join.

Add explicit match/status provenance:

```text
identity_match_method       eodhd_isin_gleif | firds_issuer_lei | direct_orgnr
identity_match_confidence   high | medium | unresolved
listing_status_source       eodhd | esma_firds | venue
source_status               active | delisted | admitted | terminated | cancelled
```

The first derived EODHD layer should retain unmatched rows:

```text
eodhd_company_listings
  eodhd_symbol_key
  company_id Nullable(String)
  isin Nullable(String)
  issuer_lei Nullable(String)
  mic Nullable(String)
  instrument_type
  is_delisted
  identity_match_method
  identity_match_confidence
  unresolved_reason
  source_run_id
  resolved_at
```

Do not overwrite EODHD status with FIRDS status. Reconcile both into
`company_listings` while retaining their separate source observations.
