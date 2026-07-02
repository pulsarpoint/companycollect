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
