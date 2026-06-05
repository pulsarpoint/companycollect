# Sweden (SE) - GDP Rank #24

> **Summary:** Sweden HVD company data is available as direct bulk ZIP downloads from Bolagsverket. Use two files: Bolagsverket's company-registration file for legal identity, registration, deregistration, activity description, and postal address; and SCB's company-register file for activity status, SNI codes, address, legal form, and register status. Organisation number format is 10 digits, often displayed as XXXXXX-XXXX.

## Official Registry

### Sweden HVD Bulk Downloads
| Field      | Detail |
|------------|--------|
| URLs       | `https://vardefulla-datamangder.bolagsverket.se/bolagsverket/bolagsverket_bulkfil.zip` and `https://vardefulla-datamangder.bolagsverket.se/scb/scb_bulkfil.zip` |
| Access     | Bulk download (free) |
| Cost       | Free |
| Auth       | None |
| Rate limit | Response headers expose rate-limit values; workflow should avoid repeated downloads by comparing hashes. |
| Notes      | These direct ZIP URLs are the source URLs for `se_workflow`. The public Bolagsverket landing pages can return anti-bot HTML and should not be scraped for ingestion. |

### SCB — Statistics Sweden (Statistiska centralbyrån)
| Field      | Detail |
|------------|--------|
| URL        | https://vardefulla-datamangder.bolagsverket.se/scb/scb_bulkfil.zip |
| Access     | Bulk ZIP file |
| Cost       | Free |
| Fields     | `PeOrgNr`, `Namn`, `Foretagsnamn`, `FtgStat`, `JEStat`, `JurForm`, `COAdress`, `Gatuadress`, `PostNr`, `PostOrt`, `Ng1`-`Ng5`, `RegDatKtid`, `Reklamsparrtyp`, plus mask columns prefixed with `m`. |
| Auth       | None |
| Rate limit | Not documented; avoid repeated downloads by file hash. |
| Notes      | File observed on 2026-06-05: ZIP contains `scb_bulkfil_JE_20260601T065839_76.txt`, ISO-8859-1, tab-separated, 34 columns, 1,813,820 logical records. `FtgStat` values: `0` never active, `1` active by SCB criteria, `9` not active. For legal persons, `PeOrgNr` can begin with `16` followed by the 10-digit organisation number; preserve the raw value and derive the 10-digit number only when the pattern is valid. |

### Bolagsverket — Swedish Companies Registration Office
| Field      | Detail |
|------------|--------|
| URL        | https://vardefulla-datamangder.bolagsverket.se/bolagsverket/bolagsverket_bulkfil.zip |
| Access     | Bulk ZIP file |
| Cost       | Free |
| Fields     | `organisationsidentitet`, `namnskyddslopnummer`, `registreringsland`, `organisationsnamn`, `organisationsform`, `avregistreringsdatum`, `avregistreringsorsak`, `pagandeAvvecklingsEllerOmstruktureringsforfarande`, `registreringsdatum`, `verksamhetsbeskrivning`, `postadress`. |
| Auth       | None |
| Rate limit | Not documented; avoid repeated downloads by file hash. |
| Notes      | File observed on 2026-06-05: ZIP contains `bolagsverket_bulkfil.txt`, UTF-8, semicolon-separated quoted CSV, 11 columns, 2,958,873 logical records. Use a real CSV parser because quoted text fields can contain delimiters and line breaks. `organisationsidentitet`, `organisationsnamn`, and `postadress` contain embedded tagged values separated by `$`; preserve the raw value and parse normalized components separately. |

## HVD Processing Plan

### Source config
Configure `se_workflow` with both HVD datasets:

```json
{
  "datasets": [
    {
      "key": "bolagsverket",
      "url": "https://vardefulla-datamangder.bolagsverket.se/bolagsverket/bolagsverket_bulkfil.zip",
      "format": "zip",
      "encoding": "utf-8",
      "delimiter": ";",
      "zip_entry": "bolagsverket_bulkfil.txt"
    },
    {
      "key": "scb",
      "url": "https://vardefulla-datamangder.bolagsverket.se/scb/scb_bulkfil.zip",
      "format": "zip",
      "encoding": "iso-8859-1",
      "delimiter": "\\t",
      "zip_entry_pattern": "scb_bulkfil_JE_*.txt"
    }
  ]
}
```

### Workflow actions
1. Verify both source files exist by requesting the direct ZIP URLs and checking for `application/zip` content.
2. Compare each remote file with the latest downloaded file for that dataset. Use the downloaded file SHA-256 as the canonical duplicate guard; headers such as `etag` and `last-modified` can be recorded as metadata but should not replace content hashing.
3. Download only datasets whose content hash has not already been processed.
4. Stream each ZIP entry and parse it into `se_workflow.raw_records`. Store one raw record per logical CSV row with dataset key, source URL, ZIP entry name, file hash, row number, raw source payload, and ingest metadata.

### Raw parsing rules
- Bolagsverket file: UTF-8, semicolon-delimited, quoted CSV.
- SCB file: ISO-8859-1, tab-delimited text.
- Do not use physical newline counts as record counts. Both files must be parsed through a CSV reader.
- Preserve every source column in the raw payload before normalization.
- Keep source code values and add normalized English display columns in `se_source` where codes can be translated.

### Normalization notes
- `organisationsidentitet`: split tagged values like `8888006510$ORGNR-IDORG` into raw value, identifier value, and identifier type.
- `organisationsnamn`: keep the raw tagged string and derive current organisation name, name type, and associated date when present.
- `postadress`: split `$`-separated components into street/address line, care-of or secondary line when present, city, postal code, and country code where possible.
- `organisationsform` and `JurForm`: preserve raw legal-form codes and add normalized legal-form labels plus `_en` labels.
- `avregistreringsorsak` and `pagandeAvvecklingsEllerOmstruktureringsforfarande`: preserve raw codes and dates, then map to normalized status/procedure tables.
- `Ng1`-`Ng5`: store SNI activity codes as ordered activity-code rows.
- `FtgStat`, `JEStat`, and `Reklamsparrtyp`: preserve raw values and normalize to status/reference tables with `_en` labels.
- `ftgstat_oppna.csv` is not part of this HVD pipeline. It is aggregate monthly company statistics by municipality, event type, and legal form; it does not contain company-level records.

### Allabolag.se (unofficial aggregator — widely used)
| Field      | Detail |
|------------|--------|
| URL        | https://www.allabolag.se |
| Access     | Scrape / Web portal (no official API) |
| Cost       | Free |
| Fields     | Organisation number, company name, board members, annual reports, financial key figures, address, SNI codes |
| Auth       | None for basic search |
| Rate limit | Undocumented; scraping feasible with care |
| Notes      | Not official but widely used as a free company data source in Sweden. Sources from Bolagsverket + annual reports. Useful for prototype/enrichment; not for production compliance use. |

## Commercial Providers

### Bisnode Sweden (Dun & Bradstreet)
| Field      | Detail |
|------------|--------|
| URL        | https://www.bisnode.se |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | Org number, credit score, financials (annual accounts), directors, group structure, payment behaviour, compliance screening |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Dominant commercial credit bureau in Sweden. Bisnode is a D&B affiliate. Strong financial data including digitized annual report data. |

### Creditsafe Sweden
| Field      | Detail |
|------------|--------|
| URL        | https://www.creditsafe.com/se |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | Org number, credit score, financials, directors, shareholders, group structure |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Good alternative to Bisnode; developer-friendly API. |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Sources from Bolagsverket; no advantage over direct Bolagsverket API for paid users |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Nasdaq Stockholm listed companies and financial institutions well covered |

## Corpscout Status
- [x] Raw input workflow in progress
- Source name: `se_hvd`
- Recommended source: Sweden HVD bulk downloads: Bolagsverket + SCB direct ZIP files
- Priority: High
