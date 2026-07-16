# Norway — source inventory

| Source | Type | Access | Formats | License | Records | Status |
|---|---|---|---|---|---|---|
| **Enhetsregisteret (entities)** | Official registry API + bulk | Public, no auth | JSON, CSV, XLSX (gzip) | NLOD 2.0 | 1,164,396 active (1,458,299 incl. dissolved) | **recommended** |
| **Enhetsregisteret (underenheter)** | Official registry API + bulk | Public, no auth | JSON, CSV, XLSX (gzip) | NLOD 2.0 | 842,538 | **recommended** |
| **Regnskapsregisteret key figures** | Official registry API | Public, no auth | JSON, JSON-LD, XML, RDF, TTL | NLOD 2.0 | Latest approved standard-layout filing only | **recommended for latest only** |
| **Regnskapsregisteret report copies** | Official document API | Public, no auth | PDF | Document terms require review | Latest 15 years per org | **historical fallback** |
| **Annual-accounts subscription** | Official paid feed | Contract + annual fee | XML over SFTP, optional TIFF | Subscription agreement | ~300,000 new filings/year | **recommended for structured archive feed** |
| data.norge.no | Open data catalog (DCAT) | Public | DCAT/JSON | NLOD 2.0 | metadata only | useful_secondary_source |
| Beneficial Ownership Register | Official registry | Controlled | — | restricted | — | blocked_by_license_uncertainty |

## Key endpoints

### Base company data — Enhetsregisteret
```
GET https://data.brreg.no/enhetsregisteret/api/enheter            # search/list
GET https://data.brreg.no/enhetsregisteret/api/enheter/{orgnr}    # single entity
GET https://data.brreg.no/enhetsregisteret/api/enheter/{orgnr}/roller
GET https://data.brreg.no/enhetsregisteret/api/underenheter       # sub-entities
GET https://data.brreg.no/enhetsregisteret/api/enheter/lastned         # bulk JSON.gz (~197MB)
GET https://data.brreg.no/enhetsregisteret/api/enheter/lastned/csv     # bulk CSV.gz (~154MB)
GET https://data.brreg.no/enhetsregisteret/api/underenheter/lastned/csv  # bulk CSV.gz (~60MB)
GET https://data.brreg.no/enhetsregisteret/api/oppdateringer/enheter?dato=<ISO8601>  # deltas
```

### Financial data — Regnskapsregisteret
```
GET https://data.brreg.no/regnskapsregisteret/regnskap/{orgnr}   # annual accounts figures (JSON)
GET https://data.brreg.no/regnskapsregisteret/regnskap/aarsregnskap/kopi/{orgnr}/aar
GET https://data.brreg.no/regnskapsregisteret/regnskap/aarsregnskap/kopi/{orgnr}/{aar}
```

## Notes
- Both registers run by Brønnøysund Register Centre (Brreg); single attribution covers both.
- No API key, no registration. Set a descriptive `User-Agent` with a contact.
- The structured open financial endpoint is latest-only; its year/type filters do not work for
  unauthenticated callers. Use it for refresh and validation, not historical bootstrap.
- For structured history, first ask Brreg whether the paid XML subscription includes a one-time
  historical delivery. Otherwise the public 15-year PDF API requires OCR at very large scale.
