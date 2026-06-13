# Norway — source inventory

| Source | Type | Access | Formats | License | Records | Status |
|---|---|---|---|---|---|---|
| **Enhetsregisteret (entities)** | Official registry API + bulk | Public, no auth | JSON, CSV, XLSX (gzip) | NLOD 2.0 | 1,164,396 active (1,458,299 incl. dissolved) | **recommended** |
| **Enhetsregisteret (underenheter)** | Official registry API + bulk | Public, no auth | JSON, CSV, XLSX (gzip) | NLOD 2.0 | 842,538 | **recommended** |
| **Regnskapsregisteret (financials)** | Official registry API | Public, no auth | JSON, JSON-LD, XML, RDF, TTL | NLOD 2.0 | ~80% of accounting-liable cos. | **recommended** |
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
```

## Notes
- Both registers run by Brønnøysund Register Centre (Brreg); single attribution covers both.
- No API key, no registration. Set a descriptive `User-Agent` with a contact.
- For full load use bulk gzip; for freshness use the `oppdateringer` delta feed; for financials
  call per orgnr and cache keyed on `sisteInnsendteAarsregnskap`.
