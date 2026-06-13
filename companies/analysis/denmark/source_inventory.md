# Denmark — source inventory

| Source | Type | Access | Auth | Formats | Records | License | Status |
|--------|------|--------|------|---------|---------|---------|--------|
| **CVR-permanent** (Det Centrale Virksomhedsregister) | Official base register (Elasticsearch) | `http://distribution.virk.dk/cvr-permanent` | HTTP Basic (free, email request) | JSON | 2,194,982 companies / 2,787,126 prod. units / 1,772,344 participants | Free reuse, CVR-loven | **recommended** |
| **Offentliggørelser / Regnskaber** (financial statements) | Official financial filings (Elasticsearch) | `http://distribution.virk.dk/offentliggoerelser` | **None** | JSON + XBRL/iXBRL/ESEF/PDF/TIFF | 6,295,759 filings | Free / open | **recommended** |
| **Registreringstekster** (registration texts) | Official change history | `http://distribution.virk.dk/registreringstekster` | HTTP Basic (free) | JSON | — | Free reuse | useful_secondary_source |
| **Virk Data / datahub.virk.dk** | Open-data catalog | `http://datahub.virk.dk` | None | metadata | — | — | useful_secondary_source |
| **cvr.dev / cvrapi.dk / apicvr.dk** | Third-party REST wrappers | `https://cvr.dev` etc. | varies | JSON | mirror of CVR | provider terms | useful_secondary_source |

## Recommendation

- **Base company data:** CVR-permanent (request free credentials at `cvrselvbetjening@erst.dk`),
  extract via scroll API. Registry key `denmark/cvr`.
- **Financial data:** Offentliggørelser (open, no auth) for filing discovery + XBRL download/parse.
  Registry key `denmark/cvrregnskab`.

Both verified live on 2026-06-13.
