# Company data sources for Switzerland (CH)

## Status

### Company registry data
- Official bulk data: **found (open)** — Zefix via the **LINDAS Linked Data SPARQL** endpoint (no auth).
- Official API: **found but gated** — Zefix Public REST API requires free **HTTP Basic** credentials.
- Open data portal: **found** — opendata.swiss (publishes Zefix as LINDAS linked data, not a flat file).
- License: **known** — Zefix is OGD / "Open use" (attribution required).
- Recommended ingestion path: **Zefix LINDAS SPARQL** (open, paged) for identity; Zefix REST (free creds) for SOGC/status detail.

### Financial data (annual accounts)
- **Largely NOT available.** Switzerland imposes **no public filing obligation** for private companies
  (AG/GmbH): under Art. 958 CO accounts are prepared but **not disclosed publicly**. Financials are
  public **only** for **listed companies** (via **SIX Swiss Exchange**) and **regulated entities**
  (banks/insurers via FINMA). For the ~99% private universe, financials are **not obtainable** openly.

## Best source

**Zefix — Zentraler Firmenindex** (Federal Commercial Registry Office / EHRA), exposed as **LINDAS
Linked Data**. A single open SPARQL endpoint (`https://lindas.admin.ch/query`, no auth) returns the
full register of **788,989** legal entities: legal name, **UID** (CHE…), CHID, EHRA-id, **legal form**
(eCH-0097), registered address, municipality, website, and business purpose. VAT number = UID + `MWST`/
`TVA`/`IVA`. The Zefix **REST API** adds SOGC (gazette) publications and status detail but needs free
Basic-auth credentials.

## Next action

Page the Zefix LINDAS SPARQL endpoint (SELECT with OFFSET/LIMIT) to build the open identity master,
keyed on **UID**. Optionally request free Zefix REST credentials for SOGC event streams / status. For
financials, only listed companies (SIX) and regulated entities are obtainable — treat private-company
financials as unavailable.
