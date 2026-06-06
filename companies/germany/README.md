# Company data sources for Germany

## Status

- Official bulk data: **not found** (no official bulk dump from Handelsregister/Unternehmensregister)
- Official API: **not found** (no official open API; commercial third-party APIs exist)
- Open data portal: **found** (OffeneRegister.de — community/NGO open data; GovData.de national catalog)
- License: **known with caveat** (OffeneRegister states CC-BY 4.0; OpenSanctions mirror labels it CC-BY-NC 4.0 — confirm before commercial use)
- Recommended ingestion path: **bulk download** (OffeneRegister) for breadth + **commercial API or targeted lookups** for freshness

## Best source

**OffeneRegister.de** — `https://daten.offeneregister.de/de_companies_ocdata.jsonl.bz2`

The only free, openly-licensed **bulk** dataset covering essentially all German companies
(~5.1M companies + ~4M officers), published by the Open Knowledge Foundation Deutschland from
OpenCorporates data. It is JSONL (OpenCorporates company schema), 260 MB compressed / ~4 GB
uncompressed, **downloaded and verified** into `raw/bulk/`.

Key limitation: coverage is mostly **2017–2019** (snapshot is stale). A newer SQLite snapshot
(`handelsregister.db`, 2022) exists in the same directory for more recent data.

The **official** Handelsregister (handelsregister.de) is the authoritative source but offers
**no bulk download and no open API**, is fragmented across 150+ local courts, and its terms forbid
automated mass retrieval. For fresh, complete, structured data the realistic options are
**paid commercial APIs** (handelsregister.ai, OpenRegister, Viaductus, Kausate) or **targeted,
rate-limited lookups** via the official portal / `bundesAPI/handelsregister`.

## Next action

1. Decide freshness requirement:
   - **Historical / breadth baseline** → ingest the downloaded `de_companies_ocdata.jsonl.bz2` (done: bulk saved, normalized sample created).
   - **More recent (2022)** → download `handelsregister.db` (3.7 GB SQLite) from the same directory.
   - **Current + maintained** → evaluate a commercial API (budget required) or build targeted lookups respecting the 60/hour limit.
2. Confirm the license (CC-BY vs CC-BY-NC) with OffeneRegister/OpenCorporates before any commercial redistribution.
3. Map the OpenCorporates schema to the internal company model using `schema_notes.md`.
