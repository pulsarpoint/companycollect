# ESMA FIRDS design

## Source overview

ESMA's Financial Instruments Reference Data System (FIRDS) is an EU-wide
securities reference source, not a Sweden-only company register. The first
consumer is the company signal work: an issuer LEI linked to a currently
admitted instrument provides authoritative evidence that a legal entity has a
publicly traded security.

The source is the public ESMA FIRDS file register:

- discovery: `esma_registers_firds_files` Solr endpoint;
- full instrument reference data: `FULINS`;
- daily instrument changes: `DLTINS`;
- consolidated cancellation evidence: `FULCAN`;
- archives: ZIP files containing ISO 20022 XML.

No credentials are required. Each registry row supplies the publication date,
download URL, file name, stable file id, and MD5 checksum.

## Ingest mode

FIRDS is a multipart bulk source. It is not loaded with dlt because the useful
unit is an immutable, checksummed ZIP archive and the XML files are large enough
to require streaming parse. Dagster handles discovery and archive provenance;
`lxml.iterparse` handles bounded-memory record extraction; DuckDB handles
set-based state reconstruction.

File-set completeness is mandatory:

- a `FULINS` publication must contain every observed CFI category
  (`C,D,E,F,H,I,J,O,R,S`) and every numbered part per category;
- a `DLTINS` or `FULCAN` publication must contain every numbered part;
- an incomplete newest publication fails the rebuild rather than silently
  presenting partial securities coverage.

Archives and metadata are stored under content-addressed object keys containing
the ESMA checksum. Downloads stream to disk, validate `Content-Length` when
present, verify ESMA's MD5, calculate SHA-256 for internal provenance, and retry
the entire transfer after transient failures.

## Parsing and identity

The parser is namespace-agnostic across compatible ISO 20022 schema versions
and streams one business record at a time:

- `FULINS`: `RefData` -> `BASELINE`;
- `DLTINS`: `NewRcrd`, `ModfdRcrd`, `TermntdRcrd`, `CancRcrd`;
- `FULCAN`: `CxlData` -> `CONSOLIDATED_CANCELLED`.

The instrument/venue key is `(ISIN, MIC)`. The core company bridge is
`issuer_lei`; FIRDS does not provide national company registration numbers.
Instrument name, CFI, notional currency, admission/trading dates, termination
date, relevant venue, and competent-authority country are preserved.
The current-state table also retains its latest lifecycle event and the full
source-file id/checksum/object/download provenance needed to trace the row.

The immutable ZIP remains the canonical raw payload in object storage.
DuckDB keeps the exact source file/row coordinates and per-record SHA-256, but
does not duplicate every XML element; this keeps full EU ingestion bounded and
fully reprocessable. Raw payloads and hashes remain excluded from ClickHouse.

## Current-state reconstruction

Current state follows ESMA's baseline-plus-delta model:

1. start from the newest complete `FULINS` publication;
2. apply complete `DLTINS` publications after that baseline in source order;
3. retain the latest event for `(ISIN, MIC)`;
4. exclude cancelled, terminated, and already-expired instruments.

The event-history table starts at the earliest retained complete baseline and
keeps subsequent deltas with `valid_from`, `valid_to`, and `is_latest`.

`FULCAN` is parsed and catalogued as reconciliation evidence only. It is not
blindly subtracted from current state because the consolidated rows contain the
instrument/venue key but no effective cancellation timestamp and may describe
historical cancellations already superseded by later records.

## Storage and publication

One source-specific DuckDB file, `data/esma_firds_source.duckdb`, owns raw
records, file/set catalogs, derived event history, and current state. The
event-history and current-state builds are separate Dagster assets. Every
asset opening the file uses the `esma_firds_duckdb` pool.

Migration `000164_corpscout_esma_firds` owns the ClickHouse DDL:

- `corpscout.firds_instrument_events`;
- `corpscout.firds_instruments_current`.

Dagster validates that both migrated tables exist, loads both staging tables,
and exchanges both only after all inserts succeed. Empty replacement is
refused. Before publication, the EU-wide current-state asset also requires at
least 1,000,000 rows, 25 competent-authority countries, and 100 MICs; it emits
per-country and top-MIC row counts to make country or venue collapse visible.

The source is never filtered by country. `listing_scopes.py` contains the
configuration-backed downstream projection rule. Sweden initially selects
equity CFIs on `XSTO`, `FNSE`, `XNGM`, `NSME`, and `XSAT`.

## Scheduling

- `esma_firds_delta_daily`: 10:40 Europe/Paris, default `STOPPED`;
- `esma_firds_full_weekly`: Sunday 12:50 Europe/Paris, default `STOPPED`.

The weekly run refreshes full, delta, and consolidated-cancellation inputs.
The daily run refreshes deltas and rebuilds state from previously materialized
full/cancellation inputs. Both remain stopped until a first controlled live
materialization validates source volume, runtime, and operational storage.

ESMA documentation describes full reference files as weekly and delta files as
daily. The live register currently exposes consolidated `FULCAN` publications
on a weekly pattern, so the implementation treats that observed cadence as
reconciliation input without assuming a daily file must exist.

## Out of scope

This slice does not yet:

- map `issuer_lei` through GLEIF to country registry identifiers;
- map EODHD symbols to ISINs;
- expose a company-level `is_publicly_traded` signal or filters;
- infer exchange-listing status from names or venue text;
- process non-EU exchange sources.

Those are downstream joins built on the FIRDS current-state table, not concerns
of source ingestion.
