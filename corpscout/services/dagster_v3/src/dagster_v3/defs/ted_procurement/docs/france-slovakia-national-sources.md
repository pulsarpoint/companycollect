# France and Slovakia national procurement sources

Status: **implemented as Dagster assets**. TED awards remain enabled for France
and Slovakia. DECP is ready to materialize; UVO is implemented but its network
boundary remains disabled until the machine-reuse licence prerequisite is met.
This document records the source boundaries, grains, value semantics, and
operational go/no-go conditions.

The company signal remains award-focused. Open competition notices may be kept
in a source's own tables, but they do not enter government-contract summaries
until a winner exists.

## France: DECP

### Selected source

- Register: Données essentielles de la commande publique (DECP), published by
  the French Ministry of Economy.
- Catalogue:
  `https://data.economie.gouv.fr/explore/dataset/decp-2022-marches-valides/`
- Bulk artifact:
  `https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/decp-2022-marches-valides/exports/csv`
- API catalogue contract:
  `https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/decp-2022-marches-valides`
- Licence: Licence Ouverte 2.0 / Etalab, subject to confirmation from the
  catalogue response at implementation time.

DECP is selected because it is an official aggregated award/contract feed and
its domestic holders overwhelmingly carry SIRET identifiers. It is a better
company-to-award boundary than a presentation-site crawl.

BOAMP is the complementary source for competition notices and award results,
including MAPA notices:
`https://www.data.gouv.fr/dataservices/api-bulletin-officiel-des-annonces-des-marches-publics-boamp`.
It is not selected for the first national award pipeline because the approved
scope is awarded contracts and DECP provides a single bulk snapshot with
structured holder identifiers.

### Ingest and asset shape

Use a separate `france_decp_procurement` package and DuckDB file.

1. `france_decp_procurement_raw_snapshot_s3` downloads the full CSV through
   the dlt request boundary and stores a content-addressed immutable source
   file and manifest in `s3://source-france-decp-procurement/`.
2. `france_decp_contract_holders_duckdb` loads with DuckDB `read_csv(...,
   all_varchar=true)` and expands the three holder slots. This is a
   non-partitioned full refresh because an official bulk artifact exists.
3. `france_decp_contract_holders_usd` converts the contract-level amount on
   notification date, falling back to publication date.
4. `france_decp_contract_holders_clickhouse` atomically replaces
   `corpscout.fr_decp_contract_holders` and resolves exact matches to
   `fr_companies.siren`.
5. `fr_government_contracts` unions DECP with TED. DECP has no reliable TED
   reference, so directive-level duplicates may remain.

The ClickHouse grain is **one row per (contract, holder)**. Expand the three
holder slots into separate rows with a stable `holder_ordinal`; do not flatten
several holders into a list that cannot join to companies.

Required audit and identity fields:

- source run, source record id, payload hash, source URL, retrieval timestamp
- contract id, nature, object/title, procedure, CPV, buyer SIRET
- notification/publication dates, duration, execution place
- holder ordinal, holder name, identifier type, identifier raw value
- normalized SIRET and derived nine-digit SIREN
- framework, offer-count, social, environmental, modification, and
  subcontracting fields published by the source

SIRET values are whitespace-normalized, must contain 14 digits, and map to
their first nine-digit SIREN. Nine-digit SIREN values are preserved and valid
French VAT shapes yield their embedded SIREN. Keep all raw holder identifiers
and types. Non-EU, RIDET, IREP, TAHITI, malformed, and missing identifiers
remain auditable but unmatched.

### Monetary contract

DECP describes `montant` as a fixed amount or estimated maximum excluding tax.
It is therefore a contract-level published amount and **not winner-attributable spend**.
Store it as
`notice_value_amount_original`/`notice_value_amount_usd` with EUR currency;
leave winner-attributable `value_amount_*` null. With multiple holders, the
contract-level amount repeats for display but is never summed per holder.

Modification and subcontracting amounts remain separate named metric triples
(`*_amount_original`, `*_amount_usd`, `*_currency`). No estimate, ceiling,
modification, or subcontract amount is coalesced into another value.

### Tests and acceptance

- Recorded bulk header fixture matches the explicit staging schema.
- A three-holder record emits three rows with stable ordinals.
- SIRET maps to SIREN; other identifier types do not.
- Empty downloads refuse to replace DuckDB or ClickHouse tables.
- All monetary source fields retain separate native and USD columns.
- A second full refresh produces the same keys and no duplicates.

## Slovakia: UVO bulletin

### Selected source and prerequisite

- Register: Vestník verejného obstarávania, operated by Úrad pre verejné
  obstarávanie (UVO).
- Portal: `https://www.uvo.gov.sk/vestnik-a-registre/vestnik`
- Month enumeration:
  `https://www.uvo.gov.sk/vestnik-a-registre/vestnik/ajaxCalendar?type=199900&start=<yyyy-mm-dd>&end=<yyyy-mm-dd>`
- Issue pages:
  `https://www.uvo.gov.sk/vestnik-a-registre/vestnik?order=<issue>&year=<year>`
- Notice detail:
  `https://www.uvo.gov.sk/vestnik-a-registre/vestnik/oznamenie/detail/<id>`

Result forms publish organizations, IČO, lots, CPV, estimated/tender values, and
winner linkage in structured eForms-like sections. UVO is procurement-specific
and includes national notices that do not reach TED.

Production operation has a **licence prerequisite**: confirm the current
machine-reuse licence in the Slovak national open-data catalogue, or locate the
official XML/open-data artifact previously advertised by UVO. Public HTML
availability alone is not sufficient authority for a scheduled bulk ingest.
Record the verified licence or permission before setting
`SLOVAKIA_UVO_MACHINE_REUSE_CONFIRMED=true`. The downloader raises before any
request while that gate is false.

### Ingest and asset shape

Use a separate `slovakia_uvo_procurement` package with a monthly partition
starting at the earliest archive month supported by the verified source
contract.

1. `slovakia_uvo_procurement_html_s3` enumerates bulletin issue pages by date,
   selects above-threshold and national result groups, and stores issue and
   detail HTML under deterministic S3 prefixes.
2. `slovakia_uvo_procurement_notices_duckdb` parses recorded HTML into an
   explicit winner-candidate table.
3. `slovakia_uvo_procurement_notices_usd` converts the winner-attributable
   BT-720 value on publication date.
4. `slovakia_uvo_procurement_notices_clickhouse` partition-replaces
   `corpscout.sk_uvo_procurement_notices` at **one row per (notice, lot, winner)**
   and resolves exact `sk_companies.ico` matches.
5. `sk_government_contracts` reads only national-law UVO results alongside TED
   to avoid known directive-level overlap.

Use the dlt request client for retries and `Retry-After`; throttle detail-page
downloads. Raw HTML is durable audit input because the available machine
surface is a presentation document rather than a versioned JSON schema.

Required fields include issue/notice ids, form type and version, buyer, buyer
IČO, procedure, title, description, CPV, execution place, publication and award
dates, lot id/title, winner ordinal/name/country/IČO, tender counts, estimated
value, lowest/highest tender, awarded tender value, source URL, raw object key,
payload hash, and retrieval timestamp.

Normalize only whitespace-formatted eight-digit IČO. Preserve `SK` VAT numbers
and every other identifier raw; they do not join to `sk_companies.ico`.
Estimated values, tender ranges, ceilings, and awarded values remain separate
metric triples at their published grains.

### Tests and acceptance

- Recorded calendar, issue, and result-detail fixtures cover pagination,
  multiple lots, multiple winners, a domestic IČO, and a VAT-only winner.
- Parser output has stable notice/lot/winner keys and resolves every winner
  reference or reports it in materialization metadata.
- Re-running a month reuses unchanged raw documents and produces no duplicates.
- Empty or structurally changed issue pages fail before replacing published
  data.
- A sampled overlap with TED reports match counts without silently
  deduplicating unmatched records.

## Slovakia: CRZ research-only alternative

The Central Register of Contracts publishes daily differential ZIP archives at
`http://www.crz.gov.sk/export/rrrr-mm-dd.zip`; each archive contains XML
metadata and attachment names. CRZ carries buyer and supplier IČO, dates, and
signed contract amounts, but it also contains grants, leases, employment, and
other public-sector agreements that are not procurement.

CRZ is therefore a **research-only** complementary source for this delivery.
Do not add it to the procurement signal until a future design proves a
source-published procurement classification (for example, a reliable UVO
reference) and measures false positives. Name/title heuristics are not an
acceptable filter.
