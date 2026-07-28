# Latvia procurement national source

Status: implemented as monthly Dagster assets. Latvia is covered by the shared
TED award pipeline and the national IUB result-notice source.

## Selected source: IUB open data

Prioritize the Procurement Monitoring Bureau (Iepirkumu uzraudzības birojs,
IUB) official open-data service:

- Catalogue: `https://www.iub.gov.lv/en/open-data`
- Files: `https://open.iub.gov.lv/data/notice/YYYY/MM/DD-MM-YYYY.json`
- Open-data record:
  `https://data.gov.lv/dati/eng/dataset/publisko-iepirkumu-pazinojumi`
- Licence: CC0 1.0

The service publishes machine-readable procurement notices without
authentication, organized as one JSON file per day and refreshed daily. The
eForms JSON history begins on 25 October 2023. It includes national procedures
as well as directive-level procedures, result and award notices, contract
modifications, and contract-execution notices.

The Electronic Procurement System (EIS) remains a complementary opportunity
and procurement-document portal. IUB notices link to EIS where applicable, so
EIS is not a separate first-choice award source.

## Snapshot and publication

The `latvia_iub_procurement` package uses monthly Dagster partitions
starting `2023-10`. For each partition:

1. Enumerate every calendar day and fetch
   `open.iub.gov.lv/data/notice/YYYY/MM/DD-MM-YYYY.json`.
2. Store each response and its checksum as an immutable raw S3 object, plus a
   monthly manifest and success marker. A missing day is recorded explicitly
   rather than silently treated as an empty response.
3. Parse the month with DuckDB using an explicit schema. Preserve the complete
   source object in the raw snapshot and keep source identifiers and business
   terms in normalized columns.
4. Convert tender-level EUR values to USD on publication date.
5. Publish through staging tables and atomic ClickHouse partition replacement.

The source warns that an eForms notice may be superseded and point to its prior
form through `clonedFrom`. Retain all versions in raw storage, resolve the chain,
and publish only the latest version in the normalized current-state tables.
Keep the source identifier, `clonedFrom`, retrieval date, and version lineage
for auditability.

## Normalized grains

The assets publish separate tables so notice awards and later contract execution are not
confused:

- `lv_iub_notices`: one row per canonical notice version, with notice type,
  legal basis, procedure identifiers, buyer, publication and decision dates,
  CPV, procedure URL, and source lineage.
- `lv_iub_notice_lots`: one row per (notice, lot), retaining estimated, awarded,
  and range values at the grain in which IUB publishes them.
- `lv_iub_notice_winners`: one row per (notice, lot, winner), retaining the
  winner name, raw identifier, normalized 11-digit registration code, tender
  identifier, and only values explicitly published at the winner-attributable
  grain.
- `lv_iub_contract_executions`: one row per (execution notice, contract, winner),
  retaining contract identifiers, signing and execution dates,
  modifications, final values, and links back to the procurement procedure.

Normalize whitespace-formatted 11-digit Latvian registration codes and the
equivalent `LV`-prefixed VAT form to `lv_companies.regcode`. Preserve every raw
identifier, and leave foreign or malformed identifiers unmatched.

Award, notice, lot, tender-range, modification, and execution amounts remain
separate named columns. An execution notice must not be counted as a second award.
Link it to the original award by the published procedure, contract,
notice, or lot identifiers when possible; never infer a link from buyer,
winner, date, and value alone.

## Company signal

The national-source signal branch reads canonical result and award winner rows.
It counts a company once per published award identity and sums only
winner-attributable values. A tender value shared by multiple consortium
parties remains visible on each evidence row with
`tender_value_attributable=0`, but does not enter company spend.

`lv_iub_notices_current` and the dependent current views remove superseded
notice identifiers referenced through `clonedFrom`. The company signal selects
national-law awards while TED supplies directive-level awards; it does not
claim identifier-based cross-source deduplication.
