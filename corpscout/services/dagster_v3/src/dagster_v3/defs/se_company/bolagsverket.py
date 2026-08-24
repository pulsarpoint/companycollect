"""Swedish company addresses as Bolagsverket registered them.

Input (source layer): sweden_company_addresses_clickhouse ->
corpscout.se_company_addresses_current, the rename-swap snapshot of the append-only
se_company_addresses history: one row per (company, address_type, source), with the
register's own provenance (source_run_id, source_record_id, source_payload_hash and the
derived source_record_uid that joins company_source_records).

This module keeps the Bolagsverket half of that snapshot -- address_type 'postal', the
registered postal address, the register being the registration authority for it -- and
writes the standard envelope followed by the address payload. One artifact row per
company: the normalizer picks exactly one address row per company per source.

address_fingerprint travels with the payload because it is the key the rest of the
address pipeline uses: se_company_address_members_current.address_key IS this
fingerprint, so it is the only way from a merged final address back to a canonical
address, a shared address_id and its geocode. Without it the final could not augment
anything.

Assets
  se_company_address_bolagsverket_clickhouse -> corpscout.se_company_address_bolagsverket
Downstream: address.py (field precedence bolagsverket > scb).
"""

from datetime import UTC, datetime

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.se_company.common import SE_COMPANY_ID_PATTERN, publish_with_stage

GROUP_NAME = "se_company_bolagsverket"
DATABASE = "corpscout"
TABLE = "se_company_address_bolagsverket"
SOURCE = "bolagsverket"
# The one address_type normalized_duckdb.py's bolagsverket_addresses CTE ever emits
# (a hard-coded literal, address_rank = 1 -- one row per company). Pinned in the
# WHERE below and re-checked by the tripwire so a change to that upstream assumption
# fails loudly here instead of silently losing rows to ReplacingMergeTree collapsing
# two same-keyed versions (see the SQL comment and the tripwire below).
ADDRESS_TYPE = "postal"
SOURCE_TABLE = "se_company_addresses_current"
# Positional insert list: the envelope (evidence_hash is MATERIALIZED, so omitted) then
# this module's payload, in the order the migration declares them -- pinned by the test.
SE_COMPANY_ADDRESS_BOLAGSVERKET_COLUMNS = (
    "company_id", "source_record_uid", "observed_at", "source_run_id",
    "address_type", "address_fingerprint", "care_of", "street_address",
    "normalized_address", "postal_code", "city", "country_code",
)

# New versions only: publish_with_stage stages these candidates and keeps only rows whose
# (company_id, source_record_uid, evidence_hash) is not already in the target -- the
# target's MATERIALIZED evidence_hash computes the hash on the stage, so it is never
# re-expressed here.
#
# observed_at is now64 at append time. The register's own updated_from_raw_at is a single
# constant for the whole weekly bulk load and is older than every resolved_at the final
# writes, so a version stamped with it would never look newer than the row it replaces and
# the change scan would never select the company again. Rows the anti-join skips are never
# rewritten, so an unchanged company keeps its original stamp instead of looking new every
# week.
#
# has_address = 0 rows are dropped: the snapshot carries one row per (company, type,
# source) whether or not the register recorded anything, and a row with no address at all
# is not an address. They are also exactly the rows whose MATERIALIZED normalized_address
# is '' by construction (migration 000265).
#
# address_type = '{ADDRESS_TYPE}' pins the one type this source has ever emitted (see
# ADDRESS_TYPE above). The artifact's ORDER BY (company_id, source_record_uid) is only a
# unique key because of that one-row-per-company invariant; without this filter, a second
# address_type slipping into the source table would not error here -- it would just let
# ReplacingMergeTree collapse the extra row at stage-write time (same ORDER BY key, two
# versions), so the staged count would silently come out lower than the source actually
# holds. The tripwire below is the independent check for exactly that.
#
# post_town is the register's name for the column; the datatype publishes it as `city`,
# which is what every other country's address shape calls it. country_code is
# LowCardinality(Nullable(String)) in the source and Nullable(String) in the artifact, so
# it is CAST explicitly rather than left to an implicit conversion.
SE_COMPANY_ADDRESS_BOLAGSVERKET_SQL = """WITH candidates AS (
    SELECT
        addresses.company_id AS company_id,
        addresses.source_record_uid AS source_record_uid,
        now64(3, 'UTC') AS observed_at,
        %(source_run_id)s AS source_run_id,
        toString(addresses.address_type) AS address_type,
        toString(addresses.address_fingerprint) AS address_fingerprint,
        addresses.care_of AS care_of,
        addresses.street_address AS street_address,
        nullIf(addresses.normalized_address, '') AS normalized_address,
        addresses.postal_code AS postal_code,
        addresses.post_town AS city,
        CAST(addresses.country_code AS Nullable(String)) AS country_code
    FROM corpscout.se_company_addresses_current AS addresses
    WHERE addresses.source = '{SOURCE}'
      AND addresses.address_type = '{ADDRESS_TYPE}'
      AND addresses.has_address = 1
      AND match(addresses.company_id, '{SE_COMPANY_ID_PATTERN}')
)
SELECT
    company_id AS company_id, source_record_uid AS source_record_uid,
    observed_at AS observed_at, source_run_id AS source_run_id,
    address_type AS address_type, address_fingerprint AS address_fingerprint,
    care_of AS care_of, street_address AS street_address,
    normalized_address AS normalized_address, postal_code AS postal_code,
    city AS city, country_code AS country_code
FROM candidates
WHERE source_record_uid != ''""".replace(
    "{SE_COMPANY_ID_PATTERN}", SE_COMPANY_ID_PATTERN
).replace("{SOURCE}", SOURCE).replace("{ADDRESS_TYPE}", ADDRESS_TYPE)

# Tripwire (I2): recomputes the candidates CTE's own filters directly against the source
# table, independently of anything publish_with_stage's stage table may already have
# collapsed. Compared against PublishCounts.staged (the row count observed on the stage
# right after the SELECT above ran) once the asset's publish call returns; a mismatch
# means the source held more matching rows than made it into the stage -- exactly the
# silent-collapse scenario the reviewer reproduced -- and is worth failing the run over
# rather than logging and moving on.
SE_COMPANY_ADDRESS_BOLAGSVERKET_SOURCE_COUNT_SQL = """SELECT count()
FROM corpscout.se_company_addresses_current AS addresses
WHERE addresses.source = %(source)s
  AND addresses.address_type = %(address_type)s
  AND addresses.has_address = 1
  AND match(addresses.company_id, '{SE_COMPANY_ID_PATTERN}')
  AND addresses.source_record_uid != ''""".replace(
    "{SE_COMPANY_ID_PATTERN}", SE_COMPANY_ID_PATTERN
)


@dg.asset(
    name="se_company_address_bolagsverket_clickhouse",
    deps=[dg.AssetKey("sweden_company_addresses_clickhouse")],
    group_name=GROUP_NAME,
    kinds={"clickhouse", "python"},
    metadata={"table": f"{DATABASE}.{TABLE}"},
    description=(
        "The postal address Bolagsverket has registered for each Swedish company, as an "
        "append-only artifact; a new version is written only when the evidence hash "
        "changes and the latest per (company, source record) survives merges."
    ),
)
def se_company_address_bolagsverket_clickhouse(
    context: dg.AssetExecutionContext, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    """Select the Bolagsverket rows of the address snapshot -> stage -> validate -> append."""
    assert_clickhouse_tables_exist(
        clickhouse, database=DATABASE, tables=(SOURCE_TABLE, TABLE)
    )
    counts = publish_with_stage(
        clickhouse=clickhouse,
        target=TABLE,
        insert_columns=SE_COMPANY_ADDRESS_BOLAGSVERKET_COLUMNS,
        select_sql=SE_COMPANY_ADDRESS_BOLAGSVERKET_SQL,
        select_parameters={"source_run_id": context.run_id},
        invalid_condition=(
            "trim(company_id) = '' OR trim(source_record_uid) = '' OR trim(address_type) = ''"
        ),
        new_versions_only=True,
    )
    with clickhouse.get_connection() as client:
        source_count = int(
            client.execute(
                SE_COMPANY_ADDRESS_BOLAGSVERKET_SOURCE_COUNT_SQL,
                {"source": SOURCE, "address_type": ADDRESS_TYPE},
            )[0][0]
        )
    context.log.info(
        "se_company_address_bolagsverket: staged=%s source_count=%s",
        counts.staged, source_count,
    )
    if source_count != counts.staged:
        raise ValueError(
            f"se_company_address_bolagsverket: staged count {counts.staged} does not "
            f"match source count {source_count} for source={SOURCE!r} "
            f"address_type={ADDRESS_TYPE!r} -- the source pipeline may be emitting more "
            "than one address row per company for this source, which "
            "ReplacingMergeTree's ORDER BY (company_id, source_record_uid) would "
            "silently collapse."
        )
    context.log.info(
        "se_company_address_bolagsverket: appended=%s total=%s", counts.inserted, counts.total
    )
    return dg.MaterializeResult(
        metadata={"appended_count": counts.inserted, "total_count": counts.total,
                  "table": f"{DATABASE}.{TABLE}", "resolved_at": datetime.now(UTC).isoformat()}
    )


defs = dg.Definitions(assets=[se_company_address_bolagsverket_clickhouse])
