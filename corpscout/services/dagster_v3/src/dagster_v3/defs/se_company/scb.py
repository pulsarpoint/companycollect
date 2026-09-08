"""The SCB address artifact of the Swedish company register.

Input (source layer): corpscout.se_company_addresses_current, the register's address
snapshot. Writes corpscout.se_company_address_scb: the address SCB holds for each company
(visiting or postal -- the register does not distinguish), one source value apart from the
Bolagsverket artifact; a new version is written only when evidence_hash changes.

The info artifact (se_company_info_scb) that used to share this module was retired with
the old publisher in basic-info slice 4 (2026-09-08).
Downstream: address_legacy.py (field precedence bolagsverket > scb).
"""

from datetime import UTC, datetime

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.se_company.common import SE_COMPANY_ID_PATTERN, publish_with_stage

GROUP_NAME = "se_company_scb"
DATABASE = "corpscout"
ADDRESS_TABLE = "se_company_address_scb"
ADDRESS_SOURCE = "scb"
# The one address_type normalized_duckdb.py's scb_addresses CTE ever emits (a
# hard-coded literal, address_rank = 1 -- one row per company). Pinned in the WHERE
# below and re-checked by the tripwire so a change to that upstream assumption fails
# loudly here instead of silently losing rows to ReplacingMergeTree collapsing two
# same-keyed versions (see the SQL comment and the tripwire below).
ADDRESS_TYPE = "visiting_or_postal"
ADDRESS_SOURCE_TABLE = "se_company_addresses_current"
SE_COMPANY_ADDRESS_SCB_COLUMNS = (
    "company_id", "source_record_uid", "observed_at", "source_run_id",
    "address_type", "address_fingerprint", "care_of", "street_address",
    "normalized_address", "postal_code", "city", "country_code",
)

# Same shape as bolagsverket.py's SELECT, one source value apart -- deliberately NOT
# factored into a shared builder: each artifact module owns its own table name, its own
# insert list and its own SELECT, and a shared generator would make a payload change in
# one source silently rewrite the other's evidence.
#
# SCB records a single address per company and does not say whether it is the visiting or
# the postal one, hence the register's own 'visiting_or_postal' type, which travels
# unchanged: the address_type is part of address_key, so renaming it here would silently
# re-key every SCB address.
#
# SCB also marks foreign addresses with the placeholders PostOrt='Utlandet' and
# PostNr='00000'. Those are the normalizer's business (migration 000265 already drops both
# from normalized_address), so they arrive here as ordinary text and are neither
# special-cased nor repaired.
#
# address_type = '{ADDRESS_TYPE}' pins the one type this source has ever emitted (see
# ADDRESS_TYPE above). The artifact's ORDER BY (company_id, source_record_uid) is only a
# unique key because of the one-row-per-company invariant this filter enforces; without
# it, a second address_type slipping into the source table would not error here -- it
# would just let ReplacingMergeTree collapse the extra row at stage-write time (same
# ORDER BY key, two versions), so the staged count would silently come out lower than
# the source actually holds. The tripwire below is the independent check for exactly
# that.
SE_COMPANY_ADDRESS_SCB_SQL = """WITH candidates AS (
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
    WHERE addresses.source = '{ADDRESS_SOURCE}'
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
).replace("{ADDRESS_SOURCE}", ADDRESS_SOURCE).replace("{ADDRESS_TYPE}", ADDRESS_TYPE)

# Tripwire (I2): recomputes the candidates CTE's own filters directly against the source
# table, independently of anything publish_with_stage's stage table may already have
# collapsed. Compared against PublishCounts.staged (the row count observed on the stage
# right after the SELECT above ran) once the asset's publish call returns; a mismatch
# means the source held a different number of matching rows than made it into the stage
# -- exactly the silent-collapse scenario the reviewer reproduced -- and is worth
# failing the run over rather than logging and moving on.
#
# Deliberately NOT filtered on address_type: that pin is the invariant this tripwire
# measures, so repeating it here would make the count agree with the pinned SELECT by
# construction -- blind to a second address_type appearing under the same
# source_record_uid (ReplacingMergeTree collapses it either way, both counts would read
# the same lower number) and blind to the literal itself being renamed upstream (both
# counts would read zero). The tripwire counts every row this source actually holds;
# the pinned SELECT counts only the one type it expects. Under today's guaranteed
# one-type-per-source invariant the two agree, so no false positives -- a second type or
# a renamed type makes them disagree.
SE_COMPANY_ADDRESS_SCB_SOURCE_COUNT_SQL = """SELECT count()
FROM corpscout.se_company_addresses_current AS addresses
WHERE addresses.source = %(source)s
  AND addresses.has_address = 1
  AND match(addresses.company_id, '{SE_COMPANY_ID_PATTERN}')
  AND addresses.source_record_uid != ''""".replace(
    "{SE_COMPANY_ID_PATTERN}", SE_COMPANY_ID_PATTERN
)


@dg.asset(
    name="se_company_address_scb_clickhouse",
    deps=[dg.AssetKey("sweden_company_addresses_clickhouse")],
    group_name=GROUP_NAME,
    kinds={"clickhouse", "python"},
    metadata={"table": f"{DATABASE}.{ADDRESS_TABLE}"},
    description=(
        "The address SCB holds for each Swedish company (visiting or postal -- the register "
        "does not distinguish), as an append-only artifact; a new version is written only "
        "when the evidence hash changes."
    ),
)
def se_company_address_scb_clickhouse(
    context: dg.AssetExecutionContext, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    """Select the SCB rows of the address snapshot -> stage -> validate -> append."""
    assert_clickhouse_tables_exist(
        clickhouse, database=DATABASE, tables=(ADDRESS_SOURCE_TABLE, ADDRESS_TABLE)
    )
    counts = publish_with_stage(
        clickhouse=clickhouse,
        target=ADDRESS_TABLE,
        insert_columns=SE_COMPANY_ADDRESS_SCB_COLUMNS,
        select_sql=SE_COMPANY_ADDRESS_SCB_SQL,
        select_parameters={"source_run_id": context.run_id},
        invalid_condition=(
            "trim(company_id) = '' OR trim(source_record_uid) = '' OR trim(address_type) = ''"
        ),
        new_versions_only=True,
    )
    with clickhouse.get_connection() as client:
        source_count = int(
            client.execute(
                SE_COMPANY_ADDRESS_SCB_SOURCE_COUNT_SQL,
                {"source": ADDRESS_SOURCE},
            )[0][0]
        )
    context.log.info(
        "se_company_address_scb: staged=%s source_count=%s", counts.staged, source_count
    )
    if source_count != counts.staged:
        raise ValueError(
            f"se_company_address_scb: staged count {counts.staged} does not match "
            f"source count {source_count} for source={ADDRESS_SOURCE!r} "
            f"address_type={ADDRESS_TYPE!r} -- the source pipeline may be emitting more "
            "than one address row per company for this source, which "
            "ReplacingMergeTree's ORDER BY (company_id, source_record_uid) would "
            "silently collapse -- or the pinned address_type no longer matches "
            "what the source emits."
        )
    context.log.info("se_company_address_scb: appended=%s total=%s", counts.inserted, counts.total)
    return dg.MaterializeResult(
        metadata={"appended_count": counts.inserted, "total_count": counts.total,
                  "table": f"{DATABASE}.{ADDRESS_TABLE}", "resolved_at": datetime.now(UTC).isoformat()}
    )


defs = dg.Definitions(assets=[se_company_address_scb_clickhouse])
