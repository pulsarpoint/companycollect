import uuid
from datetime import UTC, date, datetime

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.company_listings import tables
from dagster_v3.defs.esma_firds.listing_scopes import country_listing_scope

COUNTRY_CODE = "SE"
IDENTITY_MATCH_METHOD = "firds_issuer_lei_gleif_registered_as"

SE_LISTING_UPSTREAM_ASSET_KEYS = (
    "esma_firds_clickhouse",
    "gleif_reference_clickhouse",
    "eodhd_reference_complete",
    "sweden_company_companies_clickhouse",
)

_REQUIRED_CLICKHOUSE_TABLES = (
    tables.SE_COMPANY_LISTINGS_TABLE,
    "firds_instruments_current",
    "gleif_lei_records",
    "se_companies",
    "eodhd_symbols",
    "eodhd_symbol_mics",
)

_QUALITY_COLUMNS = (
    "candidate_rows",
    "candidates_with_issuer_lei",
    "row_count",
    "company_count",
    "isin_count",
    "mic_count",
    "listing_key_count",
    "invalid_identity_rows",
    "latest_source_publication_date",
)


def _qualified(table_name: str) -> str:
    return f"`{tables.CLICKHOUSE_DATABASE}`.`{table_name}`"


def _sweden_mic_values_sql() -> str:
    scope = country_listing_scope(COUNTRY_CODE)
    return ", ".join(f"'{mic}'" for mic in sorted(scope.mic_codes))


def _firds_scope_predicate(alias: str) -> str:
    scope = country_listing_scope(COUNTRY_CODE)
    cfi_predicate = " OR ".join(
        (f"startsWith(upperUTF8(trimBoth({alias}.cfi_code)), '{category}')")
        for category in sorted(scope.cfi_categories)
    )
    return (
        f"upperUTF8(trimBoth({alias}.mic)) IN ({_sweden_mic_values_sql()}) "
        f"AND ({cfi_predicate})"
    )


def build_se_company_listings_insert_sql(stage_table: str) -> str:
    """Build the exact FIRDS LEI to Swedish company reconciliation."""
    columns = ", ".join(tables.SE_COMPANY_LISTINGS_COLUMNS)
    firds_scope = _firds_scope_predicate("f")
    sweden_mics = _sweden_mic_values_sql()
    return f"""INSERT INTO {stage_table} ({columns})
WITH
firds_scope AS
(
    SELECT
        upperUTF8(trimBoth(f.isin)) AS isin,
        upperUTF8(trimBoth(f.mic)) AS mic,
        upperUTF8(trimBoth(f.issuer_lei)) AS issuer_lei,
        f.full_name,
        f.short_name,
        upperUTF8(trimBoth(f.cfi_code)) AS cfi_code,
        upperUTF8(trimBoth(f.notional_currency)) AS notional_currency,
        f.admission_approval_at,
        f.first_trade_at,
        f.termination_at,
        f.source_record_id,
        f.source_publication_date,
        f.source_retrieved_at
    FROM corpscout.firds_instruments_current AS f
    WHERE {firds_scope}
      AND trimBoth(f.isin) != ''
      AND trimBoth(f.mic) != ''
      AND trimBoth(f.issuer_lei) != ''
),
gleif_latest AS
(
    SELECT
        upperUTF8(trimBoth(lei)) AS lei,
        argMax(
            tuple(ifNull(jurisdiction, ''), ifNull(registered_as, '')),
            tuple(resolved_at, source_run_id)
        ) AS legal_entity
    FROM corpscout.gleif_lei_records
    WHERE trimBoth(lei) != ''
    GROUP BY lei
),
gleif_sweden AS
(
    SELECT
        lei,
        replaceRegexpAll(legal_entity.2, '[^0-9]', '') AS company_id
    FROM gleif_latest
    WHERE upperUTF8(legal_entity.1) = 'SE'
      AND length(replaceRegexpAll(legal_entity.2, '[^0-9]', '')) = 10
),
eodhd_ranked AS
(
    SELECT
        upperUTF8(trimBoth(ifNull(s.isin, ''))) AS isin,
        upperUTF8(trimBoth(m.mic)) AS mic,
        argMax(
            tuple(
                s.eodhd_symbol_key,
                s.ticker,
                s.instrument_type,
                s.is_delisted
            ),
            tuple(
                toUInt8(s.is_delisted = 0),
                m.is_primary,
                greatest(s.retrieved_at, m.resolved_at),
                s.eodhd_symbol_key
            )
        ) AS evidence
    FROM corpscout.eodhd_symbols AS s
    INNER JOIN corpscout.eodhd_symbol_mics AS m
        ON m.eodhd_symbol_key = s.eodhd_symbol_key
    WHERE trimBoth(ifNull(s.isin, '')) != ''
      AND upperUTF8(trimBoth(m.mic)) IN ({sweden_mics})
    GROUP BY isin, mic
),
eodhd_by_listing AS
(
    SELECT
        isin,
        mic,
        evidence.1 AS eodhd_symbol_key,
        evidence.2 AS ticker,
        evidence.3 AS instrument_type,
        evidence.4 AS eodhd_is_delisted
    FROM eodhd_ranked
)
SELECT
    '{COUNTRY_CODE}' AS country_code,
    c.company_id AS company_id,
    f.issuer_lei AS issuer_lei,
    f.isin AS isin,
    f.mic AS mic,
    e.ticker AS ticker,
    if(f.short_name != '', f.short_name, f.full_name) AS instrument_name,
    if(e.instrument_type = '', 'Equity', e.instrument_type) AS instrument_type,
    f.cfi_code AS cfi_code,
    f.notional_currency AS notional_currency,
    toDate(f.admission_approval_at) AS admission_date,
    toDate(f.first_trade_at) AS first_trade_date,
    toDate(f.termination_at) AS termination_date,
    'current' AS trading_status,
    toUInt8(1) AS is_current,
    '{IDENTITY_MATCH_METHOD}' AS identity_match_method,
    'exact' AS identity_match_confidence,
    'esma_firds_current' AS listing_status_source,
    toUInt8(e.eodhd_symbol_key != '' AND e.eodhd_is_delisted = 1)
        AS status_conflict,
    e.eodhd_symbol_key AS eodhd_symbol_key,
    if(
        e.eodhd_symbol_key = '',
        CAST(NULL AS Nullable(UInt8)),
        toNullable(e.eodhd_is_delisted)
    ) AS eodhd_is_delisted,
    'esma_firds' AS source_slug,
    f.source_record_id AS source_record_id,
    f.source_publication_date AS source_publication_date,
    f.source_retrieved_at AS source_retrieved_at,
    %(source_run_id)s AS source_run_id,
    %(resolved_at)s AS resolved_at
FROM firds_scope AS f
INNER JOIN gleif_sweden AS g ON g.lei = f.issuer_lei
INNER JOIN corpscout.se_companies AS c ON c.company_id = g.company_id
LEFT JOIN eodhd_by_listing AS e
    ON e.isin = f.isin AND e.mic = f.mic"""


def _quality_sql(stage_table: str) -> str:
    firds_scope = _firds_scope_predicate("f")
    return f"""WITH firds_candidates AS
(
    SELECT issuer_lei
    FROM corpscout.firds_instruments_current AS f
    WHERE {firds_scope}
      AND trimBoth(f.isin) != ''
      AND trimBoth(f.mic) != ''
)
SELECT
    (SELECT count() FROM firds_candidates) AS candidate_rows,
    (
        SELECT countIf(trimBoth(issuer_lei) != '')
        FROM firds_candidates
    ) AS candidates_with_issuer_lei,
    count() AS row_count,
    uniqExact(company_id) AS company_count,
    uniqExact(isin) AS isin_count,
    uniqExact(mic) AS mic_count,
    uniqExact((company_id, isin, mic)) AS listing_key_count,
    countIf(
        country_code != '{COUNTRY_CODE}'
        OR company_id = ''
        OR issuer_lei = ''
        OR isin = ''
        OR mic = ''
        OR is_current != 1
    ) AS invalid_identity_rows,
    max(source_publication_date) AS latest_source_publication_date
FROM {stage_table}"""


def _quality_metadata(row: tuple[object, ...]) -> dict[str, object]:
    return {column: value for column, value in zip(_QUALITY_COLUMNS, row, strict=True)}


def _validate_quality(quality: dict[str, object]) -> None:
    candidate_rows = int(quality["candidate_rows"])
    resolved_rows = int(quality["row_count"])
    listing_key_count = int(quality["listing_key_count"])
    invalid_identity_rows = int(quality["invalid_identity_rows"])

    if candidate_rows == 0:
        raise ValueError("FIRDS has no current Sweden equity listing candidates")
    if resolved_rows == 0:
        raise ValueError("Sweden listing reconciliation produced no resolved listings")
    if resolved_rows > candidate_rows:
        raise ValueError(
            "Sweden listing reconciliation expanded the FIRDS candidate grain: "
            f"resolved={resolved_rows} candidates={candidate_rows}"
        )
    if listing_key_count != resolved_rows:
        raise ValueError(
            "Sweden company listing grain mismatch: "
            f"rows={resolved_rows} unique_keys={listing_key_count}"
        )
    if invalid_identity_rows != 0:
        raise ValueError(
            "Sweden company listings contain invalid identity rows: "
            f"{invalid_identity_rows}"
        )


def replace_se_company_listings_clickhouse(
    *,
    clickhouse: ClickhouseResource,
    source_run_id: str,
    resolved_at: datetime,
) -> dict[str, object]:
    """Atomically rebuild the Sweden listing table from current sources."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.CLICKHOUSE_DATABASE,
        tables=_REQUIRED_CLICKHOUSE_TABLES,
    )
    stage_name = f"_tmp_{tables.SE_COMPANY_LISTINGS_TABLE}_{uuid.uuid4().hex}"
    qualified_stage = _qualified(stage_name)
    qualified_target = _qualified(tables.SE_COMPANY_LISTINGS_TABLE)

    with clickhouse.get_connection() as client:
        client.execute(f"CREATE TABLE {qualified_stage} AS {qualified_target}")
        primary_error: Exception | None = None
        try:
            client.execute(
                build_se_company_listings_insert_sql(qualified_stage),
                {
                    "source_run_id": source_run_id,
                    "resolved_at": resolved_at,
                },
            )
            quality = _quality_metadata(
                client.execute(_quality_sql(qualified_stage))[0]
            )
            _validate_quality(quality)
            client.execute(f"EXCHANGE TABLES {qualified_stage} AND {qualified_target}")
        except Exception as exc:
            primary_error = exc
            raise
        finally:
            try:
                client.execute(f"DROP TABLE IF EXISTS {qualified_stage}")
            except Exception:
                if primary_error is None:
                    raise

    latest_source_date = quality["latest_source_publication_date"]
    candidate_rows = int(quality["candidate_rows"])
    row_count = int(quality["row_count"])
    metadata: dict[str, object] = {
        **quality,
        "latest_source_publication_date": (
            latest_source_date.isoformat()
            if isinstance(latest_source_date, (date, datetime))
            else str(latest_source_date or "")
        ),
        "unresolved_candidate_rows": candidate_rows - row_count,
        "resolution_rate": round(row_count / candidate_rows, 6),
        "table": tables.QUALIFIED_SE_COMPANY_LISTINGS_TABLE,
        "source_run_id": source_run_id,
    }
    return metadata


@dg.asset(
    name="se_company_listings_clickhouse",
    deps=[dg.AssetKey(key) for key in SE_LISTING_UPSTREAM_ASSET_KEYS],
    group_name=tables.GROUP_NAME,
    kinds={"clickhouse", "sql"},
    pool="company_listings_clickhouse",
    metadata={"table": tables.QUALIFIED_SE_COMPANY_LISTINGS_TABLE},
    description=(
        "Atomically rebuilds current Swedish company listings from FIRDS "
        "issuer LEIs, exact GLEIF registered_as organisation numbers, and "
        "optional exact-ISIN/MIC EODHD ticker evidence."
    ),
)
def se_company_listings_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    metadata = replace_se_company_listings_clickhouse(
        clickhouse=clickhouse,
        source_run_id=context.run_id,
        resolved_at=datetime.now(UTC),
    )
    context.log.info(
        "Rebuilt Sweden company listings: rows=%s companies=%s unresolved=%s",
        metadata["row_count"],
        metadata["company_count"],
        metadata["unresolved_candidate_rows"],
    )
    return dg.MaterializeResult(metadata=metadata)


se_company_listings_job = dg.define_asset_job(
    "se_company_listings_job",
    selection=dg.AssetSelection.assets("se_company_listings_clickhouse"),
)

defs = dg.Definitions(
    assets=[se_company_listings_clickhouse],
    jobs=[se_company_listings_job],
)
