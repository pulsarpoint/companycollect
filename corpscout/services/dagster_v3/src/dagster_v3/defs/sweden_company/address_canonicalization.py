import time
from collections.abc import Callable, Iterator, Sequence
from datetime import datetime
from typing import Any

import pyarrow as pa

ENRICHMENT_SCHEMA = "sweden_company_enrichment"
CANONICAL_ADDRESSES_TABLE = "se_company_addresses_canonical_current"
ADDRESS_MEMBERS_TABLE = "se_company_address_members_current"
QUALIFIED_CANONICAL_ADDRESSES_TABLE = (
    f"{ENRICHMENT_SCHEMA}.{CANONICAL_ADDRESSES_TABLE}"
)
QUALIFIED_ADDRESS_MEMBERS_TABLE = f"{ENRICHMENT_SCHEMA}.{ADDRESS_MEMBERS_TABLE}"

CLICKHOUSE_DATABASE = "corpscout"
QUALIFIED_CLICKHOUSE_CANONICAL_ADDRESSES_TABLE = (
    f"{CLICKHOUSE_DATABASE}.{CANONICAL_ADDRESSES_TABLE}"
)
QUALIFIED_CLICKHOUSE_ADDRESS_MEMBERS_TABLE = (
    f"{CLICKHOUSE_DATABASE}.{ADDRESS_MEMBERS_TABLE}"
)

QUERY_BATCH_SIZE = 25_000
PROGRESS_LOG_ROW_INTERVAL = 500_000
_ARROW_RELATION = "_sweden_company_address_observation_batch"

SOURCE_ADDRESS_COLUMNS = (
    "company_id",
    "address_key",
    "address_type",
    "address_source",
    "raw_address",
    "street_address",
    "care_of",
    "postal_code",
    "post_town",
    "country_code",
    "registry_source_record_uid",
    "registry_source_run_id",
    "source_observed_at",
)

CANONICAL_ADDRESS_COLUMNS = (
    "company_id",
    "canonical_address_key",
    "canonical_display_address",
    "representative_address_type",
    "representative_address_source",
    "representative_source_record_uid",
    "street_address",
    "care_of",
    "postal_code",
    "post_town",
    "country_code",
    "address_kind",
    "normalized_street",
    "normalized_postal_code",
    "normalized_post_town",
    "address_types",
    "address_sources",
    "member_count",
    "normalization_run_id",
    "normalized_at",
)

ADDRESS_MEMBER_COLUMNS = (
    "company_id",
    "canonical_address_key",
    "address_key",
    "address_type",
    "address_source",
    "raw_address",
    "display_address",
    "street_address",
    "care_of",
    "postal_code",
    "post_town",
    "country_code",
    "registry_source_record_uid",
    "registry_source_run_id",
    "source_observed_at",
    "normalization_run_id",
    "normalized_at",
)

CURRENT_COMPANY_ADDRESSES_SQL = """
SELECT
    company_id,
    toString(address_fingerprint) AS address_key,
    address_type,
    source AS address_source,
    ifNull(raw_address, '') AS raw_address,
    ifNull(street_address, '') AS street_address,
    ifNull(care_of, '') AS care_of,
    ifNull(postal_code, '') AS postal_code,
    ifNull(post_town, '') AS post_town,
    ifNull(country_code, '') AS country_code,
    source_record_uid AS registry_source_record_uid,
    source_run_id AS registry_source_run_id,
    toString(observed_at) AS source_observed_at
FROM corpscout.se_company_addresses_current
WHERE has_address = 1
  AND has_observation = 1
ORDER BY company_id, address_key
"""


def replace_sweden_company_canonical_addresses(
    *,
    connection: Any,
    clickhouse_client: Any,
    normalization_run_id: str,
    normalized_at: datetime,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Build canonical addresses without removing source-specific observations."""
    _load_current_company_addresses(
        connection=connection,
        clickhouse_client=clickhouse_client,
        log=log,
    )
    connection.execute("BEGIN TRANSACTION")
    try:
        connection.execute(f"create schema if not exists {ENRICHMENT_SCHEMA}")
        connection.execute(
            f"""
            create or replace table {QUALIFIED_ADDRESS_MEMBERS_TABLE} as
            with cleaned as (
                select
                    *,
                    case
                        when upper(trim(country_code)) = '' then 'SE'
                        else upper(trim(country_code))
                    end as resolved_country_code,
                    lower(regexp_replace(
                        trim(postal_code),
                        '[^[:alnum:]]+',
                        '',
                        'g'
                    )) as normalized_postal_code,
                    trim(regexp_replace(
                        lower(trim(post_town)),
                        '[^[:alnum:]]+',
                        ' ',
                        'g'
                    )) as normalized_post_town,
                    trim(regexp_replace(
                        lower(regexp_replace(
                            trim(street_address),
                            '[, ]+[0-9]+ +(tr|trappor?)$',
                            ''
                        )),
                        '[^[:alnum:]]+',
                        ' ',
                        'g'
                    )) as normalized_street_words
                from _sweden_company_address_observations
            ),
            identities as (
                select
                    *,
                    regexp_replace(
                        normalized_street_words,
                        '[^[:alnum:]]+',
                        '',
                        'g'
                    ) as normalized_street_compact
                from cleaned
            ),
            normalized as (
                select
                    *,
                    case
                        when regexp_matches(
                            normalized_street_compact,
                            '^(box|postbox|pobox)[[:alnum:]-]+$'
                        ) then concat(
                            'box',
                            regexp_extract(
                                normalized_street_compact,
                                '^(?:box|postbox|pobox)([[:alnum:]-]+)$',
                                1
                            )
                        )
                        else normalized_street_compact
                    end as normalized_street
                from identities
            ),
            keyed as (
                select
                    *,
                    sha256(concat_ws(
                        chr(31),
                        company_id,
                        resolved_country_code,
                        normalized_street,
                        normalized_postal_code,
                        normalized_post_town
                    )) as canonical_address_key,
                    array_to_string(list_filter([
                        care_of,
                        street_address,
                        trim(concat_ws(' ', postal_code, post_town)),
                        case
                            when resolved_country_code = 'SE' then ''
                            else resolved_country_code
                        end
                    ], value -> trim(value) != ''), ', ') as display_address
                from normalized
            )
            select
                company_id,
                canonical_address_key,
                address_key,
                address_type,
                address_source,
                raw_address,
                display_address,
                street_address,
                care_of,
                postal_code,
                post_town,
                resolved_country_code as country_code,
                registry_source_record_uid,
                registry_source_run_id,
                cast(source_observed_at as timestamptz) as source_observed_at,
                ?::varchar as normalization_run_id,
                ?::timestamptz as normalized_at
            from keyed
            """,
            [normalization_run_id, normalized_at],
        )
        connection.execute(
            f"""
            create or replace table {QUALIFIED_CANONICAL_ADDRESSES_TABLE} as
            with ranked as (
                select
                    members.*,
                    trim(regexp_replace(
                        lower(trim(post_town)),
                        '[^[:alnum:]]+',
                        ' ',
                        'g'
                    )) as normalized_post_town,
                    lower(regexp_replace(
                        trim(postal_code),
                        '[^[:alnum:]]+',
                        '',
                        'g'
                    )) as normalized_postal_code,
                    case
                        when regexp_matches(
                            regexp_replace(
                                lower(trim(street_address)),
                                '[^[:alnum:]]+',
                                '',
                                'g'
                            ),
                            '^(box|postbox|pobox)[[:alnum:]-]+$'
                        ) then concat(
                            'box',
                            regexp_extract(
                                regexp_replace(
                                    lower(trim(street_address)),
                                    '[^[:alnum:]]+',
                                    '',
                                    'g'
                                ),
                                '^(?:box|postbox|pobox)([[:alnum:]-]+)$',
                                1
                            )
                        )
                        else regexp_replace(
                            lower(regexp_replace(
                                trim(street_address),
                                '[, ]+[0-9]+ +(tr|trappor?)$',
                                ''
                            )),
                            '[^[:alnum:]]+',
                            '',
                            'g'
                        )
                    end as normalized_street,
                    row_number() over (
                        partition by company_id, canonical_address_key
                        order by
                            (street_address != upper(street_address)) desc,
                            (address_source = 'bolagsverket') desc,
                            length(display_address) desc,
                            address_source,
                            address_type,
                            address_key
                    ) as representative_rank
                from {QUALIFIED_ADDRESS_MEMBERS_TABLE} members
            ),
            grouped as (
                select
                    company_id,
                    canonical_address_key,
                    first(display_address order by representative_rank)
                        as canonical_display_address,
                    first(address_type order by representative_rank)
                        as representative_address_type,
                    first(address_source order by representative_rank)
                        as representative_address_source,
                    first(registry_source_record_uid order by representative_rank)
                        as representative_source_record_uid,
                    first(street_address order by representative_rank) as street_address,
                    first(care_of order by representative_rank) as care_of,
                    first(postal_code order by representative_rank) as postal_code,
                    first(post_town order by representative_rank) as post_town,
                    first(country_code order by representative_rank) as country_code,
                    first(normalized_street order by representative_rank)
                        as normalized_street,
                    first(normalized_postal_code order by representative_rank)
                        as normalized_postal_code,
                    first(normalized_post_town order by representative_rank)
                        as normalized_post_town,
                    list_sort(list_distinct(list(address_type))) as address_types,
                    list_sort(list_distinct(list(address_source))) as address_sources,
                    count(*)::usmallint as member_count,
                    first(normalization_run_id order by representative_rank)
                        as normalization_run_id,
                    first(normalized_at order by representative_rank) as normalized_at
                from ranked
                group by company_id, canonical_address_key
            )
            select
                company_id,
                canonical_address_key,
                canonical_display_address,
                representative_address_type,
                representative_address_source,
                representative_source_record_uid,
                street_address,
                care_of,
                postal_code,
                post_town,
                country_code,
                case
                    when country_code != 'SE' then 'foreign'
                    when regexp_matches(normalized_street, '^box[[:alnum:]-]+$')
                        then 'postal_box'
                    when normalized_street = '' or normalized_postal_code in ('', '00000')
                        then 'incomplete'
                    else 'physical'
                end as address_kind,
                normalized_street,
                normalized_postal_code,
                normalized_post_town,
                address_types,
                address_sources,
                member_count,
                normalization_run_id,
                normalized_at
            from grouped
            """
        )
        _assert_canonical_address_invariants(connection)
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise

    counts = {
        "source_observations": _count(
            connection,
            "_sweden_company_address_observations",
        ),
        "canonical_addresses": _count(
            connection,
            QUALIFIED_CANONICAL_ADDRESSES_TABLE,
        ),
        "canonical_members": _count(
            connection,
            QUALIFIED_ADDRESS_MEMBERS_TABLE,
        ),
    }
    counts["deduplicated_observations"] = (
        counts["source_observations"] - counts["canonical_addresses"]
    )
    _log(log, "Completed Sweden company address canonicalization: counts=%s", counts)
    return counts


def _assert_canonical_address_invariants(connection: Any) -> None:
    [(source_rows, member_rows, assigned_source_rows)] = connection.execute(
        f"""
        select
            (select count(*) from _sweden_company_address_observations),
            (select count(*) from {QUALIFIED_ADDRESS_MEMBERS_TABLE}),
            (select count(distinct (
                company_id,
                address_source,
                address_type,
                address_key
            ))
             from {QUALIFIED_ADDRESS_MEMBERS_TABLE})
        """
    ).fetchall()
    [(canonical_rows, unique_canonical_rows, conflicting_countries)] = (
        connection.execute(
            f"""
            select
                count(*),
                count(distinct (company_id, canonical_address_key)),
                count(*) filter (where country_count > 1)
            from (
                select
                    canonical.company_id,
                    canonical.canonical_address_key,
                    count(distinct members.country_code) as country_count
                from {QUALIFIED_CANONICAL_ADDRESSES_TABLE} canonical
                join {QUALIFIED_ADDRESS_MEMBERS_TABLE} members using (
                    company_id,
                    canonical_address_key
                )
                group by canonical.company_id, canonical.canonical_address_key
            ) groups
            """
        ).fetchall()
    )
    if int(source_rows) != int(member_rows) or int(source_rows) != int(
        assigned_source_rows
    ):
        raise ValueError(
            "Every current Sweden address observation must map to exactly one "
            "canonical address"
        )
    if int(canonical_rows) != int(unique_canonical_rows):
        raise ValueError("Canonical Sweden company address keys must be unique")
    if int(conflicting_countries) != 0:
        raise ValueError("A canonical Sweden company address cannot span countries")


def _load_current_company_addresses(
    *,
    connection: Any,
    clickhouse_client: Any,
    log: Callable[..., object] | None,
) -> None:
    connection.execute(
        """
        create or replace temporary table _sweden_company_address_observations (
            company_id varchar,
            address_key varchar,
            address_type varchar,
            address_source varchar,
            raw_address varchar,
            street_address varchar,
            care_of varchar,
            postal_code varchar,
            post_town varchar,
            country_code varchar,
            registry_source_record_uid varchar,
            registry_source_run_id varchar,
            source_observed_at varchar
        )
        """
    )
    started_at = time.monotonic()
    loaded_rows = 0
    batch: list[Sequence[object]] = []
    for row in _iter_clickhouse_rows(clickhouse_client):
        batch.append(row)
        if len(batch) < QUERY_BATCH_SIZE:
            continue
        _insert_company_address_batch(connection, batch)
        loaded_rows += len(batch)
        batch.clear()
        if loaded_rows % PROGRESS_LOG_ROW_INTERVAL == 0:
            _log(
                log,
                "Loading current Sweden address observations: rows=%d elapsed_seconds=%.1f",
                loaded_rows,
                time.monotonic() - started_at,
            )
    _insert_company_address_batch(connection, batch)
    loaded_rows += len(batch)
    if loaded_rows == 0:
        raise ValueError("Sweden company address source returned zero current rows")
    _log(
        log,
        "Loaded current Sweden address observations: rows=%d elapsed_seconds=%.1f",
        loaded_rows,
        time.monotonic() - started_at,
    )


def _iter_clickhouse_rows(clickhouse_client: Any) -> Iterator[Sequence[object]]:
    execute_iter = getattr(clickhouse_client, "execute_iter", None)
    if callable(execute_iter):
        yield from execute_iter(
            CURRENT_COMPANY_ADDRESSES_SQL,
            settings={"max_block_size": QUERY_BATCH_SIZE},
        )
        return
    yield from clickhouse_client.execute(CURRENT_COMPANY_ADDRESSES_SQL)


def _insert_company_address_batch(
    connection: Any,
    rows: Sequence[Sequence[object]],
) -> None:
    if not rows:
        return
    arrow_table = pa.Table.from_arrays(
        [
            pa.array(
                ["" if row[index] is None else str(row[index]) for row in rows],
                type=pa.string(),
            )
            for index in range(len(SOURCE_ADDRESS_COLUMNS))
        ],
        names=SOURCE_ADDRESS_COLUMNS,
    )
    connection.register(_ARROW_RELATION, arrow_table)
    try:
        columns = ", ".join(SOURCE_ADDRESS_COLUMNS)
        connection.execute(
            f"insert into _sweden_company_address_observations ({columns}) "
            f"select {columns} from {_ARROW_RELATION}"
        )
    finally:
        connection.unregister(_ARROW_RELATION)


def _count(connection: Any, qualified_table: str) -> int:
    row = connection.execute(f"select count(*) from {qualified_table}").fetchone()
    return int(row[0]) if row is not None else 0


def _log(
    log: Callable[..., object] | None,
    message: str,
    *args: object,
) -> None:
    if log is not None:
        log(message, *args)
