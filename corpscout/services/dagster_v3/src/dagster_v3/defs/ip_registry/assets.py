"""Daily refresh of the IP registry reference data and the RDAP registration classes.

Seven small public files (IANA ipv4/ipv6 address space, five RIR delegated-extended
statistics, ~45 MB in total) are downloaded and verified as whole files (checksum, version
line, record and summary counts, no sharp shrink of the whole-file record count against the
current snapshot). Only the IANA blocks, the RIRs' available/reserved ranges (special
segments) and their whole-block allocated/assigned records (holder blocks) are inserted, as a
dated snapshot whose ledger row is written after its rows. A file re-published for the current
date never empties the current snapshot: its rows are inserted with a newer loaded_at (the
ReplacingMergeTree version), the ledger row follows, and only then are the older rows of that
snapshot deleted, so readers see the old rows, then old plus new, then the new rows. The loader
then drops every partition of that source except the current and the previous snapshot. Then
every cached RDAP registration is classified in SQL and rdap_network_trie is reloaded.
Non-partitioned full refresh (the whole dataset comes back per request), daily schedule
stopped by default, one pool for the chain.
"""

import hashlib
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from email.utils import parsedate_to_datetime
from ipaddress import IPv6Address

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from dlt.sources.helpers import requests
from pydantic import Field

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.commoncrawl_rdap.registry import REGISTRY_CLASS_REFRESH_SQL
from dagster_v3.defs.ip_registry import tables
from dagster_v3.defs.ip_registry.source import (
    HolderBlock,
    SpecialSegment,
    parse_delegated,
    parse_iana_csv,
    parse_md5,
)

GROUP_NAME = "ip_registry"
INSERT_BATCH = 50_000
# Refuse a snapshot whose whole-file ipv4 or ipv6 record count dropped by more than this share
# against the current one (a truncated download, a moved file) unless the run says allow_shrink.
# The kept special-segment counts are not guarded: they swing legitimately every day.
MAX_SHRINK_RATIO = 0.05
# Complete registries: 256 IPv4 /8s; the IPv6 unicast file had 51 rows on 2026-09-25 and
# only grows.
IANA_MIN_ROWS = {"iana_ipv4": 256, "iana_ipv6": 40}
FRESH_SNAPSHOT_DAYS = 3  # the RIRs publish daily
FRESH_VERIFIED_DAYS = 2  # every source must have been re-checked recently
# The registries' servers answer slowly or 5xx now and then; a loader retries the whole
# download after five minutes (dlt's session already retries single requests).
LOADER_RETRY_POLICY = dg.RetryPolicy(max_retries=2, delay=300)
USER_AGENT = "CorpScout ip-registry/1.0"
SNAPSHOT_INSERT_SQL = f"INSERT INTO corpscout.{tables.SNAPSHOTS_TABLE} ({', '.join(tables.SNAPSHOT_COLUMNS)}) VALUES"
IANA_INSERT_SQL = f"INSERT INTO corpscout.{tables.IANA_TABLE} ({', '.join(tables.IANA_COLUMNS)}) VALUES"
SPECIAL_INSERT_SQL = f"INSERT INTO corpscout.{tables.SPECIAL_TABLE} ({', '.join(tables.SPECIAL_COLUMNS)}) VALUES"
HOLDER_INSERT_SQL = f"INSERT INTO corpscout.{tables.HOLDER_TABLE} ({', '.join(tables.HOLDER_COLUMNS)}) VALUES"
# The data tables an RIR snapshot writes, both partitioned by (registry, snapshot_date).
RIR_DATA_TABLES = (tables.SPECIAL_TABLE, tables.HOLDER_TABLE)


class IpRegistryConfig(dg.Config):
    allow_shrink: bool = Field(
        default=False,
        description="Accept a file with more than 5% fewer ipv4 or ipv6 records than the current snapshot.",
    )


def fetch(url: str) -> tuple[bytes, Mapping[str, str]]:
    """The body and headers of a small public file (dlt's session retries connection errors and 5xx)."""
    response = requests.get(url, timeout=(10, 300), headers={"User-Agent": USER_AGENT})
    response.raise_for_status()
    return response.content, response.headers


def fetch_delegated(url: str) -> tuple[bytes, str]:
    """A delegated file and its .md5 text; downloaded once more when the digests disagree.

    A mismatch is usually a file replaced between the two requests. The second answer is
    returned either way; load_delegated_source refuses it if it still disagrees.
    """
    body, md5_text = b"", ""
    for _ in range(2):
        body, _ = fetch(url)
        md5_body, _ = fetch(url + ".md5")
        md5_text = md5_body.decode("utf-8")
        if hashlib.md5(body).hexdigest() == parse_md5(md5_text):
            break
    return body, md5_text


def freshness_now() -> datetime:
    """The clock of the freshness checks (a seam so tests do not depend on today's date)."""
    return datetime.now(UTC)


def snapshot_loaded_at() -> datetime:
    """Now, truncated to the millisecond of the DateTime64(3) loaded_at columns."""
    now = datetime.now(UTC)
    return now.replace(microsecond=now.microsecond // 1000 * 1000)


def current_snapshot(client, source: str) -> dict | None:
    rows = client.execute(
        f"""SELECT snapshot_date, checksum, records_ipv4, records_ipv6
        FROM corpscout.{tables.SNAPSHOTS_TABLE} FINAL
        WHERE source = %(source)s ORDER BY snapshot_date DESC LIMIT 1""",
        {"source": source},
    )
    if not rows:
        return None
    snapshot_date, checksum, records_ipv4, records_ipv6 = rows[0]
    return {
        "snapshot_date": snapshot_date,
        "checksum": checksum,
        "records_ipv4": records_ipv4,
        "records_ipv6": records_ipv6,
    }


def refuse_shrink(
    source: str, current: dict | None, ipv4: int, ipv6: int, *, allow_shrink: bool
) -> None:
    if current is None or allow_shrink:
        return
    for kind, new, old in (
        ("ipv4", ipv4, current["records_ipv4"]),
        ("ipv6", ipv6, current["records_ipv6"]),
    ):
        if old and new < old * (1 - MAX_SHRINK_RATIO):
            raise ValueError(
                f"{source}: {kind} records dropped from {old} to {new} (more than "
                f"{MAX_SHRINK_RATIO:.0%}); set allow_shrink to accept the snapshot"
            )


def refuse_older(source: str, current: dict | None, snapshot_date: date) -> None:
    if current is not None and snapshot_date < current["snapshot_date"]:
        raise ValueError(
            f"{source}: file date {snapshot_date} is older than the current snapshot "
            f"{current['snapshot_date']}"
        )


def record_snapshot(
    client,
    *,
    source: str,
    snapshot_date: date,
    checksum: str,
    serial: str,
    records: tuple[int, int],
    segments: tuple[int, int],
    holders: tuple[int, int] = (0, 0),
    url: str,
) -> None:
    """Write (or re-verify) the ledger row; the _current views switch to it from here on."""
    client.execute(
        SNAPSHOT_INSERT_SQL,
        [
            (
                source,
                snapshot_date,
                datetime.now(UTC),
                checksum,
                serial,
                *records,
                *segments,
                *holders,
                url,
            )
        ],
    )


def insert_rows(client, sql: str, rows: Sequence[tuple]) -> None:
    for offset in range(0, len(rows), INSERT_BATCH):
        client.execute(sql, rows[offset : offset + INSERT_BATCH])


def drop_snapshot_partition(
    client, *, table: str, source: str, snapshot_date: date
) -> None:
    """Drop one (source|registry, snapshot_date) partition; a missing partition is a no-op."""
    client.execute(
        f"ALTER TABLE corpscout.{table} DROP PARTITION (%(source)s, %(snapshot_date)s)",
        {"source": source, "snapshot_date": snapshot_date},
    )


def delete_stale_rows(
    client, *, table: str, source: str, snapshot_date: date, loaded_at: datetime
) -> None:
    """Delete the rows of one snapshot partition written before loaded_at.

    A synchronous mutation (mutations_sync=2) restricted to the partition: the partitions are
    small (the largest RIR snapshot is about 100k rows) and a mutation physically rewrites the
    parts, so FINAL readers never meet a lightweight-delete mask over a ReplacingMergeTree.
    """
    client.execute(
        f"""ALTER TABLE corpscout.{table} DELETE IN PARTITION (%(source)s, %(snapshot_date)s)
        WHERE loaded_at < toDateTime64(%(loaded_at)s, 3, 'UTC')""",
        {
            "source": source,
            "snapshot_date": snapshot_date,
            "loaded_at": loaded_at.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        },
        settings={"mutations_sync": 2},
    )


def drop_superseded_snapshots(
    client, *, table: str, key_column: str, source: str
) -> list[date]:
    """Drop every partition of a source except its SNAPSHOTS_KEPT newest ledger snapshots.

    Partitions are (source|registry, snapshot_date); the ledger decides which snapshots are
    worth keeping, so a partial load that never got its ledger row is dropped as well. The
    ledger itself is never trimmed.
    """
    kept = {
        snapshot_date
        for (snapshot_date,) in client.execute(
            f"""SELECT snapshot_date FROM corpscout.{tables.SNAPSHOTS_TABLE} FINAL
            WHERE source = %(source)s ORDER BY snapshot_date DESC LIMIT {tables.SNAPSHOTS_KEPT}""",
            {"source": source},
        )
    }
    present = [
        snapshot_date
        for (snapshot_date,) in client.execute(
            f"SELECT DISTINCT snapshot_date FROM corpscout.{table} WHERE {key_column} = %(source)s ORDER BY snapshot_date",
            {"source": source},
        )
    ]
    dropped = []
    for snapshot_date in present:
        if snapshot_date in kept:
            continue
        drop_snapshot_partition(
            client, table=table, source=source, snapshot_date=snapshot_date
        )
        dropped.append(snapshot_date)
    return dropped


@dataclass(frozen=True)
class SnapshotRows:
    """The rows one snapshot writes to one data table, built for a given loaded_at."""

    table: str
    key_column: str
    insert_sql: str
    count: int
    build: Callable[[datetime], list[tuple]]


def stored_rows_match(
    client, *, source: str, snapshot_date: date, parts: Sequence[SnapshotRows]
) -> bool:
    """Whether readers (FINAL) see exactly as many rows of the snapshot as the file has.

    False after a run died between the ledger row and the stale-row delete: the file is
    identical to the ledger's, but rows it removed are still visible.
    """
    for part in parts:
        [(stored,)] = client.execute(
            f"""SELECT count() FROM corpscout.{part.table} FINAL
            WHERE {part.key_column} = %(source)s AND snapshot_date = %(snapshot_date)s""",
            {"source": source, "snapshot_date": snapshot_date},
        )
        if stored != part.count:
            return False
    return True


def store_snapshot(
    client,
    *,
    source: str,
    snapshot_date: date,
    current: dict | None,
    parts: Sequence[SnapshotRows],
    write_ledger: Callable[[], None],
) -> None:
    """Insert a snapshot's rows, then its ledger row, without ever emptying the current snapshot.

    New date: the partition can only hold leftovers of an earlier failed load (it is not
    current yet), so it is dropped first. Current date (a re-published file or a repair): the
    new rows go in with a newer loaded_at next to the current ones, so FINAL readers see the old
    rows, then a superset, and after the ledger row the rows written before loaded_at are
    deleted. A failure at any step leaves the old rows visible and the next run converges.
    """
    same_date = current is not None and current["snapshot_date"] == snapshot_date
    if not same_date:
        for part in parts:
            drop_snapshot_partition(
                client, table=part.table, source=source, snapshot_date=snapshot_date
            )
    loaded_at = snapshot_loaded_at()
    for part in parts:
        insert_rows(client, part.insert_sql, part.build(loaded_at))
    write_ledger()
    if same_date:
        for part in parts:
            delete_stale_rows(
                client,
                table=part.table,
                source=source,
                snapshot_date=snapshot_date,
                loaded_at=loaded_at,
            )


def iana_snapshot_date(headers: Mapping[str, str]) -> date:
    value = headers.get("Last-Modified")
    if not value:
        raise ValueError("IANA response carries no Last-Modified header")
    return parsedate_to_datetime(value).astimezone(UTC).date()


def load_iana_source(
    client,
    source: str,
    *,
    body: bytes,
    headers: Mapping[str, str],
    url: str,
    min_rows: int | None = None,
) -> dict:
    """Parse, validate and store one IANA file; an already loaded snapshot only refreshes verified_at.

    A file re-published under the same Last-Modified date with a different checksum replaces
    that snapshot (see store_snapshot).
    """
    blocks = parse_iana_csv(body.decode("utf-8-sig"), source)
    floor = IANA_MIN_ROWS[source] if min_rows is None else min_rows
    if len(blocks) < floor:
        raise ValueError(f"{source}: {len(blocks)} rows, expected at least {floor}")
    checksum = hashlib.sha256(body).hexdigest()
    snapshot_date = iana_snapshot_date(headers)
    current = current_snapshot(client, source)
    ipv4 = sum(block.ip_version == 4 for block in blocks)
    ipv6 = sum(block.ip_version == 6 for block in blocks)
    parts = [
        SnapshotRows(
            table=tables.IANA_TABLE,
            key_column="source",
            insert_sql=IANA_INSERT_SQL,
            count=len(blocks),
            build=lambda loaded_at: [
                (
                    block.source,
                    snapshot_date,
                    block.ip_version,
                    block.prefix,
                    IPv6Address(block.first),
                    IPv6Address(block.last),
                    block.designation,
                    block.rir,
                    block.status,
                    block.assigned_on,
                    block.whois,
                    block.rdap,
                    block.note,
                    loaded_at,
                )
                for block in blocks
            ],
        )
    ]

    def write_ledger() -> None:
        record_snapshot(
            client,
            source=source,
            snapshot_date=snapshot_date,
            checksum=checksum,
            serial=headers.get("Last-Modified", ""),
            records=(ipv4, ipv6),
            segments=(ipv4, ipv6),
            url=url,
        )

    identical = (
        current is not None
        and current["snapshot_date"] == snapshot_date
        and current["checksum"] == checksum
    )
    repaired = identical and not stored_rows_match(
        client, source=source, snapshot_date=snapshot_date, parts=parts
    )
    if identical and not repaired:
        write_ledger()
    else:
        refuse_older(source, current, snapshot_date)
        store_snapshot(
            client,
            source=source,
            snapshot_date=snapshot_date,
            current=current,
            parts=parts,
            write_ledger=write_ledger,
        )
    dropped = drop_superseded_snapshots(
        client, table=tables.IANA_TABLE, key_column="source", source=source
    )
    return {
        "source": source,
        "snapshot_date": snapshot_date.isoformat(),
        "rows": len(blocks),
        "loaded": not identical or repaired,
        "repaired": repaired,
        "sha256": checksum,
        "dropped_snapshots": len(dropped),
    }


def _range_row(
    row: SpecialSegment | HolderBlock, snapshot_date: date, loaded_at: datetime
) -> tuple:
    """A special segment or holder block as a row of SPECIAL_COLUMNS / HOLDER_COLUMNS."""
    return (
        row.registry,
        snapshot_date,
        row.ip_version,
        row.cc,
        row.status,
        row.start_address,
        row.value,
        IPv6Address(row.first),
        IPv6Address(row.last),
        list(row.cidrs),
        loaded_at,
    )


def load_delegated_source(
    client, registry: str, *, body: bytes, md5_text: str, url: str, allow_shrink: bool
) -> dict:
    """Verify the published MD5, parse the whole file, validate it against the current snapshot
    and store its special segments and holder blocks.

    Write order (store_snapshot): special rows, holder rows, the ledger row (which makes them
    current), stale rows of a re-published current date; then retention. An identical file
    only refreshes verified_at.
    """
    digest = hashlib.md5(body).hexdigest()
    expected = parse_md5(md5_text)
    if digest != expected:
        raise ValueError(
            f"{registry}: MD5 {digest} does not match the published {expected}"
        )
    parsed = parse_delegated(body.decode("utf-8"), registry)
    ipv4, ipv6 = parsed.summaries.get("ipv4", 0), parsed.summaries.get("ipv6", 0)
    if not ipv4 and not ipv6:
        raise ValueError(f"{registry}: no ipv4/ipv6 records")
    segments = (parsed.special_count(4), parsed.special_count(6))
    holders = (parsed.holder_count(4), parsed.holder_count(6))
    snapshot_date = parsed.header.end_date
    current = current_snapshot(client, registry)
    parts = [
        SnapshotRows(
            table=tables.SPECIAL_TABLE,
            key_column="registry",
            insert_sql=SPECIAL_INSERT_SQL,
            count=len(parsed.special),
            build=lambda loaded_at: [
                _range_row(segment, snapshot_date, loaded_at)
                for segment in parsed.special
            ],
        ),
        SnapshotRows(
            table=tables.HOLDER_TABLE,
            key_column="registry",
            insert_sql=HOLDER_INSERT_SQL,
            count=len(parsed.holders),
            build=lambda loaded_at: [
                _range_row(block, snapshot_date, loaded_at) for block in parsed.holders
            ],
        ),
    ]

    def write_ledger() -> None:
        record_snapshot(
            client,
            source=registry,
            snapshot_date=snapshot_date,
            checksum=digest,
            serial=parsed.header.serial,
            records=(ipv4, ipv6),
            segments=segments,
            holders=holders,
            url=url,
        )

    identical = (
        current is not None
        and current["snapshot_date"] == snapshot_date
        and current["checksum"] == digest
    )
    repaired = identical and not stored_rows_match(
        client, source=registry, snapshot_date=snapshot_date, parts=parts
    )
    if identical and not repaired:
        write_ledger()
    else:
        refuse_older(registry, current, snapshot_date)
        if not identical:
            refuse_shrink(registry, current, ipv4, ipv6, allow_shrink=allow_shrink)
        store_snapshot(
            client,
            source=registry,
            snapshot_date=snapshot_date,
            current=current,
            parts=parts,
            write_ledger=write_ledger,
        )
    dropped = set()
    for table in RIR_DATA_TABLES:
        dropped.update(
            drop_superseded_snapshots(
                client, table=table, key_column="registry", source=registry
            )
        )
    return {
        "source": registry,
        "snapshot_date": snapshot_date.isoformat(),
        "records_ipv4": ipv4,
        "records_ipv6": ipv6,
        "segments_ipv4": segments[0],
        "segments_ipv6": segments[1],
        "holders_ipv4": holders[0],
        "holders_ipv6": holders[1],
        "loaded": not identical or repaired,
        "repaired": repaired,
        "md5": digest,
        "serial": parsed.header.serial,
        "dropped_snapshots": len(dropped),
    }


def iana_blocks_asset(
    retry_policy: dg.RetryPolicy | None = LOADER_RETRY_POLICY,
) -> dg.AssetsDefinition:
    @dg.asset(
        name="ip_registry_iana_blocks",
        group_name=GROUP_NAME,
        kinds={"python", "clickhouse", "iana"},
        pool=tables.IP_REGISTRY_POOL,
        retry_policy=retry_policy,
        description="Downloads IANA's ipv4-address-space and ipv6-unicast-address-assignments CSVs "
        "and stores them as a dated snapshot (Last-Modified) of the top-level address blocks.",
    )
    def _asset(
        context: dg.AssetExecutionContext, clickhouse: ClickhouseResource
    ) -> dg.MaterializeResult:
        assert_clickhouse_tables_exist(
            clickhouse,
            database=tables.DATABASE,
            tables=(tables.SNAPSHOTS_TABLE, tables.IANA_TABLE),
        )
        metadata = {}
        with clickhouse.get_connection() as client:
            for source, url in tables.IANA_SOURCES.items():
                body, headers = fetch(url)
                result = load_iana_source(
                    client, source, body=body, headers=headers, url=url
                )
                context.log.info("%s: %s", source, result)
                metadata.update(
                    {
                        f"{source}_{key}": value
                        for key, value in result.items()
                        if key != "source"
                    }
                )
        return dg.MaterializeResult(metadata=metadata)

    return _asset


def special_segment_asset(
    registry: str,
    url: str,
    retry_policy: dg.RetryPolicy | None = LOADER_RETRY_POLICY,
) -> dg.AssetsDefinition:
    @dg.asset(
        name=f"ip_registry_special_segments_{registry}",
        group_name=GROUP_NAME,
        kinds={"python", "clickhouse", "rir"},
        pool=tables.IP_REGISTRY_POOL,
        retry_policy=retry_policy,
        description=f"Downloads {url} and its .md5, verifies the whole file and stores its "
        "available/reserved ipv4/ipv6 ranges and its whole-block allocated/assigned records as "
        "the snapshot dated by the file's end date, then reloads the special-segment trie.",
    )
    def _asset(
        context: dg.AssetExecutionContext,
        config: IpRegistryConfig,
        clickhouse: ClickhouseResource,
    ) -> dg.MaterializeResult:
        assert_clickhouse_tables_exist(
            clickhouse,
            database=tables.DATABASE,
            tables=(tables.SNAPSHOTS_TABLE, tables.SPECIAL_TABLE, tables.HOLDER_TABLE),
        )
        body, md5_text = fetch_delegated(url)
        with clickhouse.get_connection() as client:
            result = load_delegated_source(
                client,
                registry,
                body=body,
                md5_text=md5_text,
                url=url,
                allow_shrink=config.allow_shrink,
            )
            client.execute(f"SYSTEM RELOAD DICTIONARY corpscout.{tables.SPECIAL_TRIE}")
        context.log.info("%s: %s", registry, result)
        return dg.MaterializeResult(metadata=result)

    return _asset


ip_registry_iana_blocks = iana_blocks_asset()
special_segment_assets = [
    special_segment_asset(registry, url) for registry, url in tables.RIR_SOURCES.items()
]


def reference_ready(client) -> bool:
    [(ready,)] = client.execute(
        f"SELECT ifNull(any(ready), 0) FROM corpscout.{tables.READY_VIEW}"
    )
    return bool(ready)


@dg.asset(
    deps=[ip_registry_iana_blocks, *special_segment_assets],
    group_name=GROUP_NAME,
    kinds={"clickhouse", "rdap"},
    pool=tables.IP_REGISTRY_POOL,
    description="Reclassifies every cached RDAP registration (reusable, registry_level, "
    "unallocated) from the current reference snapshots, then reloads rdap_network_trie so "
    "excluded registrations stop being served.",
)
def rdap_network_registry_class(
    context: dg.AssetExecutionContext, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.DATABASE,
        tables=(
            "rdap_networks_current",
            "rdap_network_registry_class",
            "rdap_network_registry_class_derived",
            tables.READY_VIEW,
        ),
    )
    with clickhouse.get_connection() as client:
        # The RIR loaders reload it too; a run that loaded nothing still classifies fresh data.
        client.execute(f"SYSTEM RELOAD DICTIONARY corpscout.{tables.SPECIAL_TRIE}")
        if not reference_ready(client):
            raise ValueError(
                "Reference data is incomplete: every source needs a loaded snapshot before classification"
            )
        client.execute(REGISTRY_CLASS_REFRESH_SQL)
        counts = dict(
            client.execute(
                "SELECT registry_class, count() FROM corpscout.rdap_network_registry_class_current GROUP BY registry_class"
            )
        )
        client.execute("SYSTEM RELOAD DICTIONARY corpscout.rdap_network_trie")
    context.log.info("RDAP registrations classified: %s", counts)
    return dg.MaterializeResult(
        metadata={
            **{
                f"networks_{registry_class}": count
                for registry_class, count in counts.items()
            },
            "networks_total": sum(counts.values()),
        }
    )


def snapshot_freshness(
    sources: Sequence[str], rows: Sequence[tuple], now: datetime
) -> dg.AssetCheckResult:
    """Fail when a source has no snapshot, was not re-verified recently, or (RIRs) is stale."""
    latest = {
        source: (snapshot_date, verified_at)
        for source, snapshot_date, verified_at in rows
    }
    problems = []
    metadata = {}
    for source in sources:
        if source not in latest:
            problems.append(f"{source}: no snapshot")
            continue
        snapshot_date, verified_at = latest[source]
        if verified_at.tzinfo is None:
            verified_at = verified_at.replace(tzinfo=UTC)
        metadata[f"{source}_snapshot_date"] = snapshot_date.isoformat()
        metadata[f"{source}_verified_at"] = verified_at.isoformat()
        if now - verified_at > timedelta(days=FRESH_VERIFIED_DAYS):
            problems.append(f"{source}: last verified {verified_at.date()}")
        if not source.startswith("iana") and now.date() - snapshot_date > timedelta(
            days=FRESH_SNAPSHOT_DAYS
        ):
            problems.append(f"{source}: snapshot {snapshot_date} is stale")
    return dg.AssetCheckResult(
        passed=not problems,
        severity=dg.AssetCheckSeverity.ERROR,
        description="Reference snapshots are current."
        if not problems
        else "; ".join(problems),
        metadata=metadata,
    )


def freshness_check(
    asset: dg.AssetsDefinition, sources: tuple[str, ...]
) -> dg.AssetChecksDefinition:
    @dg.asset_check(
        asset=asset,
        name="snapshot_fresh",
        description=f"Fails when the latest snapshot of {', '.join(sources)} is older than "
        f"{FRESH_SNAPSHOT_DAYS} days (RIRs) or was not re-verified within {FRESH_VERIFIED_DAYS} days.",
    )
    def _check(clickhouse: ClickhouseResource) -> dg.AssetCheckResult:
        with clickhouse.get_connection() as client:
            rows = client.execute(
                f"""SELECT source, max(snapshot_date), max(verified_at)
                FROM corpscout.{tables.SNAPSHOTS_TABLE} FINAL
                WHERE source IN %(sources)s GROUP BY source""",
                {"sources": sources},
            )
        return snapshot_freshness(sources, rows, freshness_now())

    return _check


@dg.asset_check(
    asset=rdap_network_registry_class,
    name="classification_complete",
    description="Fails when the reference data is not ready or a cached registration has no class row.",
)
def classification_complete(clickhouse: ClickhouseResource) -> dg.AssetCheckResult:
    with clickhouse.get_connection() as client:
        ready = reference_ready(client)
        [(missing,)] = client.execute(
            """SELECT count() FROM corpscout.rdap_networks_current
            WHERE network_key NOT IN (SELECT network_key FROM corpscout.rdap_network_registry_class_current)"""
        )
    return dg.AssetCheckResult(
        passed=ready and missing == 0,
        severity=dg.AssetCheckSeverity.ERROR,
        description=f"ready={ready}, registrations without a class row: {missing}",
        metadata={"ready": ready, "unclassified_networks": int(missing)},
    )


checks = [
    freshness_check(ip_registry_iana_blocks, tuple(tables.IANA_SOURCES)),
    *(
        freshness_check(asset, (registry,))
        for asset, registry in zip(
            special_segment_assets, tables.RIR_SOURCES, strict=True
        )
    ),
    classification_complete,
]

ip_registry_refresh_job = dg.define_asset_job(
    "ip_registry_refresh_job",
    selection=dg.AssetSelection.assets(rdap_network_registry_class).upstream(),
)
# Daily, 06:05 UTC: the RIR files dated D are all published by then (APNIC publishes D's file
# on D+1 at +10:00) and a block allocated yesterday must not stay "unallocated" for a week.
# No other schedule uses this minute. STOPPED by default; start it at instance level.
ip_registry_daily = dg.ScheduleDefinition(
    name="ip_registry_daily",
    job=ip_registry_refresh_job,
    cron_schedule="5 6 * * *",
    execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)

defs = dg.Definitions(
    assets=[
        ip_registry_iana_blocks,
        *special_segment_assets,
        rdap_network_registry_class,
    ],
    asset_checks=checks,
    jobs=[ip_registry_refresh_job],
    schedules=[ip_registry_daily],
)
