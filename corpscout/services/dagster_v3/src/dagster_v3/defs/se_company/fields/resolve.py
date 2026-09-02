"""Registry-driven resolve of Swedish company fields, and the wide re-pivot.

Replaces se_company_info_clickhouse (info.py). Every statement this asset executes is
read from corpscout.se_company_field_registry -- the per-field ``resolve_sql`` and the
``field = '*'`` projection -- never rendered here: the export is the contract shared with
the backoffice, which runs the same statements for one company after a decision.

Per run: select the company set (config ``company_ids``, the changed-company scan, or
every company under ``resolve_all``); per batch of ``company_batch_size`` run each
field's statement in registry order, then the projection through publish_with_stage
(stage -> validate -> insert), then one counts query for the metadata.

Parameters: the statements carry ClickHouse ``{name:Type}`` placeholders, bound
SERVER-SIDE. clickhouse-driver ships them over the native protocol only from a Client
built with ``server_side_params=True`` (a client-level setting: ``open_resolve_client``
builds one from the resource's own fields). Two driver quirks, verified 2026-09-02
against 26.5 with the pinned 0.2.10: a Python list is double-quoted on the wire and a
pre-rendered str is double-escaped, so Array(String) values travel as ServerSideLiteral
-- a non-str the driver quotes without escaping, whose str() is the literal escaped
once; and a datetime is converted to the server timezone at second precision, so
``resolved_at`` travels as its millisecond text.

Gate: ``execute: true`` in the run config; a bare "Materialize" click is a preview that
runs the scan and reports what a real run would select, writing nothing.

Assets
  se_company_field_resolved_clickhouse -> corpscout.se_company_field, corpscout.se_company_info
"""

import re
from collections import defaultdict
from collections.abc import Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from dataclasses import field as dc_field
from datetime import UTC, datetime
from typing import Any

import dagster as dg
from clickhouse_driver import Client
from dagster_clickhouse import ClickhouseResource
from dagster_clickhouse.resource import client_kwargs_from_resource_config
from pydantic import Field

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.se_company.common import normalized_se_company_ids, publish_with_stage
from dagster_v3.defs.se_company.fields.registry import INFO_REGISTRY, DatatypeRegistry, field_names
from dagster_v3.defs.se_company.fields.tables import (
    SE_COMPANY_FIELD,
    SE_COMPANY_FIELD_CANDIDATE,
    SE_COMPANY_FIELD_REGISTRY,
    SE_COMPANY_INFO,
)

DATABASE = "corpscout"
GROUP_NAME = "se_company_fields"
RESOLVE_ASSET = "se_company_field_resolved_clickhouse"
REGISTRY_ASSET = "se_company_field_registry_clickhouse"
# The three per-source artifact assets the old se_company_info_job carried; the weekly
# field job carries them from now on (their freshness leaves must keep a schedule).
ARTIFACT_ASSETS = ("se_company_info_scb_clickhouse", "se_company_info_esef_clickhouse",
                   "se_company_info_wikidata_clickhouse")
CANDIDATE_ASSETS = tuple(
    f"se_company_field_candidates_{source}"
    for source in ("scb", "bolagsverket", "esef", "wikidata", "ratsit", "domains", "llm"))
LLM_CANDIDATES_ASSET = "se_company_field_candidates_llm"
PARITY_CHECK_NAME = "se_company_field_parity_check"
# The decisions table (000371). Named here rather than imported from info.py, which the
# cutover plan deletes.
SE_COMPANY_INFO_FIELD_VALUE = "se_company_info_field_value"
# The registry export's extra row carrying the wide projection statement (spec 4.3).
PROJECTION_FIELD = "*"
# Spec 8.3: a company without a legal name from a register is not published -- and,
# here, not resolved either (the scan's WHERE), so it never churns the long table.
REGISTER_NAME_FIELD = "legal_name"
REGISTER_NAME_SOURCES = ("bolagsverket", "scb")
# A LEFT JOIN miss reads as this instant, not as a bare NULL comparison.
EPOCH_SQL = "toDateTime64('1970-01-01 00:00:00', 3, 'UTC')"
# The wide table's own CHECKs, spelled for the stage validation exactly as info.py did.
WIDE_INVALID_CONDITION = "trim(legal_name) = '' OR empty(source_record_uids)"
# Why the scan picked a company; overlapping counters, never a partition (a never-
# published company also has candidates newer than its epoch resolved_at).
SELECTION_REASONS = ("never_published", "new_candidates", "decision_pending", "version_changed")
SELECTION_COLUMNS = ("company_id", *SELECTION_REASONS)
# What the sensors and the schedule send: an automated run must never be a preview.
AUTOMATED_RUN_CONFIG: dict[str, Any] = {"execute": True}


class SECompanyFieldResolveConfig(dg.Config):
    # False = preview: run the scan, report the selection, write nothing.
    execute: bool = False
    company_ids: list[str] = Field(default_factory=list)
    # None = unbounded (the weekly run). A capped resolve_all pass must give
    # resolve_all_before, see below.
    max_companies: int | None = Field(default=None, ge=1)
    # Scan page size and the resolve/projection batch. 20,000 ids are ~300 KB as one
    # Array(String) parameter, which the server takes without a settings change.
    company_batch_size: int = Field(default=20_000, ge=1, le=20_000)
    # True = re-resolve every in-scope company although nothing moved (registry or
    # policy edits are caught by version_changed; this is for everything else).
    resolve_all: bool = False
    # ISO-8601 UTC cutoff for resolve_all: only companies whose published resolved_at
    # is OLDER are selected, so a pass split over several capped runs carries on where
    # it stopped instead of re-selecting the same first slice. None = the run's own
    # instant, i.e. "this pass is one run".
    resolve_all_before: str | None = None
    # Registry field names to resolve; empty = every field. The projection always
    # re-pivots every field from the long table.
    fields: list[str] = Field(default_factory=list)


def clickhouse_stamp(moment: datetime) -> str:
    """``moment`` as the millisecond text ClickHouse parses for a DateTime64(3)."""
    return moment.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


@contextmanager
def open_resolve_client(clickhouse: ClickhouseResource) -> Iterator[Client]:
    """A driver client on the resource's own connection details, with server-side
    parameters on. ``server_side_params`` is a client-level setting (it decides whether
    ``execute`` substitutes ``%(name)s`` client-side or ships the params dict to the
    server), which is why the resource's ``get_connection`` client cannot be reused."""
    kwargs = client_kwargs_from_resource_config({
        "host": clickhouse.host, "port": clickhouse.port, "user": clickhouse.user,
        "password": clickhouse.password, "database": clickhouse.database, "secure": clickhouse.secure,
        "settings": {**dict(clickhouse.settings), "server_side_params": True}})
    client = Client(**kwargs)
    try:
        yield client
    finally:
        client.disconnect()


def _quoted(text: str) -> str:
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


class ServerSideLiteral:
    """An Array(String) parameter for the driver's server-side path.

    ``text`` is the ClickHouse literal (``['a','b']``). The driver quotes a non-str
    value's ``str()`` verbatim -- no escaping -- and the server unquotes that once, so
    ``__str__`` is the literal escaped exactly once. A plain str would be escaped twice
    and a list quoted per element; both are rejected by 26.5.
    """

    __slots__ = ("text",)

    def __init__(self, text: str) -> None:
        self.text = text

    def __str__(self) -> str:
        return self.text.replace("\\", "\\\\").replace("'", "\\'")

    def __repr__(self) -> str:
        return f"ServerSideLiteral({self.text!r})"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, ServerSideLiteral) and other.text == self.text


def server_array(items: Iterable[str]) -> ServerSideLiteral:
    return ServerSideLiteral("[" + ",".join(_quoted(str(item)) for item in items) + "]")


def server_params(*, company_ids: Sequence[str], **scalars: object) -> dict[str, object]:
    """The params dict for one statement: ``company_ids`` as an Array(String) literal, a
    datetime as its millisecond text, str and int values as they are."""
    params: dict[str, object] = {"company_ids": server_array(company_ids)}
    for name, value in scalars.items():
        params[name] = clickhouse_stamp(value) if isinstance(value, datetime) else value
    return params


@dataclass(frozen=True)
class InsertHeader:
    table: str
    columns: tuple[str, ...]
    body: str


_INSERT_HEADER = re.compile(
    r"^\s*INSERT\s+INTO\s+(?P<table>[A-Za-z_`][\w.`]*)\s*\((?P<columns>[^)]*)\)\s*(?P<body>.+)$",
    re.DOTALL)


def split_insert_header(sql: str) -> InsertHeader:
    """The projection statement is ``INSERT INTO <table> (<columns>) <select>``; the
    stage publish needs the three parts apart (it inserts the SELECT into the stage
    under the header's column list, then copies the stage into the target)."""
    match = _INSERT_HEADER.match(sql)
    if match is None:
        raise ValueError(f"Expected 'INSERT INTO <table> (<columns>) <select>', got: {sql[:120]!r}")
    columns = tuple(c.strip().strip("`") for c in match.group("columns").split(",") if c.strip())
    return InsertHeader(table=match.group("table").replace("`", ""), columns=columns,
                        body=match.group("body").strip())


def build_registry_statements_sql(registry: DatatypeRegistry) -> str:
    return f"""SELECT field,
    argMax(resolve_sql, version) AS resolve_sql,
    argMax(policy_version, version) AS policy_version,
    argMax(registry_version, version) AS registry_version
FROM {SE_COMPANY_FIELD_REGISTRY}
WHERE datatype = '{registry.datatype}' AND country = '{registry.country}'
GROUP BY field
ORDER BY field"""


@dataclass(frozen=True)
class RegistryStatements:
    registry_version: str
    resolve_sql: Mapping[str, str]  # field -> statement, in registry order
    projection_sql: str


def load_registry_statements(client: Any, registry: DatatypeRegistry) -> RegistryStatements:
    """The statements the export table holds for ``registry`` -- or a refusal.

    A missing field row or a version other than the code's means the export asset has
    not run since the registry changed; running the old statements would stamp rows
    with the old version and re-select them on every scan."""
    rows = client.execute(build_registry_statements_sql(registry))
    by_field = {str(row[0]): (str(row[1]), str(row[3])) for row in rows}
    expected = [*field_names(registry), PROJECTION_FIELD]
    missing = [name for name in expected if name not in by_field]
    if missing:
        raise ValueError(
            f"{SE_COMPANY_FIELD_REGISTRY} has no row for {missing}: materialize {REGISTRY_ASSET} first")
    stale = [name for name in expected if by_field[name][1] != registry.version]
    if stale:
        raise ValueError(
            f"{SE_COMPANY_FIELD_REGISTRY} is at {by_field[stale[0]][1]!r} for {stale} but the code is at "
            f"{registry.version!r}: materialize {REGISTRY_ASSET} first")
    return RegistryStatements(
        registry_version=registry.version,
        resolve_sql={name: by_field[name][0] for name in field_names(registry)},
        projection_sql=by_field[PROJECTION_FIELD][0])


SCOPE_SQL = "({all_companies:UInt8} = 1 OR company_id IN {company_ids:Array(String)})"
FINAL_SCOPE_SQL = "({all_companies:UInt8} = 1 OR final.company_id IN {company_ids:Array(String)})"
RESOLVED_SCOPE_SQL = "({all_companies:UInt8} = 1 OR resolved.company_id IN {company_ids:Array(String)})"
RESOLVE_ALL_SQL = ("({resolve_all:UInt8} = 1 AND ifNull(published.resolved_at, " + EPOCH_SQL
                   + ") < parseDateTime64BestEffort({resolve_all_before:String}, 3, 'UTC'))")


def build_changed_companies_sql(registry: DatatypeRegistry) -> str:
    """Companies to resolve again: never published, a candidate extracted after the
    published resolution, a decision created after it, or resolved rows stamped with a
    registry/policy version the registry table no longer carries (spec 8.4).

    The old info.py scan with ``artifacts`` replaced by ``candidates`` (``max(extracted_at)``
    is the candidate table's version column, so no FINAL), the ``ledger`` CTE unchanged --
    its ``latest_correction_at`` alias is read back by name by the backoffice Pipeline
    page -- and a ``versions`` CTE comparing every resolved row with the registry
    table's current versions (FINAL: an older duplicate version would otherwise flag the
    company forever). Fields no longer exported are dropped by the INNER JOIN.

    ``has_register_name`` is spec 8.3's "SCB row mandatory" rule as a pre-filter: a
    company without a bolagsverket/scb legal-name candidate is neither resolved nor
    published, so it is not re-selected every week either.

    ``resolve_all`` and its cutoff, the keyset paging and the projected reason flags
    behave exactly as in info.py: one page per call, ``after_company_id`` resumes it,
    the reasons are the WHERE's own expressions spelled twice from one constant. Every
    ``{name:Type}`` is a server-side parameter bound from ``server_params``.
    """
    published_at = f"ifNull(published.resolved_at, {EPOCH_SQL})"
    register_sources = ", ".join(f"'{source}'" for source in REGISTER_NAME_SOURCES)
    reasons = ",\n    ".join((
        "ifNull(published.company_id, '') = '' AS never_published",
        f"candidates.latest_extracted_at > {published_at} AS new_candidates",
        f"ifNull(ledger.latest_correction_at, {EPOCH_SQL}) > {published_at} AS decision_pending",
        "ifNull(versions.version_changed, 0) = 1 AS version_changed",
    ))
    return f"""WITH current_registry AS (
    SELECT field,
        argMax(registry_version, version) AS registry_version,
        argMax(policy_version, version) AS policy_version
    FROM {SE_COMPANY_FIELD_REGISTRY}
    WHERE datatype = '{registry.datatype}' AND country = '{registry.country}' AND field != '{PROJECTION_FIELD}'
    GROUP BY field
),
candidates AS (
    SELECT company_id, max(extracted_at) AS latest_extracted_at,
        countIf(field = '{REGISTER_NAME_FIELD}' AND source IN ({register_sources})) > 0 AS has_register_name
    FROM {SE_COMPANY_FIELD_CANDIDATE}
    WHERE {SCOPE_SQL}
    GROUP BY company_id
),
ledger AS (
    SELECT company_id, max(created_at) AS latest_correction_at
    FROM {DATABASE}.{SE_COMPANY_INFO_FIELD_VALUE}
    WHERE {SCOPE_SQL}
    GROUP BY company_id
),
published AS (
    SELECT final.company_id AS company_id, final.resolved_at AS resolved_at
    FROM {SE_COMPANY_INFO} AS final FINAL
    WHERE {FINAL_SCOPE_SQL}
),
versions AS (
    SELECT resolved.company_id AS company_id,
        toUInt8(countIf(resolved.registry_version != current_registry.registry_version
                        OR resolved.policy_version != current_registry.policy_version) > 0) AS version_changed
    FROM {SE_COMPANY_FIELD} AS resolved FINAL
    INNER JOIN current_registry ON current_registry.field = resolved.field
    WHERE {RESOLVED_SCOPE_SQL}
    GROUP BY resolved.company_id
)
SELECT candidates.company_id AS company_id,
    {reasons}
FROM candidates
LEFT JOIN published ON published.company_id = candidates.company_id
LEFT JOIN ledger ON ledger.company_id = candidates.company_id
LEFT JOIN versions ON versions.company_id = candidates.company_id
WHERE candidates.has_register_name
  AND (
        ifNull(published.company_id, '') = ''
     OR {RESOLVE_ALL_SQL}
     OR candidates.latest_extracted_at > {published_at}
     OR ifNull(ledger.latest_correction_at, {EPOCH_SQL}) > {published_at}
     OR ifNull(versions.version_changed, 0) = 1
      )
  AND candidates.company_id > {{after_company_id:String}}
ORDER BY candidates.company_id
LIMIT {{page_size:UInt32}}"""


def build_batch_stats_sql() -> str:
    """Rows this run wrote for a batch, per field, source and decision flag. No FINAL:
    a company is resolved once per run, so this run's rows are unique per (company,
    field) already."""
    return f"""SELECT field, source, toUInt8(decision_id IS NOT NULL) AS from_decision, count() AS rows
FROM {SE_COMPANY_FIELD}
WHERE source_run_id = {{source_run_id:String}} AND company_id IN {{company_ids:Array(String)}}
GROUP BY field, source, from_decision
ORDER BY field, source, from_decision"""


@dataclass(frozen=True)
class FieldStats:
    rows: int
    from_decision: int
    per_source: dict[str, int]
    no_row: int


class _FieldCounters:
    """Accumulates build_batch_stats_sql rows across batches."""

    def __init__(self) -> None:
        self.rows: dict[str, int] = defaultdict(int)
        self.from_decision: dict[str, int] = defaultdict(int)
        self.per_source: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self.companies = 0

    def add(self, rows: Sequence[Sequence[Any]], *, companies: int) -> None:
        self.companies += companies
        for field_name, source, from_decision, count in rows:
            self.rows[str(field_name)] += int(count)
            self.per_source[str(field_name)][str(source)] += int(count)
            if int(from_decision):
                self.from_decision[str(field_name)] += int(count)

    def stats(self, fields: Sequence[str]) -> dict[str, FieldStats]:
        return {
            name: FieldStats(rows=self.rows[name], from_decision=self.from_decision[name],
                             per_source=dict(self.per_source[name]), no_row=self.companies - self.rows[name])
            for name in fields
        }


@dataclass
class ResolveSummary:
    companies_selected: int = 0
    companies_published: int = 0
    per_field: dict[str, FieldStats] = dc_field(default_factory=dict)
    per_reason: dict[str, int] = dc_field(default_factory=lambda: dict.fromkeys(SELECTION_REASONS, 0))
    stopped_at_cap: bool = False
    preview: bool = False
    registry_version: str = ""
    source_run_id: str = ""
    company_scope: tuple[str, ...] = ()

    def metadata(self) -> dict[str, Any]:
        """The MaterializeResult metadata: flat counters plus the per-field breakdown."""
        return {
            "companies_selected": self.companies_selected,
            "companies_published": self.companies_published,
            **self.per_reason,
            "stopped_at_cap": self.stopped_at_cap,
            "preview": self.preview,
            "registry_version": self.registry_version,
            "source_run_id": self.source_run_id,
            "company_scope": list(self.company_scope),
            "per_field": dg.MetadataValue.json({name: asdict(stats) for name, stats in self.per_field.items()}),
        }


def _resolve_batch(client: Any, statements: RegistryStatements, companies: Sequence[str], fields: Sequence[str],
                   counters: _FieldCounters, *, source_run_id: str, resolved_at: datetime) -> int:
    """Every field's statement in registry order, then the projection through the stage,
    then this run's rows counted. Returns the companies the projection published."""
    for name in fields:
        client.execute(statements.resolve_sql[name], server_params(
            company_ids=companies, field=name, source_run_id=source_run_id, resolved_at=resolved_at))
    header = split_insert_header(statements.projection_sql)
    if header.table != SE_COMPANY_INFO:
        raise ValueError(f"The projection statement targets {header.table}, not {SE_COMPANY_INFO}")
    # new_versions_only stays off: the wide table is keyed by company_id and a new
    # version per resolution is the point -- ReplacingMergeTree(resolved_at) keeps the newest.
    # publish_with_stage qualifies the bare table name itself (every caller passes bare).
    counts = publish_with_stage(
        client=client, target=SE_COMPANY_INFO.split(".")[-1], insert_columns=header.columns, select_sql=header.body,
        select_parameters=server_params(company_ids=companies), invalid_condition=WIDE_INVALID_CONDITION,
        new_versions_only=False)
    rows = client.execute(build_batch_stats_sql(), server_params(company_ids=companies, source_run_id=source_run_id))
    counters.add(rows, companies=len(companies))
    return counts.inserted


def materialize_se_company_fields(context: Any, client: Any, config: SECompanyFieldResolveConfig, *,
                                  registry: DatatypeRegistry, now: datetime) -> ResolveSummary:
    """Resolve the selected companies -- or, with ``execute`` false, only say which.

    ``context`` supplies ``run_id`` (the rows' source_run_id) and ``log.info``. ``client``
    is the one server-side-params client for the whole run (open_resolve_client). A
    preview runs the registry check and the scan exactly as a real run (every chunk,
    every page) and nothing else.
    """
    scope = normalized_se_company_ids(config.company_ids)
    unknown = sorted(set(config.fields) - set(field_names(registry)))
    if unknown:
        raise ValueError(f"Not registry fields: {unknown}")
    selected_fields = [name for name in field_names(registry) if not config.fields or name in config.fields]
    statements = load_registry_statements(client, registry)
    # One cutoff for the whole run, always bound (the predicate parses it whether or not
    # resolve_all is on); None -> the run's own instant, so nothing this run publishes
    # can be selected again by a later page of the same run.
    cutoff = (config.resolve_all_before or "").strip() or clickhouse_stamp(now)
    summary = ResolveSummary(preview=not config.execute, registry_version=statements.registry_version,
                             source_run_id=str(context.run_id), company_scope=scope)
    chunks = [tuple(scope[start : start + config.company_batch_size])
              for start in range(0, len(scope), config.company_batch_size)] or [()]
    counters = _FieldCounters()
    scan_sql = build_changed_companies_sql(registry)
    for chunk in chunks:
        if summary.stopped_at_cap:
            break
        after_company_id = ""
        while True:
            remaining = (config.max_companies - summary.companies_selected
                         if config.max_companies is not None else config.company_batch_size)
            if remaining <= 0:
                # Only reachable after a FULL page: the cap stopped us, not exhaustion.
                summary.stopped_at_cap = True
                break
            page_size = min(config.company_batch_size, remaining)
            page = client.execute(scan_sql, server_params(
                company_ids=chunk, all_companies=int(not chunk), resolve_all=int(config.resolve_all),
                resolve_all_before=cutoff, after_company_id=after_company_id, page_size=page_size))
            companies = [str(row[0]) for row in page]
            if not companies:
                break
            after_company_id = companies[-1]
            for row in page:
                for offset, reason in enumerate(SELECTION_REASONS, start=1):
                    if row[offset]:
                        summary.per_reason[reason] += 1
            summary.companies_selected += len(companies)
            if config.execute:
                published = _resolve_batch(client, statements, companies, selected_fields, counters,
                                           source_run_id=str(context.run_id), resolved_at=now)
                summary.companies_published += published
                context.log.info("se_company_field batch: companies=%s published=%s", len(companies), published)
            if len(companies) < page_size:
                break  # a short page means the scan is exhausted
    if summary.stopped_at_cap:
        context.log.info("se_company_field stopped at the max_companies cap (%s): changed companies may "
                         "remain, the next run resumes from the start of the scan", config.max_companies)
    summary.per_field = counters.stats(selected_fields) if config.execute else {}
    return summary


@dg.asset(
    name=RESOLVE_ASSET,
    deps=[dg.AssetKey(REGISTRY_ASSET), *(dg.AssetKey(name) for name in CANDIDATE_ASSETS)],
    group_name=GROUP_NAME,
    kinds={"clickhouse", "python"},
    metadata={"table": SE_COMPANY_FIELD, "wide_table": SE_COMPANY_INFO},
    description=("Resolves one value per company and registry field from the candidates and the reviewer "
                 "decisions with the exported resolve statements, then re-pivots se_company_info. A UI "
                 "materialization without execute=true is a preview that writes nothing."),
)
def se_company_field_resolved_clickhouse(context: dg.AssetExecutionContext, config: SECompanyFieldResolveConfig,
                                         clickhouse: ClickhouseResource) -> dg.MaterializeResult:
    """changed companies -> per-field resolve statements -> wide projection -> counts."""
    # The table check binds its own %(name)s parameters client-side, so it runs on the
    # resource's ordinary connection, BEFORE the server-side client is opened.
    # assert_clickhouse_tables_exist matches system.tables.name (bare), so the qualified
    # constants are split back to their bare table name for this one call.
    assert_clickhouse_tables_exist(clickhouse, database=DATABASE, tables=(
        SE_COMPANY_FIELD_REGISTRY.split(".")[-1], SE_COMPANY_FIELD_CANDIDATE.split(".")[-1],
        SE_COMPANY_FIELD.split(".")[-1], SE_COMPANY_INFO.split(".")[-1],
        SE_COMPANY_INFO_FIELD_VALUE.split(".")[-1]))
    with open_resolve_client(clickhouse) as client:
        summary = materialize_se_company_fields(context, client, config, registry=INFO_REGISTRY,
                                                now=datetime.now(UTC))
    return dg.MaterializeResult(metadata={**summary.metadata(), "table": SE_COMPANY_FIELD})


defs = dg.Definitions(assets=[se_company_field_resolved_clickhouse])
