"""The one-time import: what it adopts, what it refuses, and what the read rule then serves.

Three layers, the same shape Task 5's derivation test uses:

1. String tests pin the selection's tables, its two predicates and the columns the write
   stamps -- the things a reviewer must be able to read off the constant.
2. Fake-client tests drive the REAL asset body: the gate (a bare Materialize writes
   nothing), the two refusals, and the parameters bound to the insert.
3. A `clickhouse-local` harness executes the SQL against the migrations' own DDL on the
   deployed ClickHouse version. The selection is a three-way join with a joined HAVING,
   and a substring test cannot tell a correct join from one that adopts an identity the
   resolver already geocoded.

Layer 3 runs under BOTH `join_use_nulls` settings, like every other harness in this plan.
A miss reads as the column's type default under 0 and as NULL under 1, so a rule that
adopts the right identities under one setting could adopt a different set under the other
-- and this import is a one-shot that writes a permanent store, so there is no second
chance to notice.
"""

import re
import subprocess
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterator

import dagster as dg
import pytest

from dagster_v3.defs.sweden_company import shared_addresses
from dagster_v3.defs.sweden_company.address_geocoding import (
    CLICKHOUSE_RESULTS_EXPORT_COLUMNS,
    QUALIFIED_CLICKHOUSE_RESULTS_TABLE,
)
from dagster_v3.defs.sweden_company.address_geocoding_assets import (
    SwedenGeocodeLegacyAdoptionConfig,
    epoch_milliseconds,
    sweden_address_geocode_legacy_adoption_clickhouse,
)
from dagster_v3.defs.sweden_company.geocode_legacy_adoption import (
    ADOPTION_CANDIDATES_SQL,
    ADOPTION_DISAGREEMENT_SQL,
    ADOPTION_INSERT_SQL,
    ADOPTION_SAMPLE_SQL,
)
from dagster_v3.defs.sweden_company.geocode_store import (
    LEGACY_ADOPTED_MATCH_METHOD,
    LEGACY_ADOPTED_POLICY_VERSION,
    QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE,
    STORE_COLUMNS,
    build_current_geocodes_sql,
    build_current_resolver_geocodes_sql,
)
from tests.test_se_company_person_clickhouse_local import (
    _clickhouse_local_command,
    _literal,
    _render,
)

pytestmark = pytest.mark.integration

STORE = QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE
LEGACY = QUALIFIED_CLICKHOUSE_RESULTS_TABLE
LINKS = shared_addresses.QUALIFIED_CLICKHOUSE_COMPANY_ADDRESS_LINKS_TABLE

POLICY = "se-address-resolution-policy-v5"
LEGACY_MD5 = "md5-legacy-snapshot"
WEEK_1_MD5 = "md5-week-1"
T_RESOLVER = datetime(2026, 8, 20, 3, tzinfo=UTC)
T_IMPORT = datetime(2026, 8, 24, 12, tzinfo=UTC)
T_LATER = datetime(2026, 8, 28, 3, tzinfo=UTC)
IMPORT_RUN_ID = "adoption-run-1"
SAMPLE_SIZE = 20

# One identity per scenario, one 64-hex id each.
ADOPTABLE = "1" * 64  # resolver ambiguous, one legacy exact
ALREADY_GEOCODED = "2" * 64  # resolver matched_exact -- must NOT be adopted
DISAGREEING = "3" * 64  # two legacy rows, two coordinates -- must NOT be adopted
NOT_EXACT = "4" * 64  # legacy matched_area -- must NOT be adopted
SHARED = "5" * 64  # three companies, one agreed coordinate -- adopted ONCE
WEAK_EXACT = "6" * 64  # legacy exact BELOW confidence 1.0 -- must NOT be adopted
NO_COORDINATE = "7" * 64  # legacy exact with no coordinate -- must NOT be adopted


def test_the_selection_names_its_three_tables_and_its_two_predicates() -> None:
    for table in (LEGACY, LINKS, STORE):
        assert table in ADOPTION_CANDIDATES_SQL
    assert "match_status = 'matched_exact'" in ADOPTION_CANDIDATES_SQL
    assert "match_confidence = 1.0" in ADOPTION_CANDIDATES_SQL
    assert (
        "uniqExact(tuple(legacy.latitude, legacy.longitude)) = 1"
        in ADOPTION_CANDIDATES_SQL
    )
    # It is joined through LINKS, not members -- members carries no address_id at all.
    assert "se_company_address_members_current" not in ADOPTION_CANDIDATES_SQL


def test_the_selection_reads_the_resolver_family_not_the_served_answer() -> None:
    """The one predicate that decides whether the import is a stopgap or an overwrite.

    An identity the resolver has already geocoded is never adopted, and the outcome the
    predicate reads is the RESOLVER's -- geocode_store's stage 1 restricted to the
    resolver family -- so a second import cannot chase its own adopted rows.
    """
    resolver_read = build_current_resolver_geocodes_sql(
        columns=("address_id", "match_status")
    )
    assert resolver_read in ADOPTION_CANDIDATES_SQL
    assert resolver_read in ADOPTION_INSERT_SQL
    assert "resolver.match_status NOT IN (" in ADOPTION_CANDIDATES_SQL
    for status in (
        "matched_exact",
        "matched_corrected",
        "matched_site",
        "matched_area",
        "matched_street",
    ):
        assert f"'{status}'" in ADOPTION_CANDIDATES_SQL


def test_the_refusal_count_is_the_selection_with_the_opposite_having() -> None:
    """The refused number must be explainable by the same rule that produced the adopted
    one -- a differently-shaped second query would count a different population."""
    assert (
        "uniqExact(tuple(legacy.latitude, legacy.longitude)) > 1"
        in ADOPTION_DISAGREEMENT_SQL
    )
    assert "= 1" not in ADOPTION_DISAGREEMENT_SQL.rsplit("HAVING", 1)[1]
    for fragment in (
        "ON links.company_id = legacy.company_id",
        "AND links.canonical_address_key = legacy.address_key",
        "legacy.match_status = 'matched_exact'",
    ):
        assert fragment in ADOPTION_DISAGREEMENT_SQL


def test_the_insert_stamps_the_adoption_version_and_writes_every_store_column() -> None:
    assert ADOPTION_INSERT_SQL.startswith(
        f"INSERT INTO {STORE} (" + ", ".join(STORE_COLUMNS) + ")"
    )
    assert f"'{LEGACY_ADOPTED_POLICY_VERSION}' AS policy_version" in ADOPTION_INSERT_SQL
    assert f"'{LEGACY_ADOPTED_MATCH_METHOD}' AS match_method" in ADOPTION_INSERT_SQL
    assert "%(imported_at)s" in ADOPTION_INSERT_SQL
    assert "%(geocode_run_id)s AS geocode_run_id" in ADOPTION_INSERT_SQL
    # Every store column is projected, in the order the table declares them. The append
    # binds STORE_COLUMNS positionally, so a projection out of order would write each
    # value into its neighbour's column without raising.
    select_list = ADOPTION_INSERT_SQL.split("\nFROM ", 1)[0]
    assert re.findall(r"\bAS (\w+),?$", select_list, re.MULTILINE) == list(
        STORE_COLUMNS
    )
    # The adopted row carries the LEGACY provenance, not the resolver's.
    for column in (
        "source_url",
        "source_object_key",
        "source_md5",
        "source_snapshot_at",
        "source_retrieved_at",
    ):
        assert f"any(legacy.{column})" in ADOPTION_INSERT_SQL


def test_the_import_instant_is_bound_in_exact_milliseconds() -> None:
    """`clickhouse_driver` renders a bound datetime through `escape_datetime`: the
    sub-second part is dropped and the literal is written in the SERVER's timezone, not
    the column's. Against `matched_at DateTime64(3, 'UTC')` that literal parses as UTC, so
    on a non-UTC server every adopted row would be stamped hours away from the instant the
    import actually ran -- in a permanent store, written once. The tick count is exact and
    timezone-free, which is the same fix `build_store_append_regression_sql` carries.
    """
    assert (
        "fromUnixTimestamp64Milli(toInt64(%(imported_at)s), 'UTC') AS matched_at"
        in ADOPTION_INSERT_SQL
    )
    assert "toDateTime64(%(imported_at)s" not in ADOPTION_INSERT_SQL


def test_the_sample_reads_only_adopted_rows() -> None:
    assert f"store.policy_version = '{LEGACY_ADOPTED_POLICY_VERSION}'" in (
        ADOPTION_SAMPLE_SQL
    )
    assert "LIMIT %(sample_size)s" in ADOPTION_SAMPLE_SQL
    assert ADOPTION_SAMPLE_SQL.startswith("SELECT")


class _FakeClickhouseClient:
    """Records every statement; answers the asset's SELECTs by shape."""

    def __init__(
        self,
        *,
        existing_tables: set[str] | None = None,
        candidates: tuple[int, int, int] = (19413, 21000, 20500),
        disagreeing: int = 87,
        adopted: tuple[int, int] = (19413, 19413),
        total_adopted: int = 19413,
    ) -> None:
        self.executed: list[tuple[str, Any]] = []
        self.existing_tables = (
            {
                "se_address_geocodes",
                "se_company_address_geocode_results",
                "se_company_address_links_current",
            }
            if existing_tables is None
            else existing_tables
        )
        self.candidates = candidates
        self.disagreeing = disagreeing
        self.adopted = adopted
        self.total_adopted = total_adopted

    def execute(self, sql: str, params: Any = None) -> list[tuple]:
        self.executed.append((sql, params))
        if "system.tables" in sql:
            return [
                (table,) for table in params["tables"] if table in self.existing_tables
            ]
        if sql.startswith("INSERT INTO"):
            return []
        if "sum(legacy_rows)" in sql:
            return [self.candidates]
        if sql == ADOPTION_DISAGREEMENT_SQL:
            return [(self.disagreeing,)]
        if "geocode_run_id = %(geocode_run_id)s" in sql:
            return [self.adopted]
        if "uniqExact(address_id)" in sql:
            return [(self.total_adopted,)]
        if sql == ADOPTION_SAMPLE_SQL:
            return [(ADOPTABLE, "matched_exact", LEGACY_ADOPTED_MATCH_METHOD)]
        return []

    @property
    def statements(self) -> list[str]:
        return [sql for sql, _ in self.executed if "system.tables" not in sql]


class _FakeResource:
    def __init__(self, connection: Any) -> None:
        self._connection = connection

    @contextmanager
    def get_connection(self) -> Iterator[Any]:
        yield self._connection


def _run_import(
    client: _FakeClickhouseClient, *, execute: bool = False
) -> dg.MaterializeResult:
    asset = sweden_address_geocode_legacy_adoption_clickhouse
    return asset.node_def.compute_fn.decorated_fn(
        dg.build_asset_context(),
        SwedenGeocodeLegacyAdoptionConfig(execute=execute),
        _FakeResource(client),
    )


def test_a_bare_materialize_measures_and_writes_nothing() -> None:
    """The gate. This asset writes a permanent store once; a stray Materialize click must
    report what a real run would adopt and stop there."""
    assert SwedenGeocodeLegacyAdoptionConfig().execute is False
    client = _FakeClickhouseClient()

    result = _run_import(client)

    assert not any(
        statement.startswith("INSERT INTO") for statement in client.statements
    )
    assert result.metadata["preview"] is True
    assert result.metadata["adoptable_identities"] == 19413
    assert result.metadata["contributing_legacy_rows"] == 21000
    assert result.metadata["contributing_companies"] == 20500
    assert result.metadata["refused_disagreeing_identities"] == 87


def test_the_gated_run_inserts_once_and_reports_what_it_wrote() -> None:
    client = _FakeClickhouseClient()

    result = _run_import(client, execute=True)

    inserts = [
        (sql, params)
        for sql, params in client.executed
        if sql.startswith("INSERT INTO")
    ]
    assert len(inserts) == 1
    [(insert_sql, params)] = inserts
    assert insert_sql == ADOPTION_INSERT_SQL
    assert set(params) == {"geocode_run_id", "imported_at"}
    # The instant is bound as an integer tick count, never as a datetime.
    assert isinstance(params["imported_at"], int)
    assert result.metadata["preview"] is False
    assert result.metadata["adopted_identities"] == 19413
    # The instant the metadata names is exactly the instant the store holds -- not one
    # floor-division away from it.
    reported = datetime.fromisoformat(result.metadata["imported_at"])
    assert result.metadata["imported_at"].endswith("+00:00")
    assert reported.microsecond % 1000 == 0
    assert epoch_milliseconds(reported) == params["imported_at"]


def test_the_gated_run_refuses_an_import_that_would_adopt_nothing() -> None:
    """Zero adoptable identities means the join found nothing -- the legacy table already
    dropped, the store never backfilled, the wrong cluster. Writing nothing silently would
    look exactly like a successful one-shot."""
    client = _FakeClickhouseClient(candidates=(0, 0, 0))

    with pytest.raises(ValueError, match="adoptable"):
        _run_import(client, execute=True)

    assert not any(
        statement.startswith("INSERT INTO") for statement in client.statements
    )


def test_the_gated_run_refuses_a_write_that_broke_the_identity_grain() -> None:
    """The grain change is the whole risk: the legacy table is keyed per company, the
    store per identity. A GROUP BY that stopped collapsing would write one row per
    company-address and every downstream join would fan out."""
    client = _FakeClickhouseClient(adopted=(19500, 19413))

    with pytest.raises(ValueError, match="more than one row per address identity"):
        _run_import(client, execute=True)


def test_the_asset_asserts_its_three_clickhouse_tables_exist() -> None:
    client = _FakeClickhouseClient(existing_tables={"se_address_geocodes"})

    with pytest.raises(ValueError, match="se_company_address_geocode_results"):
        _run_import(client)


def test_the_import_is_a_one_shot_in_its_own_job_and_no_schedule() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    job = repo.get_job("sweden_address_geocode_legacy_adoption_job")

    assert {key.path[-1] for key in job.asset_layer.executable_asset_keys} == {
        "sweden_address_geocode_legacy_adoption_clickhouse"
    }
    for other in repo.get_all_jobs():
        # Dagster's own implicit job holds every asset in the repository by definition.
        if other.name == job.name or other.name.startswith("__"):
            continue
        assert "sweden_address_geocode_legacy_adoption_clickhouse" not in {
            key.path[-1] for key in other.asset_layer.executable_asset_keys
        }, other.name
    for schedule in repo.schedule_defs:
        assert schedule.job.name != job.name


MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
# 000271 creates the legacy per-company results table, 000272 and 000277 add the three
# coordinate columns the import carries across, 000274 creates the links table that holds
# the only canonical_address_key -> address_id map, 000317 creates the store. 000274 and
# 000277 also touch tables this import never reads, so statements are filtered by target
# table rather than applied wholesale.
MIGRATIONS = (
    "000271_corpscout_se_company_address_geocode_results.up.sql",
    "000272_corpscout_se_company_address_city_fallback.up.sql",
    "000274_corpscout_se_shared_addresses.up.sql",
    "000277_corpscout_se_address_geocode_spread.up.sql",
    "000317_corpscout_se_address_geocodes_store.up.sql",
)
NEEDED_TABLES = frozenset(
    {
        "se_company_address_geocode_results",
        "se_company_address_links_current",
        "se_address_geocodes",
    }
)
_TABLE_RE = re.compile(
    r"^(?:CREATE TABLE(?: IF NOT EXISTS)?|ALTER TABLE)\s+corpscout\.(\w+)",
    re.IGNORECASE,
)

_STORE_DEFAULTS: dict[str, str] = {
    "address_id": "",
    "policy_version": f"'{POLICY}'",
    "reference_md5": f"'{LEGACY_MD5}'",
    "address_identity_run_id": "'identity-run-1'",
    "normalized_match_key": "'se|storgatan|11122|stockholm'",
    "match_status": "'ambiguous'",
    "candidate_count": "3",
    "candidate_record_ids": "[]",
    "candidate_record_urls": "[]",
    "match_method": "''",
    "match_confidence": "0.0",
    "latitude": "NULL",
    "longitude": "NULL",
    "geocode_provider": "'openstreetmap'",
    "geocode_precision": "''",
    "coordinate_method": "NULL",
    "coordinate_locality": "NULL",
    "coordinate_supporting_point_count": "0",
    "coordinate_spread_meters": "NULL",
    "source_record_id": "NULL",
    "source_record_url": "NULL",
    "source_url": "'https://download.geofabrik.de/sweden-latest.osm.pbf'",
    "source_object_key": "'osm/sweden-latest.osm.pbf'",
    "source_md5": f"'{LEGACY_MD5}'",
    "source_snapshot_at": _literal(datetime(2026, 8, 18, tzinfo=UTC)),
    "source_retrieved_at": _literal(datetime(2026, 8, 18, 1, tzinfo=UTC)),
    "geocode_run_id": "'run-resolver-0'",
    "matched_at": _literal(T_RESOLVER),
}

_LEGACY_DEFAULTS: dict[str, str] = {
    "company_id": "",
    "address_key": "",
    "address_type": "'postal'",
    "address_source": "'bolagsverket'",
    "registry_source_record_uid": "'uid-1'",
    "street_address": "'Storgatan 1'",
    "postal_code": "'111 22'",
    "post_town": "'Stockholm'",
    "country_code": "'SE'",
    "normalized_match_key": "'se|storgatan 1|11122|stockholm'",
    "match_status": "'matched_exact'",
    "candidate_count": "1",
    "candidate_record_ids": "['osm/way/1']",
    "candidate_record_urls": "['https://www.openstreetmap.org/way/1']",
    "match_method": "'country_street_house_exact_unique'",
    "match_confidence": "1.0",
    "latitude": "59.33",
    "longitude": "18.06",
    "geocode_provider": "'openstreetmap'",
    "geocode_precision": "'building'",
    "coordinate_method": "'osm_record'",
    "coordinate_locality": "NULL",
    "coordinate_supporting_point_count": "1",
    "coordinate_spread_meters": "NULL",
    "source_record_id": "'osm/way/1'",
    "source_record_url": "'https://www.openstreetmap.org/way/1'",
    "source_url": "'https://download.geofabrik.de/sweden-latest.osm.pbf'",
    "source_object_key": "'osm/sweden-latest.osm.pbf'",
    "source_md5": f"'{LEGACY_MD5}'",
    "source_snapshot_at": _literal(datetime(2026, 8, 18, tzinfo=UTC)),
    "source_retrieved_at": _literal(datetime(2026, 8, 18, 1, tzinfo=UTC)),
    "source_run_id": "'run-legacy'",
    "matched_at": _literal(datetime(2026, 8, 19, tzinfo=UTC)),
}

_LINK_DEFAULTS: dict[str, str] = {
    "company_id": "",
    "address_id": "",
    "canonical_address_key": "",
    "address_types": "['postal']",
    "address_sources": "['bolagsverket']",
    "evidence_count": "1",
    "first_observed_at": _literal(datetime(2026, 8, 1, tzinfo=UTC)),
    "last_observed_at": _literal(datetime(2026, 8, 1, tzinfo=UTC)),
    "review_status": "'unreviewed'",
    "reviewed_at": "NULL",
    "reviewed_by": "''",
    "review_note": "''",
    "address_identity_run_id": "'identity-run-1'",
    "address_identity_built_at": _literal(datetime(2026, 8, 1, tzinfo=UTC)),
}


def _row(defaults: dict[str, str], columns: tuple[str, ...], **overrides: str) -> str:
    row = {**defaults, **overrides}
    assert set(row) == set(columns), set(row) ^ set(columns)
    return "(" + ", ".join(row[column] for column in columns) + ")"


def _store_row(address_id: str, **overrides: str) -> str:
    return _row(
        _STORE_DEFAULTS, STORE_COLUMNS, address_id=f"'{address_id}'", **overrides
    )


def _legacy_row(company_id: str, address_key: str, **overrides: str) -> str:
    return _row(
        _LEGACY_DEFAULTS,
        CLICKHOUSE_RESULTS_EXPORT_COLUMNS,
        company_id=f"'{company_id}'",
        address_key=f"'{address_key}'",
        **overrides,
    )


def _link_row(company_id: str, address_id: str, address_key: str) -> str:
    return _row(
        _LINK_DEFAULTS,
        shared_addresses.COMPANY_ADDRESS_LINK_COLUMNS,
        company_id=f"'{company_id}'",
        address_id=f"'{address_id}'",
        canonical_address_key=f"'{address_key}'",
    )


def _at(coordinates: tuple[float, float]) -> dict[str, str]:
    latitude, longitude = coordinates
    return {"latitude": str(latitude), "longitude": str(longitude)}


def _geocoded(
    status: str,
    coordinates: tuple[float, float],
    *,
    method: str = "country_street_house_exact_unique",
) -> dict[str, str]:
    precision = {
        "matched_exact": "'building'",
        "matched_street": "'street'",
    }[status]
    return {
        "match_status": f"'{status}'",
        "candidate_count": "1",
        "candidate_record_ids": "['osm/way/9']",
        "match_method": f"'{method}'",
        "match_confidence": "1.0",
        "geocode_precision": precision,
        "coordinate_method": "'osm_record'",
        **_at(coordinates),
    }


# (company_id, address_id, canonical_address_key), the order _link_row takes them in.
# Each identity's canonical key is its own letter; several companies share one identity.
_MEMBERSHIP = (
    ("5560000001", ADOPTABLE, "a" * 64),
    ("5560000002", ALREADY_GEOCODED, "b" * 64),
    ("5560000003", DISAGREEING, "c" * 64),
    ("5560000004", DISAGREEING, "c" * 64),
    ("5560000005", NOT_EXACT, "d" * 64),
    ("5560000006", SHARED, "e" * 64),
    ("5560000007", SHARED, "e" * 64),
    ("5560000008", SHARED, "e" * 64),
    ("5560000009", WEAK_EXACT, "f" * 64),
    ("5560000010", NO_COORDINATE, "0" * 64),
)


def _fixture_statements() -> list[str]:
    legacy_rows = [
        _legacy_row("5560000001", "a" * 64, **_at((59.33, 18.06))),
        # The resolver already answered for this one, so its legacy exact is irrelevant.
        _legacy_row("5560000002", "b" * 64, **_at((57.70, 11.97))),
        # Two companies at one identity, two different coordinates: not a decision.
        _legacy_row("5560000003", "c" * 64, **_at((55.60, 13.00))),
        _legacy_row("5560000004", "c" * 64, **_at((55.61, 13.01))),
        # Only matched_exact at confidence 1.0 is adopted.
        _legacy_row(
            "5560000005",
            "d" * 64,
            match_status="'matched_area'",
            geocode_precision="'area'",
            match_confidence="0.6",
            **_at((56.16, 15.59)),
        ),
        # Three companies, one identity, one agreed coordinate.
        *(
            _legacy_row(company, "e" * 64, **_at((58.41, 15.62)))
            for company in ("5560000006", "5560000007", "5560000008")
        ),
        # `matched_exact` is not enough on its own: the legacy matcher also emitted exact
        # matches BELOW full confidence, and only a 1.0 decision is trusted enough to
        # freeze into the store under a version of its own.
        _legacy_row(
            "5560000009",
            "f" * 64,
            match_confidence="0.98",
            **_at((59.85, 17.64)),
        ),
        # A geocoded status with no coordinate would violate the store's own
        # status/coordinate invariant the moment it landed.
        _legacy_row("5560000010", "0" * 64, latitude="NULL", longitude="NULL"),
    ]
    store_rows = [
        _store_row(ADOPTABLE),
        _store_row(ALREADY_GEOCODED, **_geocoded("matched_exact", (57.70, 11.97))),
        _store_row(DISAGREEING),
        _store_row(NOT_EXACT),
        _store_row(SHARED, match_status="'unmatched'", candidate_count="0"),
        _store_row(WEAK_EXACT),
        _store_row(NO_COORDINATE),
    ]
    return [
        f"INSERT INTO {LEGACY} ({', '.join(CLICKHOUSE_RESULTS_EXPORT_COLUMNS)}) VALUES\n"
        + ",\n".join(legacy_rows),
        f"INSERT INTO {LINKS} "
        f"({', '.join(shared_addresses.COMPANY_ADDRESS_LINK_COLUMNS)}) VALUES\n"
        + ",\n".join(_link_row(*membership) for membership in _MEMBERSHIP),
        f"INSERT INTO {STORE} ({', '.join(STORE_COLUMNS)}) VALUES\n"
        + ",\n".join(store_rows),
    ]


def _later_resolver_statement() -> str:
    """A resolver retry that answers for two identities an adopted exact was serving.

    ADOPTABLE gets a building match -- the ordinary way an adopted row stops being served.
    SHARED gets a `matched_street` from the postcode-conflict fallback, which is the L6
    interaction: the promotion's gate reads the RESOLVER's previous outcome, sees
    `unmatched`, and admits the fallback without ever knowing an adopted exact was being
    served. The demotion is by design and is what these two rows execute.
    """
    week_1 = {
        "reference_md5": f"'{WEEK_1_MD5}'",
        "source_md5": f"'{WEEK_1_MD5}'",
        "geocode_run_id": "'run-resolver-1'",
        "matched_at": _literal(T_LATER),
    }
    return f"INSERT INTO {STORE} ({', '.join(STORE_COLUMNS)}) VALUES\n" + ",\n".join(
        [
            _store_row(
                ADOPTABLE, **week_1, **_geocoded("matched_exact", (59.34, 18.07))
            ),
            _store_row(
                SHARED,
                **week_1,
                **_geocoded(
                    "matched_street",
                    (58.40, 15.60),
                    method="street_without_house_postcode_conflict",
                ),
            ),
        ]
    )


def _schema_statements() -> list[str]:
    statements: list[str] = []
    for name in MIGRATIONS:
        text = (MIGRATIONS_DIR / name).read_text(encoding="utf-8")
        for raw in text.split(";"):
            statement = "\n".join(
                line for line in raw.splitlines() if not line.strip().startswith("--")
            ).strip()
            if not statement:
                continue
            if statement.upper().startswith("CREATE DATABASE"):
                statements.append(statement)
                continue
            match = _TABLE_RE.match(statement)
            if match and match.group(1) in NEEDED_TABLES:
                statements.append(statement)
    return statements


def _marked(label: str, query: str) -> str:
    return f"SELECT '@@{label}';\n{query.rstrip().rstrip(';')} FORMAT TSV;"


_ADOPTED_SQL = f"""SELECT
    toString(address_id),
    match_status,
    match_method,
    toString(latitude),
    toString(longitude),
    geocode_precision,
    reference_md5,
    toString(matched_at),
    geocode_run_id
FROM {STORE}
WHERE policy_version = '{LEGACY_ADOPTED_POLICY_VERSION}'
ORDER BY address_id"""

_ADOPTED_GRAIN_SQL = f"""SELECT count(), uniqExact(address_id)
FROM {STORE} FINAL
WHERE policy_version = '{LEGACY_ADOPTED_POLICY_VERSION}'"""

_SERVED_SQL = f"""SELECT
    toString(served.address_id),
    served.policy_version,
    served.match_status
FROM (
{build_current_geocodes_sql(columns=("address_id", "policy_version", "match_status"))}
) AS served
ORDER BY served.address_id"""

_RESOLVER_VIEW_SQL = f"""SELECT
    toString(resolver.address_id),
    resolver.match_status
FROM (
{build_current_resolver_geocodes_sql(columns=("address_id", "match_status"))}
) AS resolver
ORDER BY resolver.address_id"""

_IMPORT_PARAMETERS = {
    "geocode_run_id": IMPORT_RUN_ID,
    "imported_at": epoch_milliseconds(T_IMPORT),
}


def _script(*, join_use_nulls: int) -> str:
    parts = [f"SET join_use_nulls = {join_use_nulls};"]
    parts.extend(f"{statement};" for statement in _schema_statements())
    parts.extend(f"{statement};" for statement in _fixture_statements())
    parts.append(_marked("candidates", ADOPTION_CANDIDATES_SQL))
    parts.append(_marked("disagreement", ADOPTION_DISAGREEMENT_SQL))
    parts.append(f"{_render(ADOPTION_INSERT_SQL, _IMPORT_PARAMETERS)};")
    parts.append(_marked("adopted", _ADOPTED_SQL))
    parts.append(
        _marked("sample", _render(ADOPTION_SAMPLE_SQL, {"sample_size": SAMPLE_SIZE}))
    )
    parts.append(_marked("resolver_view", _RESOLVER_VIEW_SQL))
    parts.append(_marked("served", _SERVED_SQL))
    # The same import again, same instant: a key-stable replace, not a second row.
    parts.append(f"{_render(ADOPTION_INSERT_SQL, _IMPORT_PARAMETERS)};")
    parts.append(_marked("grain_after_reimport", _ADOPTED_GRAIN_SQL))
    parts.append(f"{_later_resolver_statement()};")
    parts.append(_marked("served_after_resolver_success", _SERVED_SQL))
    parts.append(_marked("candidates_after_resolver_success", ADOPTION_CANDIDATES_SQL))
    return "\n".join(parts) + "\n"


@pytest.fixture(
    scope="module",
    params=(0, 1),
    ids=("join_use_nulls_off", "join_use_nulls_on"),
)
def sections(request: pytest.FixtureRequest) -> dict[str, list[list[str]]]:
    """Runs the fixture, the import and the read rule end to end in clickhouse-local."""
    command = _clickhouse_local_command()
    try:
        completed = subprocess.run(
            command,
            input=_script(join_use_nulls=request.param),
            capture_output=True,
            text=True,
            timeout=600,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:  # pragma: no cover - env
        pytest.skip(f"clickhouse-local is unusable here: {exc}")
    assert completed.returncode == 0, completed.stderr or completed.stdout
    result: dict[str, list[list[str]]] = {}
    current = ""
    for line in completed.stdout.splitlines():
        if line.startswith("@@"):
            current = line[2:]
            result[current] = []
        elif current and line.strip():
            result[current].append(line.split("\t"))
    return result


def test_only_the_trapped_decisions_are_adopted(sections) -> None:
    adopted = {row[0] for row in sections["adopted"]}
    assert adopted == {ADOPTABLE, SHARED}
    assert ALREADY_GEOCODED not in adopted, "the resolver already answered for this one"
    assert DISAGREEING not in adopted, "two legacy coordinates is not a decision"
    assert NOT_EXACT not in adopted, "only matched_exact at confidence 1.0 is adopted"
    assert WEAK_EXACT not in adopted, "an exact match below 1.0 is not a 1.0 decision"
    assert NO_COORDINATE not in adopted, "there is no coordinate here to adopt"


def test_a_shared_identity_is_adopted_exactly_once(sections) -> None:
    """Three companies, one address identity, one store row -- the grain change the whole
    import has to get right."""
    assert [row[0] for row in sections["adopted"]].count(SHARED) == 1
    candidates = {row[0]: (row[1], row[2]) for row in sections["candidates"]}
    assert candidates[SHARED] == ("3", "3")
    assert candidates[ADOPTABLE] == ("1", "1")
    assert set(candidates) == {ADOPTABLE, SHARED}


def test_the_refused_disagreement_is_counted_and_not_adopted(sections) -> None:
    assert sections["disagreement"] == [["1"]]


def test_the_adopted_row_carries_the_legacy_decision_and_its_own_version(
    sections,
) -> None:
    """Attributable forever: the version and the method say where the coordinate came
    from, the coordinate and precision are the legacy matcher's own, and the reference
    identity is the OSM snapshot that matcher ran against."""
    rows = {row[0]: row for row in sections["adopted"]}
    _, status, method, latitude, longitude, precision, reference, stamp, run = rows[
        ADOPTABLE
    ]
    assert status == "matched_exact"
    assert method == LEGACY_ADOPTED_MATCH_METHOD
    assert (float(latitude), float(longitude)) == (59.33, 18.06)
    assert precision == "building"
    assert reference == LEGACY_MD5
    assert run == IMPORT_RUN_ID
    assert stamp.startswith("2026-08-24 12:00:00")


def test_the_controller_sample_reads_the_adopted_rows(sections) -> None:
    assert {row[0] for row in sections["sample"]} == {ADOPTABLE, SHARED}


def test_an_adopted_identity_still_presents_its_resolver_outcome(sections) -> None:
    """L6, executed. The demand scan and the promotion's postcode-conflict gate both read
    the RESOLVER family, so an identity served by an adopted exact still shows its
    `ambiguous`/`unmatched` there -- which is what keeps it in the retry pool on the next
    reference bump instead of looking permanently settled."""
    resolver = {row[0]: row[1] for row in sections["resolver_view"]}
    assert resolver[ADOPTABLE] == "ambiguous"
    assert resolver[SHARED] == "unmatched"
    assert LEGACY_ADOPTED_MATCH_METHOD not in resolver.values()


def test_the_read_rule_serves_the_adopted_coordinate(sections) -> None:
    served = {row[0]: (row[1], row[2]) for row in sections["served"]}
    assert served[ADOPTABLE] == (LEGACY_ADOPTED_POLICY_VERSION, "matched_exact")
    assert served[SHARED] == (LEGACY_ADOPTED_POLICY_VERSION, "matched_exact")
    # ... and the resolver's own answer still wins where it had one.
    assert served[ALREADY_GEOCODED][0] == POLICY
    # Nothing was adopted for these, so they still read as the resolver left them.
    for identity in (DISAGREEING, NOT_EXACT, WEAK_EXACT, NO_COORDINATE):
        assert served[identity] == (POLICY, "ambiguous")


def test_a_second_import_replaces_rather_than_duplicates(sections) -> None:
    """The store is keyed (address_id, policy_version, reference_md5) and versioned by
    matched_at. Re-running the same import at the same instant must land the same rows,
    not a second copy of them."""
    assert sections["grain_after_reimport"] == [["2", "2"]]


def test_a_later_resolver_success_outranks_the_adopted_row(sections) -> None:
    """Spec 4.4's reversibility claim, executed: nothing is merged, the adopted row simply
    stops being the newest servable answer.

    SHARED is the L6 case -- the resolver's postcode-conflict street fallback, admitted by
    a gate that only ever saw `unmatched`, takes over from an adopted building exact. The
    adopted row is still in the store, still attributable, still one policy_version away.
    """
    served = {
        row[0]: (row[1], row[2]) for row in sections["served_after_resolver_success"]
    }
    assert served[ADOPTABLE] == (POLICY, "matched_exact")
    assert served[SHARED] == (POLICY, "matched_street")
    assert {row[0] for row in sections["adopted"]} == {ADOPTABLE, SHARED}


def test_a_resolver_answer_takes_the_identity_out_of_the_adoption_pool(
    sections,
) -> None:
    """A re-run after the resolver answered adopts nothing for those identities -- the
    selection reads the resolver family, so the import cannot chase its own rows."""
    assert sections["candidates_after_resolver_success"] == []
