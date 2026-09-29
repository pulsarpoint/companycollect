"""The IP search table (migration 000470) against disposable ClickHouse and PostgreSQL.

One row per inventory IP with its current enrichment, the same values ip_enrichment_current
picks, projections that answer location filters and picker counts, the queue selection
over it, the fire-and-forget refresh request and the freshness check.
"""

import logging
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import dagster as dg
import pytest

from dagster_v3.defs.ip_enrichment import search
from dagster_v3.defs.ip_enrichment.input import INPUT_RELATION
from tests.test_ip_enrichment_input import materialize, scope, server as server
from tests.test_processing_store import (
    processing_postgres_url as processing_postgres_url,
    store as store,
)

MIGRATIONS = Path(__file__).resolve().parents[3] / "clickhouse/migrations"
MIGRATION = "000470_corpscout_ip_enrichment_search"
INVENTORY_DDL = """CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_ip_addresses
(
    bucket UInt16,
    ip String,
    ip_version UInt8,
    first_seen SimpleAggregateFunction(min, DateTime64(3, 'UTC')),
    last_seen SimpleAggregateFunction(max, DateTime64(3, 'UTC'))
)
ENGINE = AggregatingMergeTree
ORDER BY (bucket, ip_version, ip)"""
RESULT_COLUMNS = (
    "ip, result_id, task_id, execution_id, input_id, source_run_id, processor_version, "
    "completed_at, ip_scope, city_lookup_status, city_checked_at, country_iso_code, "
    "country_name, subdivision_iso_codes, subdivision_names, city_name, asn_lookup_status, "
    "asn_checked_at, asn, asn_organization, rdap_lookup_status, rdap_checked_at, "
    "rdap_matched_cidr, rdap_rir, rdap_name, rdap_registrant_names"
)
# Filler rows so the planner has real granules to choose between (country, ASN and IP
# ranges never collide with the named addresses below).
FILLER_ROWS = 60_000
FILLER_IP = "IPv4NumToString(toUInt32(167772160 + number))"


def migration_statements(name: str) -> list[str]:
    text = (MIGRATIONS / f"{name}.up.sql").read_text(encoding="utf-8")
    return [statement for statement in text.split(";") if statement.strip()]


@pytest.fixture(scope="module")
def search_server(server):
    client, resource = server
    client.execute(INVENTORY_DDL)
    for statement in migration_statements(MIGRATION):
        client.execute(statement)
    # Read before the first refresh: an EMPTY view has never succeeded.
    initial_state = client.execute(search.REFRESH_STATE_SQL)
    return client, resource, initial_state


def result_row(ip, completed_at, *, city, asn, rdap, checked_at=None):
    """One result row. city/asn/rdap are (status, payload tuple or None)."""
    city_status, city_payload = city
    asn_status, asn_payload = asn
    rdap_status, rdap_payload = rdap
    country, country_name, codes, names, city_name = city_payload or (
        None,
        None,
        [],
        [],
        None,
    )
    asn_number, organization = asn_payload or (None, None)
    cidr, rir, rdap_name, registrants = rdap_payload or (None, None, None, [])
    return (
        f"('{ip}', generateUUIDv4(), generateUUIDv4(), generateUUIDv4(), '{ip}', 'run', "
        f"'test', toDateTime64('{completed_at}', 6, 'UTC'), 'global', "
        f"'{city_status}', {literal(checked_at)}, {literal(country)}, {literal(country_name)}, "
        f"{codes!r}, {names!r}, {literal(city_name)}, '{asn_status}', {literal(checked_at)}, "
        f"{literal(asn_number)}, {literal(organization)}, '{rdap_status}', {literal(checked_at)}, "
        f"{literal(cidr)}, {literal(rir)}, {literal(rdap_name)}, {registrants!r})"
    )


def literal(value) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, int):
        return str(value)
    return "'" + str(value).replace("'", "\\'") + "'"


GOOGLE_CITY = (
    "found",
    ("US", "United States", ["CA", "XX"], ["California", "x"], "Mountain View"),
)
ERROR = ("retryable_error", None)


@pytest.fixture
def refreshed(search_server):
    """Inventory + results as the pipeline writes them, then one full refresh."""
    client, resource, _ = search_server
    for table in (
        "commoncrawl_ip_addresses",
        "ip_enrichment_results",
        "ip_enrichment_input",
    ):
        client.execute(f"TRUNCATE TABLE corpscout.{table}")
    client.execute(
        """INSERT INTO corpscout.commoncrawl_ip_addresses
        SELECT toUInt16(cityHash64(ip) % 256), ip, if(isIPv4String(ip), 4, 6),
            toDateTime64(first_seen, 3, 'UTC'), toDateTime64(last_seen, 3, 'UTC')
        FROM values('ip String, first_seen String, last_seen String',
            ('8.8.8.8', '2026-01-05 00:00:00', '2026-03-01 00:00:00'),
            ('8.8.8.8', '2026-01-01 00:00:00', '2026-02-01 00:00:00'),
            ('1.1.1.1', '2026-01-01 00:00:00', '2026-01-02 00:00:00'),
            ('5.5.5.5', '2026-01-01 00:00:00', '2026-01-02 00:00:00'),
            ('5.5.5.6', '2026-01-01 00:00:00', '2026-01-02 00:00:00'),
            ('2001:4860::8888', '2026-01-01 00:00:00', '2026-01-02 00:00:00'))"""
    )
    client.execute(
        f"""INSERT INTO corpscout.commoncrawl_ip_addresses
        SELECT toUInt16(cityHash64({FILLER_IP}) % 256), {FILLER_IP}, 4,
            toDateTime64('2026-01-01 00:00:00', 3, 'UTC'), toDateTime64('2026-01-02 00:00:00', 3, 'UTC')
        FROM numbers({FILLER_ROWS})"""
    )
    rows = [
        # 8.8.8.8: a complete first lookup, then a newer attempt whose City and RDAP
        # failed (the older data stays) and whose ASN answered with a new name.
        result_row(
            "8.8.8.8",
            "2026-09-01 00:00:00",
            city=GOOGLE_CITY,
            asn=("found", (15169, "GOOGLE")),
            rdap=("found", ("8.8.8.0/24", "arin", "GOGL", ["Google LLC", "Other"])),
        ),
        result_row(
            "8.8.8.8",
            "2026-09-10 00:00:00",
            city=ERROR,
            asn=("found", (15169, "Google LLC")),
            rdap=("terminal_error", None),
        ),
        # 1.1.1.1: a newer conclusive negative City lookup clears the old location.
        result_row(
            "1.1.1.1",
            "2026-09-01 00:00:00",
            city=("found", ("AU", "Australia", ["NSW"], ["New South Wales"], "Sydney")),
            asn=("found", (13335, "CLOUDFLARENET")),
            rdap=(
                "found",
                ("1.1.1.0/24", "apnic", "APNIC-LABS", ["APNIC and Cloudflare"]),
            ),
        ),
        result_row(
            "1.1.1.1",
            "2026-09-02 00:00:00",
            city=("not_found", None),
            asn=("found", (13335, "CLOUDFLARENET")),
            rdap=(
                "found",
                ("1.1.1.0/24", "apnic", "APNIC-LABS", ["APNIC and Cloudflare"]),
            ),
        ),
        result_row(
            "5.5.5.5",
            "2026-09-01 00:00:00",
            city=("found", ("SE", "Sweden", ["AB"], ["Stockholm"], "Stockholm")),
            asn=("found", (3301, "Telia Company AB")),
            rdap=("found", ("5.5.5.0/24", "ripe", "TELIA", [])),
        ),
        result_row(
            "5.5.5.6",
            "2026-09-01 00:00:00",
            city=("found", ("SE", "Sweden", ["O"], ["Vastra Gotaland"], "Gothenburg")),
            asn=("found", (3301, "Telia Company AB")),
            rdap=("found", ("5.5.5.0/24", "ripe", "TELIA", [])),
        ),
        # Enriched but not in the DNS inventory: not a row of the search table.
        result_row(
            "9.9.9.9",
            "2026-09-01 00:00:00",
            city=("found", ("CH", "Switzerland", [], [], "Zurich")),
            asn=("found", (19281, "QUAD9-AS-1")),
            rdap=("found", ("9.9.9.0/24", "arin", "QUAD9", [])),
        ),
    ]
    client.execute(
        f"INSERT INTO corpscout.ip_enrichment_results ({RESULT_COLUMNS}) VALUES "
        + ", ".join(rows)
    )
    client.execute(
        f"""INSERT INTO corpscout.ip_enrichment_results
            (ip, result_id, task_id, execution_id, input_id, source_run_id, processor_version,
             completed_at, ip_scope, city_lookup_status, country_iso_code, country_name,
             subdivision_iso_codes, subdivision_names, city_name, asn_lookup_status, asn,
             asn_organization, rdap_lookup_status)
        SELECT {FILLER_IP}, generateUUIDv4(), generateUUIDv4(), generateUUIDv4(), {FILLER_IP},
            'run', 'test', toDateTime64('2026-09-01 00:00:00', 6, 'UTC'), 'global', 'found',
            ['DE', 'FR', 'NL', 'GB'][number % 4 + 1], 'Filler', ['R' || toString(number % 7)],
            ['Region'], 'City ' || toString(number % 50), 'found',
            toUInt32(100000 + number % 400), 'Filler network ' || toString(number % 400), 'not_found'
        FROM numbers({FILLER_ROWS})"""
    )
    client.execute(f"SYSTEM REFRESH VIEW {search.SEARCH_RELATION}")
    client.execute(f"SYSTEM WAIT VIEW {search.SEARCH_RELATION}")
    return client, resource


NAMED = ("1.1.1.1", "2001:4860::8888", "5.5.5.5", "5.5.5.6", "8.8.8.8")


def test_one_row_per_inventory_ip_with_the_current_state(refreshed):
    client, _ = refreshed
    assert client.execute(
        f"SELECT count(), uniqExact(ip) FROM {search.SEARCH_RELATION}"
    ) == [(FILLER_ROWS + len(NAMED), FILLER_ROWS + len(NAMED))]
    rows = client.execute(
        f"""SELECT ip, ip_version, toString(first_seen), toString(last_seen), enriched,
            toString(completed_at), toString(city_lookup_status), toString(asn_lookup_status),
            toString(rdap_lookup_status), asn, asn_organization, country_iso_code,
            country_name, subdivision_iso_code, subdivision_name, city_name,
            rdap_matched_cidr, rdap_rir, rdap_name, rdap_registrant
        FROM {search.SEARCH_RELATION} WHERE ip IN %(ips)s ORDER BY ip""",
        {"ips": NAMED + ("9.9.9.9",)},
    )
    assert rows == [
        ("1.1.1.1", 4, "2026-01-01 00:00:00.000", "2026-01-02 00:00:00.000", 1,
         "2026-09-02 00:00:00.000000", "not_found", "found", "found", 13335,
         "CLOUDFLARENET", "", "", "", "", "", "1.1.1.0/24", "apnic", "APNIC-LABS",
         "APNIC and Cloudflare"),
        ("2001:4860::8888", 6, "2026-01-01 00:00:00.000", "2026-01-02 00:00:00.000", 0,
         None, "not_attempted", "not_attempted", "not_attempted", 0, "", "", "", "", "",
         "", "", "", "", ""),
        ("5.5.5.5", 4, "2026-01-01 00:00:00.000", "2026-01-02 00:00:00.000", 1,
         "2026-09-01 00:00:00.000000", "found", "found", "found", 3301,
         "Telia Company AB", "SE", "Sweden", "AB", "Stockholm", "Stockholm",
         "5.5.5.0/24", "ripe", "TELIA", ""),
        ("5.5.5.6", 4, "2026-01-01 00:00:00.000", "2026-01-02 00:00:00.000", 1,
         "2026-09-01 00:00:00.000000", "found", "found", "found", 3301,
         "Telia Company AB", "SE", "Sweden", "O", "Vastra Gotaland", "Gothenburg",
         "5.5.5.0/24", "ripe", "TELIA", ""),
        ("8.8.8.8", 4, "2026-01-01 00:00:00.000", "2026-03-01 00:00:00.000", 1,
         "2026-09-10 00:00:00.000000", "retryable_error", "found", "terminal_error", 15169,
         "Google LLC", "US", "United States", "CA", "California", "Mountain View",
         "8.8.8.0/24", "arin", "GOGL", "Google LLC"),
    ]  # fmt: skip


def test_values_match_ip_enrichment_current_for_every_enriched_ip(refreshed):
    """The lean aggregation must pick exactly what the current view picks."""
    client, _ = refreshed
    differing = client.execute(
        f"""SELECT s.ip FROM {search.SEARCH_RELATION} AS s
        INNER JOIN corpscout.ip_enrichment_current AS c ON c.bucket = s.bucket AND c.ip = s.ip
        WHERE (s.completed_at, s.city_lookup_status, s.asn_lookup_status, s.rdap_lookup_status,
               s.asn, s.asn_organization, s.country_iso_code, s.country_name,
               s.subdivision_iso_code, s.subdivision_name, s.city_name, s.rdap_matched_cidr,
               s.rdap_rir, s.rdap_name, s.rdap_registrant)
            != (c.completed_at, c.city_lookup_status, c.asn_lookup_status, c.rdap_lookup_status,
               ifNull(c.asn, 0), ifNull(c.asn_organization, ''), ifNull(c.country_iso_code, ''),
               ifNull(c.country_name, ''), c.subdivision_iso_codes[1], c.subdivision_names[1],
               ifNull(c.city_name, ''), ifNull(c.rdap_matched_cidr, ''), ifNull(c.rdap_rir, ''),
               ifNull(c.rdap_name, ''), c.rdap_registrant_names[1])"""
    )
    assert differing == []
    [(compared,)] = client.execute(
        f"""SELECT count() FROM {search.SEARCH_RELATION} AS s
        INNER JOIN corpscout.ip_enrichment_current AS c ON c.bucket = s.bucket AND c.ip = s.ip"""
    )
    assert compared == FILLER_ROWS + 4


def plan(client, sql: str) -> str:
    return "\n".join(row[0] for row in client.execute(sql))


def test_location_filters_read_the_location_projection_in_order(refreshed):
    client, _ = refreshed
    explained = plan(
        client,
        f"""EXPLAIN actions = 1, indexes = 1
        SELECT bucket, ip FROM {search.SEARCH_RELATION}
        WHERE country_iso_code IN ('SE') AND subdivision_iso_code IN ('AB')
          AND (country_iso_code, subdivision_iso_code, city_name, bucket, ip)
              > ('SE', 'AB', '', 0, '')
        ORDER BY country_iso_code, subdivision_iso_code, city_name, bucket, ip
        LIMIT 51""",
    )
    assert "ReadFromMergeTree (by_location)" in explained
    assert "ReadType: InOrder" in explained
    assert client.execute(
        f"""SELECT ip FROM {search.SEARCH_RELATION}
        WHERE country_iso_code = 'SE' ORDER BY country_iso_code, subdivision_iso_code,
            city_name, bucket, ip"""
    ) == [("5.5.5.5",), ("5.5.5.6",)]


def test_asn_filter_reads_the_primary_key_in_order(refreshed):
    client, _ = refreshed
    explained = plan(
        client,
        f"""EXPLAIN actions = 1, indexes = 1
        SELECT bucket, ip FROM {search.SEARCH_RELATION}
        WHERE asn IN (3301) AND (asn, bucket, ip) > (0, 0, '')
        ORDER BY asn, bucket, ip LIMIT 51""",
    )
    assert "by_location" not in explained and "ReadType: InOrder" in explained
    # The primary key range of one ASN, not a scan.
    assert "Condition: (asn in 1-element set)" in explained
    assert "Granules: 1/" in explained


def test_count_projections_answer_the_filter_pickers(refreshed):
    client, _ = refreshed
    for sql, projection in (
        (
            "SELECT asn, asn_organization, count() FROM {t} GROUP BY asn, asn_organization",
            "asn_counts",
        ),
        (
            "SELECT country_iso_code, country_name, count() FROM {t} "
            "GROUP BY country_iso_code, country_name",
            "country_counts",
        ),
        (
            "SELECT subdivision_iso_code, subdivision_name, count() FROM {t} "
            "WHERE country_iso_code = 'SE' GROUP BY subdivision_iso_code, subdivision_name",
            "location_counts",
        ),
        (
            "SELECT city_name, count() FROM {t} WHERE country_iso_code = 'SE' "
            "AND subdivision_iso_code IN ('AB') GROUP BY city_name",
            "location_counts",
        ),
    ):
        query = sql.format(t=search.SEARCH_RELATION)
        assert f"ReadFromMergeTree ({projection})" in plan(client, f"EXPLAIN {query}")
    assert client.execute(
        f"""SELECT asn, asn_organization, count() FROM {search.SEARCH_RELATION}
        WHERE asn IN (3301, 15169) GROUP BY asn, asn_organization ORDER BY asn"""
    ) == [(3301, "Telia Company AB", 2), (15169, "Google LLC", 1)]


def test_exact_ip_search_uses_the_bloom_filter(refreshed):
    client, _ = refreshed
    explained = plan(
        client,
        f"""EXPLAIN indexes = 1 SELECT ip FROM {search.SEARCH_RELATION}
        WHERE bucket = toUInt16(cityHash64('8.8.8.8') % 256) AND ip = '8.8.8.8'""",
    )
    assert "Name: ip_bloom" in explained


def test_queue_selection_reads_the_search_table_with_the_backoffice_filters(
    refreshed, store
):
    client, resource = refreshed

    def selected(**config):
        client.execute(f"TRUNCATE TABLE {INPUT_RELATION}")
        result = materialize(
            resource,
            store[1],
            queue_scope=scope(),
            source_name="backoffice:ip-addresses",
            source_relation=search.SEARCH_RELATION,
            observed_at_column="last_seen",
            select_all=True,
            **config,
        )
        assert result.success
        return [
            row[0]
            for row in client.execute(f"SELECT ip FROM {INPUT_RELATION} ORDER BY ip")
        ]

    assert selected(filters={"asn": ["3301", "15169"], "ip_version": ["4"]}) == [
        "5.5.5.5",
        "5.5.5.6",
        "8.8.8.8",
    ]
    assert selected(
        filters={"country_iso_code": ["SE"], "subdivision_iso_code": ["AB"]}
    ) == ["5.5.5.5"]
    assert selected(
        filters={"country_iso_code": ["SE"], "city_name": ["Gothenburg"]}
    ) == ["5.5.5.6"]
    assert selected(
        filters={"asn": ["3301"]}, ip_search="5.5.5.", excluded_ips=["5.5.5.6"]
    ) == ["5.5.5.5"]
    [(observed,)] = client.execute(
        f"SELECT toString(observed_at) FROM {INPUT_RELATION} WHERE ip = '5.5.5.5'"
    )
    assert observed == "2026-01-02 00:00:00.000000"


def test_a_refresh_request_returns_at_once_and_the_view_rebuilds(refreshed, caplog):
    client, _ = refreshed
    client.execute(
        """INSERT INTO corpscout.commoncrawl_ip_addresses VALUES
        (toUInt16(cityHash64('4.4.4.4') % 256), '4.4.4.4', 4, '2026-01-01', '2026-01-02')"""
    )
    log = logging.getLogger("test-search-refresh")
    started = time.monotonic()
    assert search.request_search_refresh(client, log) is True
    assert time.monotonic() - started < 5  # returned without waiting for the rebuild
    deadline = time.monotonic() + 60
    # The rebuild swaps in a table that holds the newly observed address.
    while client.execute(
        f"SELECT enriched FROM {search.SEARCH_RELATION} WHERE ip = '4.4.4.4'"
    ) != [(0,)]:
        assert time.monotonic() < deadline, "the requested refresh never landed"
        time.sleep(0.2)
    client.execute(f"SYSTEM WAIT VIEW {search.SEARCH_RELATION}")
    result = search.search_freshness(
        client.execute(search.REFRESH_STATE_SQL), datetime.now(UTC)
    )
    assert result.passed, result.description


def test_the_freshness_check_warns_before_the_first_refresh(search_server):
    _, _, initial_state = search_server
    result = search.search_freshness(initial_state, datetime.now(UTC))
    assert not result.passed and "never refreshed successfully" in result.description
    assert result.severity == dg.AssetCheckSeverity.WARN


class FakeClient:
    def __init__(self, error=None):
        self.statements, self.error = [], error

    def execute(self, sql, *args, **kwargs):
        self.statements.append(sql)
        if self.error:
            raise self.error


def test_refresh_request_issues_one_statement_and_never_waits():
    client = FakeClient()
    assert search.request_search_refresh(client, logging.getLogger("t")) is True
    assert client.statements == ["SYSTEM REFRESH VIEW corpscout.ip_enrichment_search"]


def test_a_failed_refresh_request_is_a_warning(caplog):
    client = FakeClient(RuntimeError("view is missing"))
    with caplog.at_level(logging.WARNING):
        assert search.request_search_refresh(client, logging.getLogger("t")) is False
    assert (
        "Could not request a refresh" in caplog.text
        and "view is missing" in caplog.text
    )


def test_freshness_rules():
    now = datetime(2026, 9, 29, 12, tzinfo=UTC)
    fresh = int((now - timedelta(hours=20)).timestamp())
    stale = int((now - timedelta(hours=37)).timestamp())
    assert search.search_freshness([("Scheduled", "", fresh)], now).passed
    old = search.search_freshness([("Scheduled", "", stale)], now)
    assert not old.passed and "more than 36 hours ago" in old.description
    failed = search.search_freshness(
        [("Scheduled", "Memory limit exceeded", fresh)], now
    )
    assert not failed.passed and "Memory limit exceeded" in failed.description
    missing = search.search_freshness([], now)
    assert not missing.passed and missing.metadata["refresh_row_found"].value is False
    assert not search.search_freshness([("Scheduled", "", None)], now).passed


def test_check_targets_the_results_asset_and_the_job_selects_only_the_check():
    assert search.ip_enrichment_search_fresh.check_key == search.CHECK_KEY
    assert search.CHECK_KEY.asset_key == dg.AssetKey("ip_enrichment_results")
    assert (
        search.ip_enrichment_search_freshness_job.selection
        == dg.AssetSelection.checks(search.CHECK_KEY)
    )
