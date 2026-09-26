"""Task-scoped enrichment with real storage and controlled external lookup responses."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import dagster as dg
import pytest
from clickhouse_driver import Client

from dagster_v3.defs.common.processing import ProcessingResource
from dagster_v3.defs.commoncrawl_geoip.resources import MaxMindDatabaseResource
from dagster_v3.defs.commoncrawl_rdap import client as rdap_client
from dagster_v3.defs.commoncrawl_rdap import apnic_whois, ripe_rest
from dagster_v3.defs.commoncrawl_rdap.assets import (
    RDAP_NETWORK_INSERT_SQL,
    RDAP_SEGMENT_INSERT_SQL,
)
from dagster_v3.defs.commoncrawl_rdap.client import RdapClient, RdapClientError
from dagster_v3.defs.commoncrawl_rdap.rdap import (
    RdapLookupResponse,
    normalize_rdap_network,
)
from dagster_v3.defs.commoncrawl_rdap.apnic_whois import ApnicWhoisClient
from dagster_v3.defs.commoncrawl_rdap.ripe_rest import RipeRestClient
from dagster_v3.defs.ip_enrichment import enrichment, results
from dagster_v3.defs.ip_enrichment.enrichment import (
    IpEnrichmentResultsConfig,
    RdapEnricher,
)
from tests.test_ip_enrichment_input import (
    materialize as prepare_input,
    server as server,
)
from tests.test_ip_registry import apply_migration, seed_reference_data
from tests.test_processing_store import (
    processing_postgres_url as processing_postgres_url,
    store as store,
)

# The real RdapClient.lookup_ip, before the environment fixture stubs it.
REAL_RDAP_LOOKUP_IP = RdapClient.lookup_ip


class Reader:
    def __init__(self, kind):
        self.kind = kind
        self.calls = []
        self.fail = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def metadata(self):
        return SimpleNamespace(
            database_type=f"GeoLite2-{self.kind}", build_epoch=1785542400
        )

    def get_with_prefix_len(self, address):
        self.calls.append(str(address))
        if self.fail:
            raise ValueError("unreadable test record")
        prefix = 24 if address.version == 4 else 48
        if self.kind == "ASN":
            return {
                "autonomous_system_number": 15169,
                "autonomous_system_organization": "Test ASN",
            }, prefix
        return {
            "country": {"iso_code": "US", "names": {"en": "United States"}},
            "city": {"names": {"en": "Test City"}},
            "location": {"latitude": 37.4, "longitude": -122.1},
        }, prefix


def response(ip, *, start=None, end=None, **raw):
    ipv6 = ":" in ip
    return RdapLookupResponse(
        rir="arin",
        raw_response={
            "objectClassName": "ip network",
            "handle": "TEST-" + ip,
            "startAddress": start
            or ("2001:4860::" if ipv6 else ip.rsplit(".", 1)[0] + ".0"),
            "endAddress": end
            or (
                "2001:4860:ffff:ffff:ffff:ffff:ffff:ffff"
                if ipv6
                else ip.rsplit(".", 1)[0] + ".255"
            ),
            "ipVersion": "v6" if ipv6 else "v4",
            "name": "Test registration",
            "country": "CA",
            "status": ["active"],
            **raw,
        },
    )


def rest_object(
    ip, *, start=None, end=None, netname="RIPE-TEST-NET", org="ORG-TEST1-RIPE"
):
    """A RIPE REST search object (inetnum) as rest.db.ripe.net returns it with no-referenced."""
    start = start or ip.rsplit(".", 1)[0] + ".0"
    end = end or ip.rsplit(".", 1)[0] + ".255"
    attributes = [
        {"name": "inetnum", "value": f"{start} - {end}"},
        {"name": "netname", "value": netname},
        {"name": "descr", "value": "A person's name may appear here"},
        {"name": "country", "value": "SE"},
        {"name": "admin-c", "value": "AB1234-RIPE"},
        {"name": "tech-c", "value": "AB1234-RIPE"},
        {"name": "status", "value": "ASSIGNED PA"},
        {"name": "mnt-by", "value": "TEST-MNT"},
        {"name": "created", "value": "2010-05-04T10:00:00Z"},
        {"name": "last-modified", "value": "2024-01-02T03:04:05Z"},
        {"name": "source", "value": "RIPE"},
    ]
    if org:
        attributes.insert(4, {"name": "org", "value": org})
    return {
        "type": "inetnum",
        "primary-key": {
            "attribute": [{"name": "inetnum", "value": f"{start} - {end}"}]
        },
        "attributes": {"attribute": attributes},
    }


# The answer of `whois -h whois.apnic.net -- "-r 103.35.64.49"` captured on 2026-09-26,
# verbatim: the % comments, the multi-line route descr and the route object after the
# inetnum are all real and the parser must skip them.
APNIC_FPT_ANSWER = """% [whois.apnic.net]
% Whois data copyright terms    http://www.apnic.net/db/dbcopyright.html

% Information related to '103.35.64.0 - 103.35.67.255'

% Abuse contact for '103.35.64.0 - 103.35.67.255' is 'hm-changed@vnnic.vn'

inetnum:        103.35.64.0 - 103.35.67.255
netname:        FPT-VN
descr:          FPT Telecom
descr:          2nd Floor, FPT Cau Giay Tower, 17 Duy Tan, Dich Vong Hau, Cau Giay District, Hanoi
admin-c:        FHIG1-AP
tech-c:         FHIG1-AP
country:        VN
mnt-by:         MAINT-VN-VNNIC
mnt-lower:      MAINT-VN-FPT
mnt-irt:        IRT-VNNIC-AP
mnt-routes:     MAINT-VN-FPT
status:         ALLOCATED PORTABLE
last-modified:  2017-11-19T08:36:30Z
source:         APNIC

% Information related to '103.35.64.0/24AS18403'

route:          103.35.64.0/24
origin:         AS18403
descr:          Vietnam Internet Network Information Center (VNNIC)
                18 Nguyen Du Str, Hai Ba Trung District, Hanoi City, Vietnam
                10th floor, MITEC Tower, Duong Dinh Nghe, Cau Giay, Hanoi
mnt-by:         MAINT-VN-FPT
last-modified:  2019-08-01T06:36:29Z
source:         APNIC

% This query was served by the APNIC Whois Service version 1.88.48 (WHOIS-UK2)


"""

# JPNIC's own object for a /24 it holds (from the 2026-09-25 dump): NIR-managed space whose
# holder lives in JPNIC's database.
APNIC_JPNIC_ANSWER = """% [whois.apnic.net]

inetnum:        202.12.14.0 - 202.12.14.255
netname:        JPNIC-NET-JP
descr:          Japan Network Information Center
country:        JP
admin-c:        JNIC1-AP
tech-c:         JNIC1-AP
status:         ASSIGNED PORTABLE
mnt-by:         MAINT-JPNIC
last-modified:  2008-09-04T06:51:28Z
source:         APNIC

% This query was served by the APNIC Whois Service version 1.88.48 (WHOIS-UK2)
"""


def apnic_answer(
    ip,
    *,
    netname="APNIC-TEST-NET",
    descr=("Test Holder Pty Ltd", "1 Test Street, Sydney NSW 2000"),
    status="ASSIGNED NON-PORTABLE",
    mnt_by="MAINT-AU-TEST",
):
    """A port-43 answer with -r for the /24 around ``ip``, in the shape whois.apnic.net returns."""
    start, end = ip.rsplit(".", 1)[0] + ".0", ip.rsplit(".", 1)[0] + ".255"
    lines = [
        f"inetnum:        {start} - {end}",
        f"netname:        {netname}",
        *(f"descr:          {line}" for line in descr),
        "country:        AU",
        "admin-c:        TEST1-AP",
        "tech-c:         TEST1-AP",
        f"mnt-by:         {mnt_by}",
        f"status:         {status}",
        "last-modified:  2024-01-02T03:04:05Z",
        "source:         APNIC",
    ]
    return (
        "% [whois.apnic.net]\n% Whois data copyright terms    http://www.apnic.net/db/dbcopyright.html\n\n"
        + "\n".join(lines)
        + "\n\n% This query was served by the APNIC Whois Service version 1.88.34 (WHOIS-AU1)\n"
    )


@pytest.fixture
def environment(server, store, tmp_path, monkeypatch):
    client, resource = server
    migration = (
        Path(__file__).resolve().parents[3]
        / "clickhouse/migrations/000124_corpscout_rdap_networks.up.sql"
    )
    for statement in migration.read_text().split("CREATE DICTIONARY", 1)[0].split(";"):
        if any(
            line.strip() and not line.lstrip().startswith("--")
            for line in statement.splitlines()
        ):
            client.execute(statement)
    client.execute("DROP DICTIONARY IF EXISTS corpscout.rdap_network_trie")
    client.execute("""CREATE DICTIONARY corpscout.rdap_network_trie
        (cidr String, matched_cidr String, network_key String) PRIMARY KEY cidr
        SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'test' PASSWORD 'test'
            DB 'corpscout' TABLE 'rdap_network_segments_current'))
        LAYOUT(IP_TRIE()) LIFETIME(0)""")
    # Reference data, classes and the class-aware trie view (000450/000451), idempotent.
    apply_migration(client, "000450_corpscout_ip_registry_reference_data.up.sql")
    apply_migration(
        client, "000451_corpscout_rdap_trie_registry_class_exclusion.up.sql"
    )
    for table in (
        "ip_enrichment_input",
        "ip_enrichment_results",
        "rdap_networks",
        "rdap_network_segments",
        "rdap_ip_lookup_results",
        "rdap_network_registry_class",
        "ip_registry_snapshots",
        "ip_registry_iana_blocks",
        "ip_registry_special_segments",
        "ip_registry_holder_blocks",
    ):
        client.execute(f"TRUNCATE TABLE corpscout.{table}")
    client.execute("SYSTEM RELOAD DICTIONARY corpscout.ip_registry_special_trie")
    city, asn = Reader("City"), Reader("ASN")
    for kind in ("City", "ASN"):
        (tmp_path / f"GeoLite2-{kind}.mmdb").touch()
    monkeypatch.setattr(
        enrichment.maxminddb,
        "open_database",
        lambda path: city if "City" in str(path) else asn,
    )
    calls = []

    def lookup(self, ip):
        calls.append(ip)
        return response(ip)

    monkeypatch.setattr(enrichment.RdapClient, "lookup_ip", lookup)
    # RIPE addresses (5/8 and 2a0x:: in these tests) go to the REST client, 202/8 to APNIC's
    # whois (its real parser over a stubbed port-43 exchange); everything else to RDAP as
    # ARIN. Every stub records the address in env.calls.
    monkeypatch.setattr(
        RdapClient,
        "registry_for",
        lambda self, ip: (
            "ripe"
            if ip.startswith(("5.", "2a0"))
            else "apnic"
            if ip.startswith("202.")
            else "arin"
        ),
    )

    def rest_lookup(self, ip):
        calls.append(ip)
        return RdapLookupResponse(
            rir="ripe", raw_response=ripe_rest.rdap_shape(rest_object(ip))
        )

    def apnic_query(self, ip):
        calls.append(ip)
        return apnic_answer(ip)

    monkeypatch.setattr(RipeRestClient, "lookup_ip", rest_lookup)
    monkeypatch.setattr(ApnicWhoisClient, "query", apnic_query)
    with dg.DagsterInstance.ephemeral() as instance:
        yield SimpleNamespace(
            client=client,
            resource=resource,
            dsn=store[1],
            calls=calls,
            city=city,
            asn=asn,
            instance=instance,
            maxmind=MaxMindDatabaseResource(database_directory=str(tmp_path)),
        )


def select(env, ips, *, scope=None, **config):
    """Append ``ips`` to the open draft of ``scope`` (a fresh scope by default); the task id."""
    result = prepare_input(
        env.resource,
        env.dsn,
        queue_scope=scope or "scope-" + uuid4().hex,
        ips=ips,
        **config,
    )
    assert result.success
    return (
        result.asset_materializations_for_node("ip_enrichment_input")[0]
        .metadata["task_id"]
        .value
    )


def run(env, task, **config):
    return dg.materialize(
        [results.ip_enrichment_results, dg.AssetSpec("ip_enrichment_input")],
        instance=env.instance,
        resources={
            "clickhouse": env.resource,
            "processing": ProcessingResource(postgres_url=env.dsn),
            "maxmind_geoip": env.maxmind,
        },
        run_config={
            "ops": {
                "ip_enrichment_results": {
                    "config": {"task_id": task, "request_delay_seconds": 0, **config}
                }
            }
        },
        raise_on_error=False,
    )


def outcome(result):
    return {
        key: value.value
        for key, value in result.asset_materializations_for_node(
            "ip_enrichment_results"
        )[0].metadata.items()
    }


def task_row(env, task):
    with ProcessingResource(postgres_url=env.dsn).get_store() as store:
        return store.task(task)


def test_draft_geoip_rdap_segments_completion_and_purge(environment):
    env = environment
    select(env, ["9.9.9.9"])
    task = select(env, ["8.8.8.8", "8.8.8.9", "2001:4860::8888", "127.0.0.1"])
    first = run(env, task, batch_size=1)
    assert first.success
    # One request per /24: whichever of 8.8.8.8/8.8.8.9 comes first in bucket order.
    assert sorted(call.rsplit(".", 1)[0] for call in env.calls) == [
        "2001:4860::8888",
        "8.8.8",
    ]
    assert "127.0.0.1" not in env.city.calls
    assert env.client.execute("""SELECT ip, country_iso_code, rdap_country_code,
        city_network, asn_network, rdap_matched_cidr FROM corpscout.ip_enrichment_current
        WHERE ip='8.8.8.8'""") == [
        ("8.8.8.8", "US", "CA", "8.8.8.0/24", "8.8.8.0/24", "8.8.8.0/24")
    ]
    assert env.client.execute(
        "SELECT count(), uniqExact(task_id), uniqExact(execution_id), min(attempt) FROM corpscout.ip_enrichment_results FINAL"
    ) == [(4, 1, 1, 1)]
    assert env.client.execute(
        "SELECT rdap_lookup_status, ip_scope FROM corpscout.ip_enrichment_current WHERE ip='127.0.0.1'"
    ) == [("not_global", "loopback")]
    assert env.client.execute(
        "SELECT count() FROM corpscout.rdap_network_segments FINAL"
    ) == [(2,)]
    metadata = outcome(first)
    assert metadata["completion_status"] == "completed"
    assert (
        metadata["written"],
        metadata["succeeded_pages"],
        metadata["failed_pages"],
        metadata["skipped_recent"],
    ) == (4, 4, 0, 0)
    assert metadata["execution_id"] == first.run_id
    assert metadata["rdap_requests_by_registry"] == {"arin": 2}
    assert (
        metadata["geolite2_city_build"] == "2026-08-01"
    )  # the fixture readers' build epoch 1785542400
    assert (metadata["budget_waits"], metadata["rdap_deferrals_by_registry"]) == (0, {})
    record = task_row(env, task)
    assert record["status"] == "completed" and record["inputs_purged_at"] is not None
    assert (
        record["succeeded_count"],
        record["terminal_failed_count"],
        record["skipped_count"],
    ) == (4, 0, 0)
    assert env.client.execute(
        "SELECT count() FROM corpscout.ip_enrichment_input WHERE task_id = %(task)s",
        {"task": task},
    ) == [(0,)]
    tags = env.instance.get_run_by_id(first.run_id).tags
    assert (
        tags["ip_enrichment/outcome"] == "completed"
        and tags["ip_enrichment/succeeded_pages"] == "4"
    )
    calls_before = list(env.calls)
    # A completed task re-run only retries cleanup; another draft reuses the cached coverage.
    assert outcome(run(env, task))["already_completed"] is True
    assert run(env, select(env, ["8.8.8.10"])).success
    assert env.calls == calls_before


def test_errors_are_published_outcomes_and_backoff_holds_until_forced(
    environment, monkeypatch
):
    env = environment
    attempts = []

    def unavailable(self, ip):
        attempts.append(ip)
        raise RdapClientError(
            "rate limit", code="rate_limited", retryable=True, status_code=429
        )

    monkeypatch.setattr(RdapClient, "lookup_ip", unavailable)
    task = select(env, ["8.8.8.8"])
    first = run(env, task)
    assert (
        first.success and outcome(first)["completion_status"] == "completed_with_errors"
    )
    assert env.client.execute("""SELECT city_lookup_status, asn_lookup_status, rdap_lookup_status,
        country_iso_code, rdap_error_code, rdap_retry_after > rdap_checked_at
        FROM corpscout.ip_enrichment_current""") == [
        ("found", "found", "retryable_error", "US", "rate_limited", 1)
    ]
    record = task_row(env, task)
    assert (
        record["status"],
        record["terminal_failed_count"],
        record["inputs_purged_at"] is not None,
    ) == ("completed", 1, True)
    # The failed address goes to a new draft (retry mode); the saved backoff still applies there.
    retry = prepare_input(
        env.resource,
        env.dsn,
        queue_scope="scope-" + uuid4().hex,
        retry_failed_task_id=task,
    )
    assert retry.success
    again = (
        retry.asset_materializations_for_node("ip_enrichment_input")[0]
        .metadata["task_id"]
        .value
    )
    assert task_row(env, again)["total"] == 1
    assert run(env, again).success and attempts == ["8.8.8.8"]
    monkeypatch.setattr(RdapClient, "lookup_ip", lambda self, ip: response(ip))
    assert run(env, select(env, ["8.8.8.8"]), force_rdap=True).success
    assert env.client.execute(
        "SELECT rdap_lookup_status FROM corpscout.ip_enrichment_current"
    ) == [("found",)]


def test_request_budget_fails_the_run_and_the_same_task_resumes(environment):
    env = environment
    task = select(env, ["1.1.1.1", "8.8.8.8"])
    first = run(env, task, max_requests=1)
    assert not first.success
    assert env.client.execute(
        "SELECT count() FROM corpscout.ip_enrichment_results FINAL"
    ) == [(1,)]
    assert task_row(env, task)["status"] == "selected"
    # Transport settings may change; the saved execution is resumed without execution_id.
    resumed = run(
        env, task, max_requests=1, batch_size=7, registry_daily_budgets={"ripe": 5}
    )
    assert resumed.success and outcome(resumed)["execution_id"] == first.run_id
    assert env.calls == ["1.1.1.1", "8.8.8.8"]
    assert env.client.execute(
        "SELECT count(), uniqExact(execution_id) FROM corpscout.ip_enrichment_results FINAL"
    ) == [(2, 1)]


def test_registry_budget_defers_and_the_run_waits_for_the_window(
    environment, monkeypatch
):
    env = environment
    clock = {"now": 0.0}
    slept = []

    def fake_sleep(seconds):
        slept.append(seconds)
        clock["now"] += seconds

    monkeypatch.setattr(enrichment, "monotonic", lambda: clock["now"])
    monkeypatch.setattr(enrichment, "sleep", fake_sleep)
    task = select(env, ["5.1.1.1", "5.2.2.2", "8.8.8.8"])
    result = run(env, task, batch_size=1, registry_daily_budgets={"ripe": 1})
    assert result.success
    # One RIPE miss per day: whichever RIPE address comes first in bucket order is fetched,
    # the other is deferred, ARIN continues, then the run waits a full window.
    assert (
        len(env.calls) == 3
        and "8.8.8.8" in env.calls[:2]
        and env.calls[2].startswith("5.")
    )
    metadata = outcome(result)
    assert metadata["completion_status"] == "completed" and metadata["written"] == 3
    assert metadata["budget_waits"] == 1 and metadata[
        "budget_wait_seconds"
    ] == pytest.approx(86_400)
    assert metadata["rdap_deferrals_by_registry"] == {
        "ripe": 2
    }  # once per pass until it slept
    assert metadata["rdap_requests_by_registry"] == {"ripe": 2, "arin": 1}
    assert "ripe" not in metadata["rdap_person_entities_by_registry"]
    assert sum(slept) == pytest.approx(86_400)


def test_registry_usage_sql_reads_the_last_day(environment):
    env = environment
    seed_network(
        env,
        "5.1.1.1",
        fetched_at=datetime.now(UTC) - timedelta(hours=1),
        handle="RIPE-1",
    )
    seed_network(
        env,
        "5.2.2.2",
        fetched_at=datetime.now(UTC) - timedelta(days=2),
        handle="RIPE-2",
    )
    rows = env.client.execute(results.REGISTRY_USAGE_SQL)
    assert [(rir, 3500 < seconds < 3700) for rir, seconds in rows] == [("arin", True)]
    enricher = resolver(env, registry_daily_budgets={"arin": 1})
    enricher.seed_registry_usage(rows)
    assert enricher.resolve_page(page(env, "8.8.8.8")) == {} and enricher.deferred == {
        "arin": 1
    }


@pytest.mark.parametrize("kind", ["catch_all", "wrong_range"])
def test_invalid_registration_coverage_is_a_terminal_error(
    environment, monkeypatch, kind
):
    env = environment
    monkeypatch.setattr(
        RdapClient,
        "lookup_ip",
        lambda self, ip: response(
            ip,
            start="0.0.0.0" if kind == "catch_all" else "9.9.9.0",
            end="255.255.255.255" if kind == "catch_all" else "9.9.9.255",
        ),
    )
    result = run(env, select(env, ["8.8.8.8"]))
    assert result.success and outcome(result)["failed_pages"] == 1
    assert env.client.execute(
        "SELECT rdap_lookup_status, city_lookup_status FROM corpscout.ip_enrichment_current"
    ) == [("terminal_error", "found")]
    assert env.client.execute("SELECT count() FROM corpscout.rdap_networks") == [(0,)]


def test_city_lookup_failure_does_not_discard_asn_or_rdap(environment):
    env = environment
    env.city.fail = True
    result = run(env, select(env, ["8.8.8.8"]))
    assert (
        result.success
        and outcome(result)["completion_status"] == "completed_with_errors"
    )
    assert env.client.execute(
        "SELECT city_lookup_status, asn_lookup_status, rdap_lookup_status, asn FROM corpscout.ip_enrichment_current"
    ) == [("retryable_error", "found", "found", 15169)]


def test_task_id_must_name_a_draft(environment):
    env = environment
    assert not run(env, str(uuid4())).success
    legacy = str(uuid4())
    with ProcessingResource(postgres_url=env.dsn).get_store() as store:
        store.prepare_selection(
            legacy, processor="ip-enrichment-v1", fingerprint="legacy"
        )
        assert store.task(legacy)["queue_scope"] is None
    assert not run(env, legacy).success
    assert env.calls == env.city.calls == []


def test_resume_after_lost_write_acknowledgement_does_not_repeat_lookups(
    environment, monkeypatch
):
    env = environment
    task = select(env, ["8.8.8.8"])
    execute = Client.execute
    interrupted = False

    def lost_ack(self, query, *args, **kwargs):
        nonlocal interrupted
        value = execute(self, query, *args, **kwargs)
        if (
            query.lstrip().startswith("INSERT INTO corpscout.ip_enrichment_results")
            and not interrupted
        ):
            interrupted = True
            raise ConnectionError("lost acknowledgement after durable insert")
        return value

    monkeypatch.setattr(Client, "execute", lost_ack)
    first = run(env, task)
    assert not first.success
    assert env.client.execute(
        "SELECT count() FROM corpscout.ip_enrichment_results FINAL"
    ) == [(1,)]
    assert run(env, task).success
    assert env.calls == env.city.calls == ["8.8.8.8"]
    assert env.client.execute(
        "SELECT count() FROM corpscout.ip_enrichment_results FINAL"
    ) == [(1,)]


def test_non_aligned_rdap_range_saves_exact_matching_segment(environment, monkeypatch):
    env = environment
    monkeypatch.setattr(
        RdapClient,
        "lookup_ip",
        lambda self, ip: response(ip, start="8.8.8.1", end="8.8.8.10"),
    )
    assert run(env, select(env, ["8.8.8.8"])).success
    assert env.client.execute("""SELECT rdap_start_address, rdap_end_address, rdap_matched_cidr
        FROM corpscout.ip_enrichment_current""") == [
        ("8.8.8.1", "8.8.8.10", "8.8.8.8/31")
    ]
    assert env.client.execute(
        "SELECT count() FROM corpscout.rdap_network_segments FINAL"
    ) == [(5,)]


def test_optional_parent_failure_preserves_direct_registration(
    environment, monkeypatch
):
    env = environment

    def direct(self, ip):
        found = response(ip)
        found.raw_response["links"] = [
            {"rel": "up", "href": "https://rdap.arin.net/registry/ip/8.0.0.0/8"}
        ]
        return found

    def parent(self, url, *, rir):
        raise RdapClientError(
            "parent unavailable", code="remote_server", retryable=True
        )

    monkeypatch.setattr(RdapClient, "lookup_ip", direct)
    monkeypatch.setattr(RdapClient, "lookup_up_url", parent)
    result = run(env, select(env, ["8.8.8.8"]))
    assert result.success
    metadata = outcome(result)
    assert metadata["parent_lookup_failures"] == 1 and metadata["rdap_requests"] == 2
    assert env.client.execute(
        "SELECT rdap_lookup_status, rdap_matched_cidr FROM corpscout.ip_enrichment_current"
    ) == [("found", "8.8.8.0/24")]


def test_resume_rejects_a_changed_lookup_policy_or_a_foreign_execution(environment):
    env = environment
    task = select(env, ["1.1.1.1", "8.8.8.8"])
    first = run(env, task, max_requests=1)
    assert not first.success
    assert not run(env, task, force_rdap=True).success  # frozen profile
    assert not run(env, task, rdap_cache_days=5).success
    assert not run(
        env, task, execution_id=str(uuid4())
    ).success  # only the saved execution resumes
    assert env.calls == ["1.1.1.1"]
    assert run(env, task, execution_id=first.run_id, max_requests=5).success


def test_page_work_is_bounded_per_page_not_per_address(environment, monkeypatch):
    env = environment
    seed_network(env, "8.8.8.1", fetched_at=datetime.now(UTC))
    ips = [f"8.8.8.{n}" for n in range(1, 41)]
    queries = []
    execute = Client.execute

    def counting(self, query, *args, **kwargs):
        queries.append(query)
        return execute(self, query, *args, **kwargs)

    monkeypatch.setattr(Client, "execute", counting)
    assert run(env, select(env, ips), batch_size=10).success
    assert env.calls == []
    four_pages = query_kinds(queries)
    assert (
        four_pages["negative"] <= 4
        and four_pages["trie"] <= 4
        and four_pages["networks"] <= 1
    )
    assert (
        four_pages["markers"] == 0 and four_pages["results"] == 1
    )  # 40 rows < 500: one flush
    assert four_pages["context"] == 0
    assert not any("raw_response" in q for q in queries)
    queries.clear()
    assert run(env, select(env, ips), batch_size=40).success
    one_page = query_kinds(queries)
    assert (
        one_page["negative"] <= 1 and one_page["trie"] <= 1 and one_page["results"] == 1
    )
    assert env.client.execute(
        "SELECT count() FROM corpscout.ip_enrichment_results FINAL"
    ) == [(80,)]


def test_lost_cleanup_ack_does_not_repeat_lookups(environment, monkeypatch):
    env = environment
    task = select(env, ["8.8.8.8"])
    execute = Client.execute
    interrupted = False

    def lost_ack(self, query, *args, **kwargs):
        nonlocal interrupted
        value = execute(self, query, *args, **kwargs)
        if (
            query.startswith("ALTER TABLE corpscout.ip_enrichment_input DROP PARTITION")
            and not interrupted
        ):
            interrupted = True
            raise ConnectionError("lost cleanup acknowledgement")
        return value

    monkeypatch.setattr(Client, "execute", lost_ack)
    assert not run(env, task).success
    record = task_row(env, task)
    assert record["status"] == "completed" and record["inputs_purged_at"] is None
    before = list(env.calls)
    assert run(env, task).success
    assert env.calls == before and task_row(env, task)["inputs_purged_at"] is not None


def test_new_submissions_after_start_form_the_next_draft(environment):
    env = environment
    scope = "scope-" + uuid4().hex
    first = select(env, ["8.8.8.8"], scope=scope)
    assert run(env, first).success
    second = select(env, ["1.1.1.1"], scope=scope)
    assert second != first and task_row(env, second)["status"] == "draft"


def test_run_metadata_publishes_every_per_registry_counter(environment):
    env = environment
    metadata = outcome(run(env, select(env, ["5.1.1.1", "202.1.1.1", "8.8.8.8"])))
    assert metadata["rdap_requests_by_registry"] == {"ripe": 1, "apnic": 1, "arin": 1}
    assert (
        metadata["rdap_fallbacks_by_registry"],
        metadata["reroutes_by_registry"],
        metadata["pauses_by_registry"],
        metadata["rdap_person_entities_by_registry"],
    ) == ({}, {}, {}, {})


def test_run_metadata_publishes_reroutes_and_fallbacks(environment, monkeypatch):
    env = environment
    arin, ripe = "https://rdap.arin.net/registry/ip/", "https://rdap.db.ripe.net/ip/"
    monkeypatch.setattr(RdapClient, "lookup_ip", REAL_RDAP_LOOKUP_IP)
    fetched = fake_rdap_http(
        monkeypatch,
        {
            # RIPE-managed space inside an ARIN block: rerouted to REST.
            arin + "45.10.1.1": HttpAnswer(301, location=ripe + "45.10.1.1"),
            # The fallback of a catch-all REST answer follows RDAP to RIPE.
            arin + "5.9.9.9": HttpAnswer(301, location=ripe + "5.9.9.9"),
            ripe + "5.9.9.9": HttpAnswer(
                200, body=ripe_rdap_body("5.9.9.9", persons=2)
            ),
        },
    )
    stub = RipeRestClient.lookup_ip  # the fixture's REST stub

    def rest(self, ip):
        if ip != "5.9.9.9":
            return stub(self, ip)
        env.calls.append(ip)
        return RdapLookupResponse(
            rir="ripe",
            raw_response=ripe_rest.rdap_shape(
                rest_object(ip, start="0.0.0.0", end="255.255.255.255")
            ),
        )

    monkeypatch.setattr(RipeRestClient, "lookup_ip", rest)
    result = run(env, select(env, ["45.10.1.1", "5.9.9.9"]))
    assert result.success
    metadata = outcome(result)
    assert metadata["completion_status"] == "completed"
    assert metadata["reroutes_by_registry"] == {"ripe": 1}
    assert metadata["rdap_fallbacks_by_registry"] == {"ripe": 1}
    assert metadata["rdap_person_entities_by_registry"] == {"ripe:fallback": 2}
    assert metadata["rdap_requests_by_registry"] == {"arin": 1, "ripe": 3}
    assert metadata["pauses_by_registry"] == {}
    assert ripe + "45.10.1.1" not in fetched  # the rerouted body is never requested
    assert env.client.execute(
        "SELECT ip, rdap_lookup_status, rdap_rir FROM corpscout.ip_enrichment_current ORDER BY ip"
    ) == [("45.10.1.1", "found", "ripe"), ("5.9.9.9", "found", "ripe")]


def test_a_failure_mid_pass_still_stores_the_buffered_results(environment, monkeypatch):
    env = environment
    resolve = RdapEnricher.resolve_page
    pages = []

    def failing_second_page(self, rows):
        pages.append(rows)
        if len(pages) == 2:
            raise RuntimeError("resolver failed")
        return resolve(self, rows)

    monkeypatch.setattr(RdapEnricher, "resolve_page", failing_second_page)
    task = select(env, ["1.1.1.1", "8.8.8.8"])
    assert not run(env, task, batch_size=1).success
    # The first page was only buffered (1 row < 500, < 5 s) when the second failed.
    assert env.client.execute(
        "SELECT count() FROM corpscout.ip_enrichment_results FINAL"
    ) == [(1,)]
    monkeypatch.setattr(RdapEnricher, "resolve_page", resolve)
    assert run(env, task).success and len(env.calls) == 2


def test_remaining_pages_fill_across_buckets_and_never_repeat(monkeypatch):
    layout = {1: 7, 2: 2, 3: 4}
    entries = {
        bucket: [f"{bucket:03d}:{n}" for n in range(count)]
        for bucket, count in layout.items()
    }
    limits = []

    def remaining_entries(client, task, *, bucket, after=None, limit):
        limits.append(limit)
        ids = [i for i in entries[bucket] if after is None or i > after][:limit]
        return [{"input_id": i, "bucket": bucket} for i in ids]

    monkeypatch.setattr(results, "remaining_entries", remaining_entries)
    pages = list(results.remaining_pages(None, {}, sorted(layout), size=3))
    ids = [row["input_id"] for page in pages for row in page]
    assert ids == [i for bucket in sorted(layout) for i in entries[bucket]]
    assert len(ids) == len(set(ids)) == 13
    assert [len(page) for page in pages] == [3, 3, 3, 3, 1]
    assert [row["bucket"] for row in pages[2]] == [1, 2, 2]  # bucket 1 split, then 2
    assert all(0 < limit <= 3 for limit in limits)


def test_registry_usage_sql_charges_nir_answers_to_apnic(environment):
    env = environment
    normalized = normalize_rdap_network(
        replace(response("202.3.3.3"), rir="jpnic"),
        fetched_at=datetime.now(UTC) - timedelta(minutes=5),
        segment_role="lookup_result",
    )
    env.client.execute(
        RDAP_NETWORK_INSERT_SQL, [normalized.network.clickhouse_values()]
    )
    assert [rir for rir, _ in env.client.execute(results.REGISTRY_USAGE_SQL)] == [
        "apnic"
    ]


def test_failed_bootstrap_pauses_the_run_instead_of_storing_errors(
    environment, monkeypatch
):
    env = environment
    clock = {"now": 0.0}
    slept = []

    def fake_sleep(seconds):
        slept.append(seconds)
        clock["now"] += seconds

    monkeypatch.setattr(enrichment, "monotonic", lambda: clock["now"])
    monkeypatch.setattr(enrichment, "sleep", fake_sleep)
    failures = iter([True])

    def registry_for(self, ip):
        if next(failures, False):
            raise RdapClientError(
                "IANA unavailable", code="bootstrap_error", retryable=True
            )
        return "arin"

    monkeypatch.setattr(RdapClient, "registry_for", registry_for)
    result = run(env, select(env, ["8.8.8.8", "1.1.1.1"]), batch_size=1)
    assert result.success
    metadata = outcome(result)
    assert metadata["completion_status"] == "completed"
    assert metadata["pauses_by_registry"] == {"bootstrap": 1}
    assert metadata["rdap_deferrals_by_registry"] == {"bootstrap": 2}
    assert (metadata["budget_waits"], sum(slept)) == (1, pytest.approx(60))
    assert sorted(env.calls) == ["1.1.1.1", "8.8.8.8"]
    assert env.client.execute(
        "SELECT countIf(error_code = 'bootstrap_error') FROM corpscout.rdap_ip_lookup_results"
    ) == [(0,)]
    assert env.client.execute(
        "SELECT rdap_lookup_status, count() FROM corpscout.ip_enrichment_results FINAL GROUP BY 1"
    ) == [("found", 2)]


def resolver(env, *, started_at=None, cache_days=30, clock=None, sleep=None, **config):
    started = started_at or datetime.now(UTC)
    settings = IpEnrichmentResultsConfig(
        task_id=str(uuid4()),
        request_delay_seconds=0,
        rdap_cache_days=cache_days,
        **config,
    )
    return RdapEnricher(
        env.client,
        RdapClient(user_agent="test"),
        RipeRestClient(user_agent="test"),
        ApnicWhoisClient(),
        settings,
        SimpleNamespace(info=lambda *a: None, warning=lambda *a: None),
        started_at=started,
        cache_cutoff=started - timedelta(days=cache_days),
        clock=clock,
        sleep=sleep,
    )


def page(env, *ips):
    """Rows as the results loop would read them, with ClickHouse's own buckets."""
    buckets = dict(
        env.client.execute(
            "SELECT ip, toUInt16(cityHash64(ip) %% 256) FROM (SELECT arrayJoin(%(ips)s) AS ip)",
            {"ips": list(ips)},
        )
    )
    return [
        {
            "input_id": f"{buckets[ip]:03d}:{ip}",
            "ip": ip,
            "ip_version": 4 if "." in ip else 6,
            "bucket": buckets[ip],
        }
        for ip in ips
    ]


def seed_network(env, ip, *, fetched_at, **raw):
    """A reusable registration in the RDAP cache, visible to the trie after a reload."""
    normalized = normalize_rdap_network(
        response(ip, **raw), fetched_at=fetched_at, segment_role="lookup_result"
    )
    env.client.execute(
        RDAP_NETWORK_INSERT_SQL, [normalized.network.clickhouse_values()]
    )
    env.client.execute(
        RDAP_SEGMENT_INSERT_SQL, [s.clickhouse_values() for s in normalized.segments]
    )
    env.client.execute("SYSTEM RELOAD DICTIONARY corpscout.rdap_network_trie")
    return normalized


def query_kinds(queries):
    return {
        "negative": sum(
            q.lstrip().startswith("SELECT ip, lookup_status") for q in queries
        ),
        "trie": sum(
            "dictGetOrDefault('corpscout.rdap_network_trie'" in q for q in queries
        ),
        "networks": sum(
            q.lstrip().startswith("SELECT network_key, rir") for q in queries
        ),
        "markers": sum(
            q.lstrip().startswith("INSERT INTO corpscout.rdap_ip_lookup_results")
            for q in queries
        ),
        "context": sum(
            q.lstrip().startswith("SELECT ifNull((SELECT ready") for q in queries
        ),
        "results": sum(
            q.lstrip().startswith("INSERT INTO corpscout.ip_enrichment_results")
            for q in queries
        ),
    }


def test_page_resolution_uses_a_fixed_number_of_round_trips(environment, monkeypatch):
    env = environment
    seed_network(env, "8.8.8.1", fetched_at=datetime.now(UTC))
    queries = []
    execute = Client.execute
    monkeypatch.setattr(
        Client,
        "execute",
        lambda self, query, *a, **k: (
            queries.append(query),
            execute(self, query, *a, **k),
        )[1],
    )
    rows = page(env, *[f"8.8.8.{n}" for n in range(1, 41)], "127.0.0.1", "10.0.0.1")
    enricher = resolver(env)
    resolved = enricher.resolve_page(rows)
    assert env.calls == []  # every global address was a trie hit
    assert {ip: r["rdap_lookup_status"] for ip, r in resolved.items()} == {
        **{row["ip"]: "found" for row in rows[:40]},
        "127.0.0.1": "not_global",
        "10.0.0.1": "not_global",
    }
    assert resolved["8.8.8.40"]["rdap_matched_cidr"] == "8.8.8.0/24"
    assert query_kinds(queries) == {
        "negative": 1,
        "trie": 1,
        "networks": 1,
        "markers": 1,
        "context": 0,
        "results": 0,
    }
    assert not any("raw_response" in q for q in queries)
    # A second page of the same run hits the in-process network cache: no network read.
    queries.clear()
    assert (
        enricher.resolve_page(page(env, "8.8.8.7"))["8.8.8.7"]["rdap_lookup_status"]
        == "found"
    )
    assert query_kinds(queries)["networks"] == 0
    # A fresh resolver (a resume) reads the row once.
    resolver(env).resolve_page(page(env, "8.8.8.7"))
    assert query_kinds(queries)["networks"] == 1


def test_cache_window_is_the_frozen_execution_not_now(environment):
    env = environment
    started = datetime.now(UTC)
    seed_network(env, "8.8.8.1", fetched_at=started - timedelta(days=40))
    stale = resolver(env, started_at=started)  # 30-day window: the network is stale
    resolved = stale.resolve_page(page(env, "8.8.8.8"))
    assert (
        env.calls == ["8.8.8.8"]
        and resolved["8.8.8.8"]["rdap_lookup_status"] == "found"
    )
    assert stale.cache_hits == 0 and stale.requests == 1
    wide = resolver(env, started_at=started, cache_days=60)
    env.calls.clear()
    # A network fetched after started_at (by this or another execution) is still reusable.
    env.client.execute("SYSTEM RELOAD DICTIONARY corpscout.rdap_network_trie")
    assert (
        wide.resolve_page(page(env, "8.8.8.9"))["8.8.8.9"]["rdap_lookup_status"]
        == "found"
    )
    assert env.calls == [] and wide.cache_hits == 1


def test_negative_markers_are_honoured_by_the_frozen_start(environment, monkeypatch):
    env = environment
    started = datetime.now(UTC)

    def limited(self, ip):
        env.calls.append(ip)
        raise RdapClientError(
            "rate limit", code="rate_limited", retryable=True, status_code=429
        )

    monkeypatch.setattr(RdapClient, "lookup_ip", limited)
    first = resolver(
        env, started_at=started, rate_limit_retry_seconds=3600
    ).resolve_page(page(env, "8.8.8.8"))
    assert first["8.8.8.8"]["rdap_lookup_status"] == "retryable_error"
    assert env.calls == ["8.8.8.8"]
    # Still inside the backoff relative to a later execution's start: served from the marker.
    again = resolver(env, started_at=started + timedelta(minutes=5)).resolve_page(
        page(env, "8.8.8.8")
    )
    assert again["8.8.8.8"]["rdap_error_code"] == "rate_limited" and env.calls == [
        "8.8.8.8"
    ]
    # An execution that starts after retry_after asks again; force_rdap always asks.
    later = resolver(env, started_at=started + timedelta(hours=2)).resolve_page(
        page(env, "8.8.8.8")
    )
    assert later["8.8.8.8"][
        "rdap_lookup_status"
    ] == "retryable_error" and env.calls == ["8.8.8.8", "8.8.8.8"]
    resolver(env, started_at=started, force_rdap=True).resolve_page(
        page(env, "8.8.8.8")
    )
    assert env.calls == ["8.8.8.8"] * 3


def test_registry_level_response_answers_only_its_ip(environment, monkeypatch):
    env = environment
    seed_reference_data(env.client)  # ip_registry_ready = 1: classes are decided
    monkeypatch.setattr(
        RdapClient,
        "lookup_ip",
        lambda self, ip: (
            env.calls.append(ip),
            response(
                ip,
                start="103.0.0.0",
                end="103.255.255.255",
                handle="103.0.0.0 - 103.255.255.255",
                name="APNIC-AP",
            ),
        )[1],
    )
    first = resolver(env)
    resolved = first.resolve_page(page(env, "103.35.64.49"))
    assert resolved["103.35.64.49"]["rdap_lookup_status"] == "found"
    assert resolved["103.35.64.49"]["rdap_name"] == "APNIC-AP"
    assert resolved["103.35.64.49"]["rdap_matched_cidr"] == "103.0.0.0/8"
    assert first.registry_level_responses == 1
    # Stored with the ordinary segment role; its class row keeps it out of the trie.
    assert env.client.execute(
        "SELECT DISTINCT segment_role FROM corpscout.rdap_network_segments"
    ) == [("lookup_result",)]
    assert env.client.execute(
        "SELECT network_key, registry_class, covered_rir_blocks FROM corpscout.rdap_network_registry_class_current"
    ) == [("arin:103.0.0.0 - 103.255.255.255", "registry_level", 1)]
    env.client.execute("SYSTEM RELOAD DICTIONARY corpscout.rdap_network_trie")
    assert env.client.execute(
        "SELECT count() FROM corpscout.rdap_network_segments_current"
    ) == [(0,)]
    # Another address in the block is not served by the trie nor by the in-run cache.
    second = first.resolve_page(page(env, "103.15.66.50"))
    assert second["103.15.66.50"]["rdap_lookup_status"] == "found"
    assert env.calls == ["103.35.64.49", "103.15.66.50"]
    # The queried address itself is served by its own lookup marker next time.
    third = resolver(env)
    assert (
        third.resolve_page(page(env, "103.35.64.49"))["103.35.64.49"]["rdap_name"]
        == "APNIC-AP"
    )
    assert env.calls == ["103.35.64.49", "103.15.66.50"] and third.cache_hits == 1
    # A holder registration is classified reusable and serves its neighbours.
    monkeypatch.setattr(
        RdapClient,
        "lookup_ip",
        lambda self, ip: (
            env.calls.append(ip),
            response(ip, start="103.35.64.0", end="103.35.67.255"),
        )[1],
    )
    holder = resolver(env)
    assert (
        holder.resolve_page(page(env, "103.35.64.1", "103.35.64.2"))["103.35.64.2"][
            "rdap_matched_cidr"
        ]
        == "103.35.64.0/22"
    )
    assert (
        env.calls[-1] == "103.35.64.1"
        and holder.requests == 1
        and holder.cache_hits == 1
    )
    assert env.client.execute(
        "SELECT registry_class FROM corpscout.rdap_network_registry_class_current WHERE network_key = 'arin:TEST-103.35.64.1'"
    ) == [("reusable",)]


def test_misses_reuse_networks_fetched_earlier_in_the_run_and_stop_at_the_budget(
    environment, monkeypatch
):
    env = environment
    queries = []
    execute = Client.execute
    monkeypatch.setattr(
        Client,
        "execute",
        lambda self, query, *a, **k: (
            queries.append(query),
            execute(self, query, *a, **k),
        )[1],
    )
    enricher = resolver(env, max_requests=1)
    resolved = enricher.resolve_page(page(env, "8.8.8.8", "8.8.8.9", "1.1.1.1"))
    assert env.calls == ["8.8.8.8"]
    assert (
        resolved["8.8.8.9"]["rdap_matched_cidr"] == "8.8.8.0/24"
    )  # in-run reuse, no HTTP
    assert "1.1.1.1" not in resolved and enricher.budget_reached
    assert enricher.cache_hits == 1 and enricher.requests == 1
    assert (
        query_kinds(queries)["context"] == 1
    )  # one classification round trip per miss
    # The page's markers were written for what was resolved.
    assert env.client.execute(
        "SELECT ip, lookup_status FROM corpscout.rdap_ip_lookup_results_current ORDER BY ip"
    ) == [("8.8.8.8", "found")]


def test_ripe_addresses_use_the_rest_api_and_carry_no_person_data(
    environment, monkeypatch
):
    env = environment
    rdap_calls = []
    monkeypatch.setattr(
        RdapClient,
        "lookup_ip",
        lambda self, ip: (rdap_calls.append(ip), response(ip))[1],
    )
    enricher = resolver(env)
    resolved = enricher.resolve_page(page(env, "5.1.1.1", "5.1.1.2"))
    assert (
        env.calls == ["5.1.1.1"] and rdap_calls == []
    )  # REST once, the neighbour from the in-run cache
    found = resolved["5.1.1.2"]
    assert (
        found["rdap_rir"],
        found["rdap_name"],
        found["rdap_registration_type"],
        found["rdap_country_code"],
    ) == ("ripe", "RIPE-TEST-NET", "ASSIGNED PA", "SE")
    assert (
        found["rdap_handle"] == "5.1.1.0 - 5.1.1.255"
        and found["rdap_network_key"] == "ripe:5.1.1.0 - 5.1.1.255"
    )
    assert (
        found["rdap_registrant_handles"] == ["ORG-TEST1-RIPE"]
        and found["rdap_registrant_names"] == []
    )
    assert (
        found["rdap_self_url"]
        == "https://rest.db.ripe.net/ripe/inetnum/5.1.1.0%20-%205.1.1.255"
    )
    assert (
        enricher.person_entities_by_registry == {}
        and enricher.requests_by_registry == {"ripe": 1}
    )
    [(raw,)] = env.client.execute("SELECT raw_response FROM corpscout.rdap_networks")
    assert "descr" not in raw and "admin-c" not in raw and '"source":"ripe-rest"' in raw
    # The root object for unallocated space is a catch-all: one RDAP request follows, once.
    monkeypatch.setattr(
        RipeRestClient,
        "lookup_ip",
        lambda self, ip: (
            env.calls.append(ip),
            RdapLookupResponse(
                rir="ripe",
                raw_response=ripe_rest.rdap_shape(
                    rest_object(
                        ip, start="0.0.0.0", end="255.255.255.255", netname="IANA-BLK"
                    )
                ),
            ),
        )[1],
    )
    resolved = enricher.resolve_page(page(env, "5.9.9.9"))
    assert env.calls == ["5.1.1.1", "5.9.9.9"] and rdap_calls == ["5.9.9.9"]
    assert (
        resolved["5.9.9.9"]["rdap_lookup_status"] == "found"
        and resolved["5.9.9.9"]["rdap_rir"] == "arin"
    )
    # ripe_rest=false keeps RDAP for RIPE addresses.
    plain = resolver(env, ripe_rest=False)
    plain.resolve_page(page(env, "5.7.7.7"))
    assert rdap_calls == ["5.9.9.9", "5.7.7.7"]


def test_rdap_shape_builds_an_ip_network_without_contacts():
    shaped = ripe_rest.rdap_shape(rest_object("5.1.1.1"))
    assert shaped["objectClassName"] == "ip network" and shaped["ipVersion"] == "v4"
    assert (shaped["handle"], shaped["startAddress"], shaped["endAddress"]) == (
        "5.1.1.0 - 5.1.1.255",
        "5.1.1.0",
        "5.1.1.255",
    )
    assert (shaped["name"], shaped["type"], shaped["country"], shaped["status"]) == (
        "RIPE-TEST-NET",
        "ASSIGNED PA",
        "SE",
        ["active"],
    )
    assert shaped["entities"] == [
        {
            "objectClassName": "entity",
            "handle": "ORG-TEST1-RIPE",
            "roles": ["registrant"],
        }
    ]
    assert shaped["events"] == [
        {"eventAction": "registration", "eventDate": "2010-05-04T10:00:00Z"},
        {"eventAction": "last changed", "eventDate": "2024-01-02T03:04:05Z"},
    ]
    assert shaped["corpscout"] == {
        "source": "ripe-rest",
        "flags": "no-referenced",
        "mnt_by": ["TEST-MNT"],
    }
    assert "descr" not in str(shaped) and "admin-c" not in str(shaped)
    six = ripe_rest.rdap_shape(
        {
            "type": "inet6num",
            "attributes": {
                "attribute": [
                    {"name": "inet6num", "value": "2001:638:501::/48"},
                    {"name": "netname", "value": "UNI-ESSEN"},
                    {"name": "status", "value": "ASSIGNED"},
                ]
            },
        }
    )
    assert (
        six["startAddress"],
        six["endAddress"],
        six["ipVersion"],
        six["entities"],
        six["events"],
    ) == ("2001:638:501::", "2001:638:501:ffff:ffff:ffff:ffff:ffff", "v6", [], [])
    assert ripe_rest.rdap_shape(rest_object("5.1.1.1", org=None))["entities"] == []
    with pytest.raises(ValueError, match="not a network object"):
        ripe_rest.rdap_shape(
            {
                "type": "route",
                "attributes": {"attribute": [{"name": "route", "value": "5.0.0.0/8"}]},
            }
        )


def test_ripe_rest_client_maps_answers_and_errors(monkeypatch):
    class Response:
        def __init__(self, status, payload=None):
            self.status_code = status
            self._payload = payload

        def json(self):
            if self._payload is None:
                raise ValueError("no json")
            return self._payload

    seen = []

    class Session:
        headers = {}

        def get(self, url, *, params, headers, timeout):
            seen.append((url, params, headers["Accept"]))
            return answers.pop(0)

    client = RipeRestClient(user_agent="test", session=Session())
    answers = [
        Response(
            200,
            {
                "objects": {
                    "object": [
                        {"type": "route", "attributes": {"attribute": []}},
                        rest_object("5.1.1.1"),
                    ]
                }
            },
        )
    ]
    found = client.lookup_ip("5.1.1.1")
    assert found.rir == "ripe" and found.raw_response["handle"] == "5.1.1.0 - 5.1.1.255"
    assert seen == [
        (
            ripe_rest.SEARCH_URL,
            [
                ("query-string", "5.1.1.1"),
                ("flags", "no-referenced"),
                ("flags", "no-personal"),
                ("source", "ripe"),
                ("type-filter", "inetnum"),
                ("type-filter", "inet6num"),
            ],
            "application/json",
        )
    ]
    for status, payload, code, retryable in [
        (404, None, "not_found", False),
        (429, None, "rate_limited", True),
        (503, None, "remote_server", True),
        (403, None, "access_denied", True),  # a source-address block: back off, retry
        (418, None, "query_error", False),
        (200, None, "invalid_response", False),
        (200, ["not", "an", "object"], "invalid_response", False),
        (200, {"objects": None}, "invalid_response", False),
        (200, {"objects": {"object": None}}, "invalid_response", False),
        (200, {"objects": {"object": []}}, "not_found", False),
    ]:
        answers = [Response(status, payload)]
        with pytest.raises(RdapClientError) as raised:
            client.lookup_ip("5.1.1.1")
        assert (raised.value.code, raised.value.retryable) == (code, retryable), status


def test_apnic_addresses_use_whois_r_and_take_the_holder_from_the_first_descr(
    environment, monkeypatch
):
    env = environment
    rdap_calls = []
    monkeypatch.setattr(
        RdapClient,
        "lookup_ip",
        lambda self, ip: (rdap_calls.append(ip), response(ip))[1],
    )
    monkeypatch.setattr(RdapClient, "registry_for", lambda self, ip: "apnic")
    monkeypatch.setattr(
        ApnicWhoisClient,
        "query",
        lambda self, ip: (env.calls.append(ip), APNIC_FPT_ANSWER)[1],
    )
    enricher = resolver(env)
    resolved = enricher.resolve_page(page(env, "103.35.64.49", "103.35.65.7"))
    assert (
        env.calls == ["103.35.64.49"] and rdap_calls == []
    )  # whois once, the neighbour from the in-run cache
    found = resolved["103.35.65.7"]
    assert (
        found["rdap_rir"],
        found["rdap_name"],
        found["rdap_registration_type"],
        found["rdap_country_code"],
    ) == ("apnic", "FPT-VN", "ALLOCATED PORTABLE", "VN")
    assert (
        found["rdap_handle"] == "103.35.64.0 - 103.35.67.255"
        and found["rdap_matched_cidr"] == "103.35.64.0/22"
    )
    assert (
        found["rdap_registrant_names"] == ["FPT Telecom"]
        and found["rdap_registrant_handles"] == []
    )
    assert found["rdap_self_url"] == "https://rdap.apnic.net/ip/103.35.64.0"
    assert found["rdap_last_changed_at"] == datetime(
        2017, 11, 19, 8, 36, 30, tzinfo=UTC
    )
    assert (
        enricher.person_entities_by_registry == {}
        and enricher.requests_by_registry == {"apnic": 1}
    )
    [(raw,)] = env.client.execute("SELECT raw_response FROM corpscout.rdap_networks")
    assert (
        "FHIG1-AP" not in raw
        and "Duy Tan" not in raw
        and '"source":"apnic-whois"' in raw
    )


def test_apnic_nir_allocation_objects_fall_back_to_rdap(environment, monkeypatch):
    env = environment
    rdap_calls = []
    monkeypatch.setattr(
        RdapClient,
        "lookup_ip",
        lambda self, ip: (rdap_calls.append(ip), response(ip))[1],
    )
    monkeypatch.setattr(
        ApnicWhoisClient,
        "query",
        lambda self, ip: (env.calls.append(ip), APNIC_JPNIC_ANSWER)[1],
    )
    resolved = resolver(env).resolve_page(
        page(env, "202.12.14.5")
    )  # 202/8 is APNIC in the fixture
    assert env.calls == ["202.12.14.5"] and rdap_calls == ["202.12.14.5"]
    assert (
        resolved["202.12.14.5"]["rdap_lookup_status"] == "found"
        and resolved["202.12.14.5"]["rdap_rir"] == "arin"
    )
    # The rule: the NIR's own object, never the maintainer. FPT's /22 is maintained by VNNIC and is a holder's.
    fpt = apnic_whois.network_object(apnic_whois.parse_answer(APNIC_FPT_ANSWER))
    assert apnic_whois.nir_of(
        "FPT-VN", "FPT Telecom"
    ) == "" and not apnic_whois.is_nir_object(apnic_whois.rdap_shape(fpt))
    assert apnic_whois.is_nir_object(
        apnic_whois.rdap_shape(
            apnic_whois.network_object(apnic_whois.parse_answer(APNIC_JPNIC_ANSWER))
        )
    )
    assert (
        apnic_whois.nir_of("JPNIC-NET-JP-ERX", "Japan Network Information Center")
        == "jpnic"
    )
    # NIR-style netnames on members' own objects are holders (seen on prod 2026-09-26).
    assert apnic_whois.nir_of("IDNIC-TADULAKO-ID", "Universitas Tadulako") == ""
    assert apnic_whois.nir_of("JPNIC-NET-JP", "") == ""
    assert apnic_whois.nir_of("KIDC", "Korea Internet Data Center") == ""
    assert apnic_whois.nir_of("SOLUSINET-ID", "PT iForte Global Internet") == ""
    assert (
        apnic_whois.nir_of("CIDR-BLK3-TW", "Taiwan Network Information Center")
        == "twnic"
    )
    assert apnic_whois.nir_of("KORNET", "Korea Telecom") == ""
    assert apnic_whois.nir_of("MEGAEGG", "MEGA EGG") == ""
    # apnic_whois=false keeps RDAP for APNIC addresses.
    resolver(env, apnic_whois=False).resolve_page(page(env, "202.12.9.9"))
    assert env.calls == ["202.12.14.5"] and rdap_calls == ["202.12.14.5", "202.12.9.9"]


def test_apnic_whois_client_parses_answers_and_maps_errors():
    sent = []

    class Socket:
        def __init__(self, answer):
            self.answer = answer.encode()

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def settimeout(self, seconds):
            pass

        def sendall(self, data):
            sent.append(data)

        def recv(self, size):
            chunk, self.answer = self.answer[:size], self.answer[size:]
            return chunk

    answers = []

    def connect(address, timeout):
        assert address == ("whois.apnic.net", 43) and timeout == 10.0
        answer = answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return Socket(answer)

    client = ApnicWhoisClient(connect=connect)
    answers = [APNIC_FPT_ANSWER]
    found = client.lookup_ip("103.35.64.49")
    assert sent == [b"-r 103.35.64.49\r\n"]
    assert (
        found.rir == "apnic"
        and found.raw_response["handle"] == "103.35.64.0 - 103.35.67.255"
    )
    assert (
        found.raw_response["name"] == "FPT-VN"
        and found.raw_response["type"] == "ALLOCATED PORTABLE"
    )
    assert found.raw_response["entities"] == [
        {
            "objectClassName": "entity",
            "handle": None,
            "roles": ["registrant"],
            "vcardArray": [
                "vcard",
                [
                    ["version", {}, "text", "4.0"],
                    ["kind", {}, "text", "org"],
                    ["fn", {}, "text", "FPT Telecom"],
                ],
            ],
        }
    ]
    assert found.raw_response["corpscout"] == {
        "source": "apnic-whois",
        "flags": "-r",
        "nir": "",
        "mnt_by": ["MAINT-VN-VNNIC"],
        "mnt_irt": ["IRT-VNNIC-AP"],
    }
    assert "FHIG1-AP" not in str(found.raw_response) and "Duy Tan" not in str(
        found.raw_response
    )
    # Status spellings of the dump are normalised; inet6num ranges are prefixes.
    six = apnic_whois.rdap_shape(
        apnic_whois.network_object(
            apnic_whois.parse_answer(
                "inet6num:       2001:200::/35\nnetname:        WIDE-JP\ndescr:          WIDE Project\nstatus:         Allocated non-portable\nsource:         APNIC\n"
            )
        )
    )
    assert (six["startAddress"], six["endAddress"], six["ipVersion"], six["type"]) == (
        "2001:200::",
        "2001:200:1fff:ffff:ffff:ffff:ffff:ffff",
        "v6",
        "ALLOCATED NON-PORTABLE",
    )
    assert (
        apnic_whois.rdap_shape(
            [
                ("inetnum", "1.0.0.0 - 1.0.0.255"),
                ("netname", "X"),
                ("status", "ASSIGNED  NON-PORTABLE"),
            ]
        )["type"]
        == "ASSIGNED NON-PORTABLE"
    )
    with pytest.raises(ValueError, match="not a network object"):
        apnic_whois.rdap_shape([("route", "1.0.0.0/24")])
    footer = "\n% This query was served by the APNIC Whois Service version 1.88.48 (WHOIS-UK2)\n"
    for answer, code, retryable in [
        ("%ERROR:101: no entries found\n" + footer, "not_found", False),
        ("%ERROR:201: access denied\n" + footer, "rate_limited", True),
        ("%ERROR:208: too many connections\n" + footer, "rate_limited", True),
        ("%ERROR:305: connection limit\n", "query_error", True),
        (
            "route:          1.0.0.0/24\norigin:         AS1\nsource:         APNIC\n"
            + footer,
            "not_found",
            False,
        ),
        # Nothing, or an answer cut off before the footer, is retried, never "no object".
        ("", "transport_error", True),
        (
            APNIC_FPT_ANSWER.split("% Information related to '103.35.64.0/24")[0][:300],
            "transport_error",
            True,
        ),
        (OSError("connection refused"), "transport_error", True),
    ]:
        answers = [answer]
        with pytest.raises(RdapClientError) as raised:
            client.lookup_ip("1.0.0.1")
        assert (raised.value.code, raised.value.retryable) == (code, retryable), answer


def test_person_entities_counts_person_and_role_vcards_nested_included():
    assert enrichment.person_entities({}) == 0
    raw = {
        "entities": [
            {
                "objectClassName": "entity",
                "handle": "ORG-A",
                "roles": ["registrant"],
                "vcardArray": [
                    "vcard",
                    [["kind", {}, "text", "org"], ["fn", {}, "text", "A GmbH"]],
                ],
            },
            {
                "objectClassName": "entity",
                "handle": "P1",
                "vcardArray": [
                    "vcard",
                    [
                        ["kind", {}, "text", "individual"],
                        ["fn", {}, "text", "A Person"],
                    ],
                ],
                "entities": [
                    {
                        "objectClassName": "entity",
                        "handle": "P2",
                        "vcardArray": ["vcard", [["kind", {}, "text", "individual"]]],
                    }
                ],
            },
            {
                "objectClassName": "entity",
                "handle": "R1",
                "vcardArray": ["vcard", [["kind", {}, "text", "group"]]],
            },
        ]
    }
    # P1, P2 (persons) and R1 (a role object); the org registrant is not personal data.
    assert enrichment.person_entities(raw) == 3
    # A role-only answer (common for large ISPs in RIPE's RDAP) still shows up.
    role_only = {
        "entities": [
            {
                "objectClassName": "entity",
                "handle": "NOC-RIPE",
                "roles": ["abuse", "technical"],
                "vcardArray": [
                    "vcard",
                    [["version", {}, "text", "4.0"], ["kind", {}, "text", "group"]],
                ],
            }
        ]
    }
    assert enrichment.person_entities(role_only) == 1


def test_registry_budget_defers_misses_and_frees_after_the_window(environment):
    env = environment
    clock = {"now": 1000.0}
    slept = []

    def sleep(seconds):
        slept.append(seconds)
        clock["now"] += seconds

    enricher = resolver(
        env, registry_daily_budgets={"arin": 2}, clock=lambda: clock["now"], sleep=sleep
    )
    resolved = enricher.resolve_page(
        page(env, "8.8.8.8", "8.8.4.4", "1.1.1.1", "5.1.1.1")
    )
    assert env.calls == [
        "8.8.8.8",
        "8.8.4.4",
        "5.1.1.1",
    ]  # the third ARIN miss is deferred, RIPE is not budgeted
    assert "1.1.1.1" not in resolved and not enricher.budget_reached
    assert enricher.deferred == {"arin": 1} and enricher.deferrals_by_registry == {
        "arin": 1
    }
    assert enricher.requests_by_registry == {"arin": 2, "ripe": 1}
    assert (
        enricher.person_entities_by_registry == {}
    )  # the test responses carry no vCards
    assert enricher.seconds_until_budget_frees() == pytest.approx(86_400)
    clock["now"] += 3600
    assert enricher.resolve_page(page(env, "1.1.1.1")) == {} and enricher.deferred == {
        "arin": 2
    }
    assert enricher.wait_for_registry_budget() == pytest.approx(86_400 - 3600)
    assert enricher.deferred == {} and sum(slept) == pytest.approx(86_400 - 3600)
    assert (
        enricher.resolve_page(page(env, "1.1.1.1"))["1.1.1.1"]["rdap_lookup_status"]
        == "found"
    )
    # Requests other runs made in the last day count against the same window.
    fresh = resolver(
        env, registry_daily_budgets={"arin": 2}, clock=lambda: clock["now"]
    )
    fresh.seed_registry_usage([("arin", 100.0), ("arin", 50.0), ("ripe", 10.0)])
    assert fresh.resolve_page(page(env, "9.9.9.9")) == {} and fresh.deferred == {
        "arin": 1
    }
    assert (
        fresh.resolve_page(page(env, "5.4.4.4"))["5.4.4.4"]["rdap_lookup_status"]
        == "found"
    )


def test_registry_for_maps_the_bootstrap_endpoint_to_the_registry_name(monkeypatch):
    client = rdap_client.RdapClient(user_agent="test")
    monkeypatch.setattr(client, "_ensure_bootstrapped", lambda: None)
    urls = {
        "5.134.16.1": "https://rdap.db.ripe.net/ip/5.134.16.1",
        "8.8.8.8": "https://rdap.arin.net/registry/ip/8.8.8.8",
        "2001:db8::1": "https://unknown.example/ip/2001:db8::1",
    }
    monkeypatch.setattr(
        rdap_client.whoisit,
        "build_query",
        lambda *, query_type, query_value: ("GET", urls[query_value], True),
    )
    assert client.registry_for("5.134.16.1") == "ripe"
    assert client.registry_for("8.8.8.8") == "arin"
    assert client.registry_for("2001:db8::1") == ""
    # No exact bootstrap match: whoisit would pick a random registry; '' instead.
    monkeypatch.setattr(
        rdap_client.whoisit,
        "build_query",
        lambda *, query_type, query_value: ("GET", urls["5.134.16.1"], False),
    )
    assert client.registry_for("5.134.16.1") == ""

    def refused(**kwargs):
        raise rdap_client.QueryError(
            "You need to load bootstrap data before making any queries"
        )

    monkeypatch.setattr(rdap_client.whoisit, "build_query", refused)
    assert client.registry_for("8.8.8.8") == ""
    assert rdap_client.RIR_BY_HOST["rdap.db.ripe.net"] == "ripe"


def ripe_rdap_body(ip, persons=2):
    """RIPE's RDAP answer for a network, with person objects embedded as RIPE sends them."""
    start = ip.rsplit(".", 2)[0] + ".0.0"
    end = ip.rsplit(".", 2)[0] + ".255.255"
    return {
        "objectClassName": "ip network",
        "handle": f"{start} - {end}",
        "startAddress": start,
        "endAddress": end,
        "ipVersion": "v4",
        "name": "RIPE-RDAP-NET",
        "type": "ASSIGNED PA",
        "country": "NL",
        "status": ["active"],
        "links": [{"rel": "self", "href": f"https://rdap.db.ripe.net/ip/{start}/16"}],
        "entities": [
            {
                "objectClassName": "entity",
                "handle": f"P{n}-RIPE",
                "roles": ["administrative"],
                "vcardArray": [
                    "vcard",
                    [
                        ["kind", {}, "text", "individual"],
                        ["fn", {}, "text", f"Person {n}"],
                    ],
                ],
            }
            for n in range(persons)
        ],
    }


class HttpAnswer:
    """What whoisit's http_request returns: status, headers, JSON body."""

    def __init__(self, status, *, location=None, body=None):
        self.status_code = status
        self.headers = {"Location": location} if location else {}
        self._body = body
        self.text = ""

    def close(self):
        pass

    def json(self):
        return self._body


def fake_rdap_http(monkeypatch, routes, *, base="https://rdap.arin.net/registry/ip/"):
    """Route RdapClient's HTTP through ``routes`` (url -> HttpAnswer); returns fetched URLs.

    whoisit's bootstrap is stubbed to send every query to ``base``.
    """
    fetched = []

    def http_request(session, url, **kwargs):
        assert kwargs == {"allow_redirects": False}
        fetched.append(url)
        return routes[url]

    def whoisit_ip(query, *, rir, include_raw, session):
        # whoisit's own request (no rerouted hosts): follows redirects itself.
        url = f"{base}{query}"
        for _ in range(rdap_client.MAX_REDIRECTS + 1):
            fetched.append(url)
            answer = routes[url]
            if answer.status_code not in rdap_client.REDIRECT_STATUSES:
                break
            url = answer.headers["Location"]
        host = url.split("/")[2]
        return {"rir": rdap_client.RIR_BY_HOST[host], "raw": answer.json()}

    monkeypatch.setattr(rdap_client, "http_request", http_request)
    monkeypatch.setattr(rdap_client.whoisit, "ip", whoisit_ip)
    monkeypatch.setattr(RdapClient, "_ensure_bootstrapped", lambda self: None)
    monkeypatch.setattr(
        rdap_client.whoisit,
        "build_query",
        lambda *, query_type, query_value, rir=None: (
            "GET",
            f"{base}{query_value}",
            True,
        ),
    )
    return fetched


def test_rdap_client_follows_redirects_by_hand_and_refuses_rerouted_hosts(monkeypatch):
    arin = "https://rdap.arin.net/registry/ip/45.10.1.1"
    ripe = "https://rdap.db.ripe.net/ip/45.10.1.1"
    fetched = fake_rdap_http(
        monkeypatch,
        {
            arin: HttpAnswer(301, location=ripe),
            ripe: HttpAnswer(200, body=ripe_rdap_body("45.10.1.1")),
        },
    )
    guarded = RdapClient(user_agent="test", reroute_hosts={"rdap.db.ripe.net"})
    with pytest.raises(rdap_client.RdapRedirect) as raised:
        guarded.lookup_ip("45.10.1.1")
    assert (raised.value.registry, raised.value.location) == ("ripe", ripe)
    assert fetched == [arin]  # RIPE's body, with its person objects, is never fetched
    # A redirect elsewhere is followed by hand.
    lacnic = "https://rdap.lacnic.net/rdap/ip/45.10.1.1"
    fetched = fake_rdap_http(
        monkeypatch,
        {
            arin: HttpAnswer(301, location=lacnic),
            lacnic: HttpAnswer(200, body=ripe_rdap_body("45.10.1.1")),
        },
    )
    assert guarded.lookup_ip("45.10.1.1").raw_response["name"] == "RIPE-RDAP-NET"
    assert fetched == [arin, lacnic]
    # A redirect loop stops after MAX_REDIRECTS hops.
    fetched = fake_rdap_http(monkeypatch, {arin: HttpAnswer(302, location=arin)})
    with pytest.raises(RdapClientError) as loop:
        guarded.lookup_ip("45.10.1.1")
    assert loop.value.code == "query_error" and len(fetched) == 6
    # Without rerouted hosts, whoisit's own request is used (as before this change).
    fetched = fake_rdap_http(
        monkeypatch,
        {
            arin: HttpAnswer(301, location=ripe),
            ripe: HttpAnswer(200, body=ripe_rdap_body("45.10.1.1")),
        },
    )
    assert RdapClient(user_agent="test").lookup_ip("45.10.1.1").rir == "ripe"
    assert fetched == [arin, ripe]


def test_redirects_to_ripe_are_answered_by_rest_and_fallbacks_are_counted_apart(
    environment, monkeypatch
):
    env = environment
    arin = "https://rdap.arin.net/registry/ip/45.10.1.1"
    ripe = "https://rdap.db.ripe.net/ip/45.10.1.1"
    monkeypatch.setattr(RdapClient, "lookup_ip", REAL_RDAP_LOOKUP_IP)
    fetched = fake_rdap_http(
        monkeypatch,
        {
            arin: HttpAnswer(301, location=ripe),
            ripe: HttpAnswer(200, body=ripe_rdap_body("45.10.1.1", persons=3)),
            arin.replace("1.1", "2.2"): HttpAnswer(
                301, location=ripe.replace("1.1", "2.2")
            ),
            ripe.replace("1.1", "2.2"): HttpAnswer(
                200, body=ripe_rdap_body("45.10.2.2", persons=3)
            ),
        },
    )
    enricher = resolver(env)  # registry_for says arin for 45/8 (the fixture)
    resolved = enricher.resolve_page(page(env, "45.10.1.1"))
    assert fetched == [arin] and env.calls == ["45.10.1.1"]  # REST answered
    found = resolved["45.10.1.1"]
    assert (found["rdap_rir"], found["rdap_network_key"]) == (
        "ripe",
        "ripe:45.10.1.0 - 45.10.1.255",
    )
    assert enricher.reroutes_by_registry == {"ripe": 1}
    assert enricher.requests_by_registry == {"arin": 1, "ripe": 1}
    assert enricher.person_entities_by_registry == {}
    # A catch-all REST answer falls back to RDAP: that answer is counted apart.
    monkeypatch.setattr(
        RipeRestClient,
        "lookup_ip",
        lambda self, ip: (
            env.calls.append(ip),
            RdapLookupResponse(
                rir="ripe",
                raw_response=ripe_rest.rdap_shape(
                    rest_object(ip, start="0.0.0.0", end="255.255.255.255")
                ),
            ),
        )[1],
    )
    fetched.clear()
    fallback = enricher.resolve_page(page(env, "45.10.2.2"))
    assert fallback["45.10.2.2"]["rdap_lookup_status"] == "found"
    assert fetched == [
        "https://rdap.arin.net/registry/ip/45.10.2.2",  # redirect refused, REST asked
        "https://rdap.arin.net/registry/ip/45.10.2.2",  # the fallback follows it
        "https://rdap.db.ripe.net/ip/45.10.2.2",
    ]
    assert env.calls == ["45.10.1.1", "45.10.2.2"]
    assert enricher.rdap_fallbacks_by_registry == {"ripe": 1}
    assert enricher.person_entities_by_registry == {"ripe:fallback": 3}
    assert "ripe" not in enricher.person_entities_by_registry


def test_rate_limited_registry_is_paused_and_its_misses_deferred(
    environment, monkeypatch
):
    env = environment
    clock = {"now": 5000.0}

    def limited(self, ip):
        env.calls.append(ip)
        raise RdapClientError(
            "rate limit", code="rate_limited", retryable=True, status_code=429
        )

    monkeypatch.setattr(RdapClient, "lookup_ip", limited)
    enricher = resolver(env, clock=lambda: clock["now"])
    resolved = enricher.resolve_page(page(env, "8.8.8.8", "1.1.1.1", "5.1.1.1"))
    assert resolved["8.8.8.8"]["rdap_error_code"] == "rate_limited"
    assert "1.1.1.1" not in resolved  # deferred: no result, no marker
    assert resolved["5.1.1.1"]["rdap_lookup_status"] == "found"  # RIPE is not paused
    assert env.calls == ["8.8.8.8", "5.1.1.1"]
    assert enricher.deferred == {"arin": 1} and enricher.pauses_by_registry == {
        "arin": 1
    }
    assert env.client.execute(
        "SELECT ip FROM corpscout.rdap_ip_lookup_results_current ORDER BY ip"
    ) == [("5.1.1.1",), ("8.8.8.8",)]
    assert enricher.seconds_until_budget_frees() == pytest.approx(3600)
    clock["now"] += 3600
    monkeypatch.setattr(RdapClient, "lookup_ip", lambda self, ip: response(ip))
    enricher.reset_pass()
    assert (
        enricher.resolve_page(page(env, "1.1.1.1"))["1.1.1.1"]["rdap_lookup_status"]
        == "found"
    )
    # RIPE REST 403 (a source-address block) pauses RIPE for at least 15 minutes.
    monkeypatch.setattr(
        RipeRestClient,
        "lookup_ip",
        lambda self, ip: (_ for _ in ()).throw(
            RdapClientError(
                "blocked", code="access_denied", retryable=True, status_code=403
            )
        ),
    )
    blocked = enricher.resolve_page(page(env, "5.2.2.2", "5.3.3.3"))
    assert blocked["5.2.2.2"]["rdap_lookup_status"] == "retryable_error"
    assert "5.3.3.3" not in blocked and enricher.deferred == {"ripe": 1}
    assert enricher.seconds_until_budget_frees() == pytest.approx(900)


def test_bootstrap_failure_pauses_every_miss_with_back_off(environment, monkeypatch):
    env = environment
    clock = {"now": 0.0}
    attempts = []
    available = {"ok": False}

    def registry_for(self, ip):
        attempts.append(ip)
        if not available["ok"]:
            raise RdapClientError(
                "no bootstrap", code="bootstrap_transport_error", retryable=True
            )
        return "arin"

    monkeypatch.setattr(RdapClient, "registry_for", registry_for)
    enricher = resolver(env, clock=lambda: clock["now"])
    # The first miss fails the bootstrap; the rest of the page is deferred without
    # asking again, and no address gets a result or a marker.
    assert enricher.resolve_page(page(env, "8.8.8.8", "1.1.1.1")) == {}
    assert attempts == ["8.8.8.8"] and enricher.deferred == {"bootstrap": 2}
    assert enricher.seconds_until_budget_frees() == pytest.approx(60)
    assert env.client.execute(
        "SELECT count() FROM corpscout.rdap_ip_lookup_results"
    ) == [(0,)]
    clock["now"] = 60.0
    enricher.reset_pass()
    assert enricher.resolve_page(page(env, "8.8.8.8")) == {}
    assert enricher.seconds_until_budget_frees() == pytest.approx(120)  # doubled
    clock["now"] = 180.0 + 20 * 900  # far later: the cap is 15 minutes
    for _ in range(5):
        enricher.reset_pass()
        enricher.resolve_page(page(env, "8.8.8.8"))
        clock["now"] += enricher.seconds_until_budget_frees()
    assert enricher.pauses_by_registry == {"bootstrap": 7}
    enricher.reset_pass()
    enricher.resolve_page(page(env, "8.8.8.8"))
    assert enricher.seconds_until_budget_frees() == pytest.approx(900)
    clock["now"] += 900
    available["ok"] = True
    enricher.reset_pass()
    resolved = enricher.resolve_page(page(env, "8.8.8.8"))
    assert resolved["8.8.8.8"]["rdap_lookup_status"] == "found"
    assert enricher._bootstrap_failures == 0


def test_budget_keys_are_registry_names_and_the_cache_keeps_a_page(environment):
    task = str(uuid4())
    with pytest.raises(ValueError, match="unknown registry 'ripencc'"):
        IpEnrichmentResultsConfig(task_id=task, registry_daily_budgets={"ripencc": 5})
    assert IpEnrichmentResultsConfig(
        task_id=task, registry_daily_budgets={" RIPE ": 5}
    ).registry_daily_budgets == {"ripe": 5}
    assert resolver(environment, batch_size=5000)._cached_cap == 10_000
    assert resolver(environment)._cached_cap == 4096
    # Unbudgeted registries keep no send times.
    enricher = resolver(environment, registry_daily_budgets={"arin": 3})
    enricher.seed_registry_usage([("ripe", 5.0), ("arin", 5.0)])
    assert set(enricher._sent) == {"arin"}


def arin_rdap_body(ip, *, up=None):
    """ARIN's RDAP answer for the /24 around ``ip``, optionally with an up link."""
    body = dict(response(ip).raw_response)
    links = [{"rel": "self", "href": f"https://rdap.arin.net/registry/ip/{ip}"}]
    if up:
        links.append({"rel": "up", "href": up})
    body["links"] = links
    return body


def test_rerouted_host_is_refused_before_any_fetch(environment, monkeypatch):
    # The bootstrap itself sends the query to RIPE's RDAP server: nothing is fetched.
    monkeypatch.setattr(RdapClient, "lookup_ip", REAL_RDAP_LOOKUP_IP)
    fetched = fake_rdap_http(
        monkeypatch,
        {"https://rdap.db.ripe.net/ip/45.10.1.1": HttpAnswer(200, body={})},
        base="https://RDAP.db.ripe.net:443/ip/",
    )
    guarded = RdapClient(user_agent="test", reroute_hosts={"rdap.db.ripe.net"})
    with pytest.raises(rdap_client.RdapRedirect) as raised:
        guarded.lookup_ip("45.10.1.1")
    assert raised.value.registry == "ripe" and raised.value.status_code is None
    assert fetched == []
    assert (
        rdap_client.url_host("https://RDAP.APNIC.net:443/ip/1.0.0.1")
        == "rdap.apnic.net"
    )
    # An address without an exact bootstrap match is never sent anywhere.
    env = environment
    rdap_calls = []
    monkeypatch.setattr(
        RdapClient,
        "lookup_ip",
        lambda self, ip: (rdap_calls.append(ip), response(ip))[1],
    )
    monkeypatch.setattr(RdapClient, "registry_for", lambda self, ip: "")
    enricher = resolver(env)
    resolved = enricher.resolve_page(page(env, "45.10.1.1"))
    assert (
        resolved["45.10.1.1"]["rdap_lookup_status"],
        resolved["45.10.1.1"]["rdap_error_code"],
    ) == ("terminal_error", "no_registry")
    assert resolved["45.10.1.1"]["rdap_retry_after"] is None
    assert rdap_calls == [] and env.calls == [] and enricher.requests == 0
    assert env.client.execute(
        "SELECT ip, lookup_status, error_code FROM corpscout.rdap_ip_lookup_results_current"
    ) == [("45.10.1.1", "terminal_error", "no_registry")]
    # The terminal marker is served within rdap_cache_days: the next run does not ask
    # the bootstrap again and still requests nothing.
    monkeypatch.setattr(
        RdapClient,
        "registry_for",
        lambda self, ip: pytest.fail("a cached no_registry marker was re-resolved"),
    )
    again = resolver(env)
    cached = again.resolve_page(page(env, "45.10.1.1"))["45.10.1.1"]
    assert (cached["rdap_lookup_status"], cached["rdap_error_code"]) == (
        "terminal_error",
        "no_registry",
    )
    assert rdap_calls == [] and again.requests == 0 and again.cache_hits == 1


def test_redirects_to_apnic_are_answered_by_whois(environment, monkeypatch):
    env = environment
    arin = "https://rdap.arin.net/registry/ip/45.64.1.1"
    apnic = "https://rdap.apnic.net/ip/45.64.1.1"
    monkeypatch.setattr(RdapClient, "lookup_ip", REAL_RDAP_LOOKUP_IP)
    fetched = fake_rdap_http(
        monkeypatch, {arin: HttpAnswer(301, location=apnic), apnic: HttpAnswer(500)}
    )
    enricher = resolver(env)  # registry_for says arin for 45/8 (the fixture)
    found = enricher.resolve_page(page(env, "45.64.1.1"))["45.64.1.1"]
    assert fetched == [arin] and env.calls == ["45.64.1.1"]  # whois answered
    assert (found["rdap_rir"], found["rdap_name"], found["rdap_registrant_names"]) == (
        "apnic",
        "APNIC-TEST-NET",
        ["Test Holder Pty Ltd"],
    )
    assert enricher.reroutes_by_registry == {"apnic": 1}
    assert enricher.requests_by_registry == {"arin": 1, "apnic": 1}
    assert enricher.person_entities_by_registry == {}


def test_parent_redirect_into_ripe_is_a_parent_failure_without_a_fetch(
    environment, monkeypatch
):
    env = environment
    direct = "https://rdap.arin.net/registry/ip/45.20.1.1"
    parent = "https://rdap.arin.net/registry/ip/45.0.0.0/8"
    ripe = "https://rdap.db.ripe.net/ip/45.0.0.0/8"
    monkeypatch.setattr(RdapClient, "lookup_ip", REAL_RDAP_LOOKUP_IP)
    fetched = fake_rdap_http(
        monkeypatch,
        {
            direct: HttpAnswer(200, body=arin_rdap_body("45.20.1.1", up=parent)),
            parent: HttpAnswer(301, location=ripe),
            ripe: HttpAnswer(200, body=ripe_rdap_body("45.0.0.0", persons=4)),
        },
    )
    enricher = resolver(env, parent_depth=1)
    found = enricher.resolve_page(page(env, "45.20.1.1"))["45.20.1.1"]
    assert found["rdap_lookup_status"] == "found" and found["rdap_rir"] == "arin"
    assert fetched == [direct, parent]  # RIPE's body is never fetched
    assert enricher.parent_failures == 1 and enricher.person_entities_by_registry == {}
    assert env.client.execute("SELECT count() FROM corpscout.rdap_networks") == [(1,)]


def test_results_job_is_never_retried_by_the_run_retry_daemon():
    # dagster.yaml enables run retries (max_retries 2) and they apply to a
    # dg.Failure(allow_retries=False) too (checked against Dagster 1.13.9): without this
    # tag a max_requests stop would be relaunched and resume the same execution.
    job = results.ip_enrichment_results_job
    assert job.run_tags["dagster/max_retries"] == "0"
    # The GraphQL launch (backoffice, launchpad) merges the definition tags.
    assert job.tags["dagster/max_retries"] == "0"


# --- Task 10: embedded IPv4 (6to4, IPv4-mapped) and Teredo --------------------------------

TEREDO = "2001:0:4136:e378:8000:63bf:3fff:fdd2"


def lookup_markers(env):
    """(ip, status, error_code, bucket is ClickHouse's own) of every stored lookup marker."""
    return env.client.execute(
        """SELECT ip, lookup_status, error_code, bucket = toUInt16(cityHash64(ip) % 256)
        FROM corpscout.rdap_ip_lookup_results FINAL ORDER BY ip"""
    )


def test_6to4_addresses_are_resolved_through_their_embedded_ipv4(environment):
    env = environment
    enricher = resolver(env)
    resolved = enricher.resolve_page(page(env, "2002:808:808::1"))
    assert env.calls == [
        "8.8.8.8"
    ]  # exactly one request, for the IPv4, to its registry
    assert enricher.requests_by_registry == {"arin": 1}
    found = resolved["2002:808:808::1"]
    assert (
        found["rdap_lookup_status"],
        found["rdap_matched_cidr"],
        found["rdap_start_address"],
        found["rdap_end_address"],
    ) == ("found", "8.8.8.0/24", "8.8.8.0", "8.8.8.255")
    # The IPv4's marker, in the IPv4's bucket; none for the IPv6 address; IPv4 segments only.
    assert lookup_markers(env) == [("8.8.8.8", "found", None, 1)]
    assert env.client.execute(
        "SELECT DISTINCT ip_version FROM corpscout.rdap_network_segments"
    ) == [(4,)]
    # Another 6to4 address of the same IPv4 /24: the in-run cache, no request.
    resolved = enricher.resolve_page(page(env, "2002:808:807::1"))
    assert resolved["2002:808:807::1"]["rdap_matched_cidr"] == "8.8.8.0/24"
    assert env.calls == ["8.8.8.8"] and enricher.cache_hits == 1
    assert enricher.embedded_ipv4_lookups == {"6to4": 2}
    # A resume: the trie serves both another 6to4 address and the plain IPv4.
    env.client.execute("SYSTEM RELOAD DICTIONARY corpscout.rdap_network_trie")
    resumed = resolver(env)
    resolved = resumed.resolve_page(page(env, "2002:808:806::1", "8.8.8.5"))
    assert env.calls == ["8.8.8.8"] and resumed.cache_hits == 2
    assert {ip: r["rdap_network_key"] for ip, r in resolved.items()} == {
        "2002:808:806::1": "arin:TEST-8.8.8.8",
        "8.8.8.5": "arin:TEST-8.8.8.8",
    }


def test_ipv4_mapped_addresses_are_resolved_through_their_ipv4(environment):
    env = environment
    enricher = resolver(env)
    resolved = enricher.resolve_page(
        page(
            env,
            "::ffff:9.9.9.9",
            "::ffff:9.9.9.10",
            "2002:909:909::1",  # the same IPv4 as a 6to4 address
            "::ffff:10.0.0.1",
            "2002:a00:1::1",  # 6to4 of a private IPv4
        )
    )
    assert len(env.calls) == 1 and env.calls[0].startswith("9.9.9.")
    assert {ip: r["rdap_lookup_status"] for ip, r in resolved.items()} == {
        "::ffff:9.9.9.9": "found",
        "::ffff:9.9.9.10": "found",
        "2002:909:909::1": "found",
        "::ffff:10.0.0.1": "not_global",
        "2002:a00:1::1": "not_global",
    }
    assert resolved["::ffff:9.9.9.9"]["rdap_matched_cidr"] == "9.9.9.0/24"
    assert enricher.embedded_ipv4_lookups == {"ipv4_mapped": 2, "6to4": 1}
    # One IPv4 marker, for the requested IPv4 (the other came from the in-run cache,
    # which writes none); none for the IPv6 forms; non-global addresses keep their own.
    assert lookup_markers(env) == [
        ("2002:a00:1::1", "not_global", None, 1),
        (env.calls[0], "found", None, 1),
        ("::ffff:10.0.0.1", "not_global", None, 1),
    ]


def test_teredo_is_special_and_unmapped_ipv6_stays_no_registry(
    environment, monkeypatch
):
    env = environment
    monkeypatch.setattr(RdapClient, "registry_for", lambda self, ip: "")
    enricher = resolver(env)
    resolved = enricher.resolve_page(page(env, TEREDO, "2c0f:ffff::1"))
    assert env.calls == [] and enricher.requests == 0
    assert outcome_of(resolved[TEREDO]) == ("not_global", None)
    assert outcome_of(resolved["2c0f:ffff::1"]) == ("terminal_error", "no_registry")
    assert (enricher.teredo_special, enricher.embedded_ipv4_lookups) == (1, {})


def outcome_of(result):
    return result["rdap_lookup_status"], result["rdap_error_code"]


def test_6to4_address_in_ripe_space_uses_the_rest_path(environment, monkeypatch):
    env = environment
    rdap_calls = []
    monkeypatch.setattr(
        RdapClient,
        "lookup_ip",
        lambda self, ip: (rdap_calls.append(ip), response(ip))[1],
    )
    enricher = resolver(env)
    resolved = enricher.resolve_page(page(env, "2002:501:101::1"))  # 5.1.1.1
    assert env.calls == ["5.1.1.1"] and rdap_calls == []  # REST, never RDAP
    found = resolved["2002:501:101::1"]
    assert (found["rdap_rir"], found["rdap_name"], found["rdap_matched_cidr"]) == (
        "ripe",
        "RIPE-TEST-NET",
        "5.1.1.0/24",
    )
    assert enricher.requests_by_registry == {"ripe": 1}
    assert enricher.person_entities_by_registry == {}


def test_embedded_ipv4_run_metadata_scope_and_geoip(environment):
    env = environment
    ips = ["2002:808:808::1", "::ffff:8.8.4.4", TEREDO, "::ffff:10.0.0.1"]
    result = run(env, select(env, ips))
    assert result.success
    metadata = outcome(result)
    assert metadata["completion_status"] == "completed"
    assert metadata["embedded_ipv4_lookups"] == {"6to4": 1, "ipv4_mapped": 1}
    assert metadata["teredo_special"] == 1
    assert metadata["rdap_requests"] == 2
    assert sorted(env.calls) == ["8.8.4.4", "8.8.8.8"]
    assert env.client.execute(
        """SELECT ip, ip_scope, city_lookup_status, asn_lookup_status, rdap_lookup_status,
            rdap_matched_cidr FROM corpscout.ip_enrichment_current ORDER BY ip"""
    ) == [
        (TEREDO, "private", "not_global", "not_global", "not_global", None),
        ("2002:808:808::1", "global", "found", "found", "found", "8.8.8.0/24"),
        ("::ffff:10.0.0.1", "private", "not_global", "not_global", "not_global", None),
        ("::ffff:8.8.4.4", "global", "found", "found", "found", "8.8.4.0/24"),
    ]
    # GeoIP looks the IPv6 address up as it is (MaxMind aliases it to the IPv4 tree).
    assert "2002:808:808::1" in env.city.calls and "8.8.8.8" not in env.city.calls


def test_maxmind_aliases_6to4_and_ipv4_mapped_to_the_ipv4_record():
    """The vendored GeoLite2 test databases answer both forms with the IPv4's record."""
    fixtures = Path(__file__).parent / "fixtures/geolite2"
    checked_at = datetime.now(UTC)
    with (
        enrichment.maxminddb.open_database(
            str(fixtures / "GeoLite2-City-Test.mmdb")
        ) as city,
        enrichment.maxminddb.open_database(
            str(fixtures / "GeoLite2-ASN-Test.mmdb")
        ) as asn,
    ):

        def geo(ip):
            result = enrichment.geoip_result(
                ip, city, asn, checked_at=checked_at, retry_seconds=60
            )
            return (
                result["ip_scope"],
                result["city_lookup_status"],
                result["city_name"],
                result["asn_lookup_status"],
                result["asn"],
            )

        london = ("global", "found", "London", "not_found", None)
        assert geo("81.2.69.160") == london
        assert geo("2002:5102:45a0::1") == london
        assert geo("::ffff:81.2.69.160") == london
        linkoping = geo("89.160.20.112")
        assert linkoping[1:] == ("found", "Linköping", "found", 29518)
        assert geo("2002:59a0:1470::1") == linkoping
        assert geo("::ffff:89.160.20.112") == linkoping
        assert geo(TEREDO)[:2] == ("private", "not_global")
