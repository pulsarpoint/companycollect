"""Task-scoped enrichment with real storage and controlled external lookup responses."""

from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import dagster as dg
import pytest

from dagster_v3.defs.common.processing import ProcessingResource
from dagster_v3.defs.commoncrawl_geoip.resources import MaxMindDatabaseResource
from dagster_v3.defs.commoncrawl_rdap.client import RdapClientError
from dagster_v3.defs.commoncrawl_rdap.rdap import RdapLookupResponse
from dagster_v3.defs.ip_enrichment import enrichment, results
from dagster_v3.defs.ip_enrichment.input import ip_enrichment_input
from tests.test_ip_enrichment_input import (
    materialize as prepare_input,
    server as server,
)
from tests.test_ip_registry import apply_migration, seed_reference_data
from tests.test_processing_store import (
    processing_postgres_url as processing_postgres_url,
    store as store,
)


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


def response(ip, *, start=None, end=None):
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
        },
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
    # Reference data, classes and the class-aware trie view (000449/000450), idempotent.
    apply_migration(client, "000449_corpscout_ip_registry_reference_data.up.sql")
    apply_migration(
        client, "000450_corpscout_rdap_trie_registry_class_exclusion.up.sql"
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


def select(env, ips):
    task = str(uuid4())
    assert prepare_input(env.resource, env.dsn, task_id=task, ips=ips).success
    return task


def run(env, task, **config):
    return dg.materialize(
        [results.ip_enrichment_results],
        instance=env.instance,
        resources={
            "clickhouse": env.resource,
            "processing": ProcessingResource(postgres_url=env.dsn),
            "maxmind_geoip": env.maxmind,
        },
        run_config={
            "ops": {
                "ip_enrichment_results": {
                    "config": {
                        "task_id": task,
                        "request_delay_seconds": 0,
                        **config,
                    }
                }
            }
        },
        raise_on_error=False,
    )


def test_selected_task_geoip_rdap_segments_and_resume(environment):
    env = environment
    select(env, ["9.9.9.9"])
    task = select(env, ["8.8.8.8", "8.8.8.9", "2001:4860::8888", "127.0.0.1"])
    first = run(env, task, batch_size=1)
    assert first.success
    assert sorted(env.calls) == ["2001:4860::8888", "8.8.8.8"]
    assert "127.0.0.1" not in env.city.calls
    assert env.client.execute("""SELECT ip, country_iso_code, rdap_country_code,
        city_network, asn_network, rdap_matched_cidr FROM corpscout.ip_enrichment_current
        WHERE ip='8.8.8.8'""") == [
        ("8.8.8.8", "US", "CA", "8.8.8.0/24", "8.8.8.0/24", "8.8.8.0/24")
    ]
    assert env.client.execute(
        "SELECT count(), uniqExact(task_id) FROM corpscout.ip_enrichment_results FINAL"
    ) == [(4, 1)]
    assert env.client.execute(
        "SELECT rdap_lookup_status, ip_scope FROM corpscout.ip_enrichment_current WHERE ip='127.0.0.1'"
    ) == [("not_global", "loopback")]
    assert env.client.execute(
        "SELECT count() FROM corpscout.rdap_network_segments FINAL"
    ) == [(2,)]
    calls_before = list(env.calls)
    assert run(env, task, execution_id=first.run_id, batch_size=2).success
    assert env.calls == calls_before
    assert env.client.execute(
        "SELECT count() FROM corpscout.ip_enrichment_results FINAL"
    ) == [(4,)]
    assert run(env, select(env, ["8.8.8.10"])).success
    assert (
        env.calls == calls_before
    )  # Cached coverage survives into another execution/task.


def test_errors_keep_geoip_and_retry_backoff(environment, monkeypatch):
    env = environment
    attempts = []

    def unavailable(self, ip):
        attempts.append(ip)
        raise RdapClientError(
            "rate limit", code="rate_limited", retryable=True, status_code=429
        )

    monkeypatch.setattr(enrichment.RdapClient, "lookup_ip", unavailable)
    task = select(env, ["8.8.8.8"])
    first = run(env, task)
    assert not first.success
    assert env.client.execute("""SELECT city_lookup_status, asn_lookup_status, rdap_lookup_status,
        country_iso_code, rdap_error_code, rdap_retry_after > rdap_checked_at
        FROM corpscout.ip_enrichment_current""") == [
        ("found", "found", "retryable_error", "US", "rate_limited", 1)
    ]
    assert not run(env, task, execution_id=first.run_id).success
    assert not run(env, task).success
    assert attempts == ["8.8.8.8"]
    monkeypatch.setattr(
        enrichment.RdapClient, "lookup_ip", lambda self, ip: response(ip)
    )
    assert run(env, task, force_rdap=True).success
    assert env.client.execute(
        "SELECT rdap_lookup_status FROM corpscout.ip_enrichment_current"
    ) == [("found",)]


def test_request_budget_is_resumable_without_marking_unprocessed_inputs_done(
    environment,
):
    env = environment
    task = select(env, ["1.1.1.1", "8.8.8.8"])
    first = run(env, task, max_requests=1)
    assert not first.success
    assert env.client.execute(
        "SELECT count() FROM corpscout.ip_enrichment_results FINAL"
    ) == [(1,)]
    assert run(env, task, execution_id=first.run_id, max_requests=1).success
    assert env.calls == ["1.1.1.1", "8.8.8.8"]
    assert env.client.execute(
        "SELECT count() FROM corpscout.ip_enrichment_results FINAL"
    ) == [(2,)]


@pytest.mark.parametrize("kind", ["catch_all", "wrong_range"])
def test_invalid_registration_coverage_is_not_saved(environment, monkeypatch, kind):
    env = environment
    monkeypatch.setattr(
        enrichment.RdapClient,
        "lookup_ip",
        lambda self, ip: response(
            ip,
            start="0.0.0.0" if kind == "catch_all" else "9.9.9.0",
            end="255.255.255.255" if kind == "catch_all" else "9.9.9.255",
        ),
    )
    assert not run(env, select(env, ["8.8.8.8"])).success
    assert env.client.execute(
        "SELECT rdap_lookup_status, city_lookup_status FROM corpscout.ip_enrichment_current"
    ) == [("terminal_error", "found")]
    assert env.client.execute("SELECT count() FROM corpscout.rdap_networks") == [(0,)]


def test_city_lookup_failure_does_not_discard_asn_or_rdap(environment):
    env = environment
    env.city.fail = True
    assert not run(env, select(env, ["8.8.8.8"])).success
    assert env.client.execute(
        "SELECT city_lookup_status, asn_lookup_status, rdap_lookup_status, asn FROM corpscout.ip_enrichment_current"
    ) == [("retryable_error", "found", "found", 15169)]


def test_unknown_task_fails_before_external_lookups(environment):
    env = environment
    assert not run(env, str(uuid4())).success
    assert env.calls == env.city.calls == []


def test_resume_after_lost_write_acknowledgement_does_not_repeat_lookup(
    environment, monkeypatch
):
    env = environment
    task = select(env, ["8.8.8.8"])
    real_insert = results.insert_result

    def write_then_disconnect(client, record):
        real_insert(client, record)
        raise ConnectionError("lost acknowledgement after durable insert")

    monkeypatch.setattr(results, "insert_result", write_then_disconnect)
    first = run(env, task)
    assert not first.success
    assert env.client.execute(
        "SELECT count() FROM corpscout.ip_enrichment_results FINAL"
    ) == [(1,)]
    monkeypatch.setattr(results, "insert_result", real_insert)
    assert run(env, task, execution_id=first.run_id).success
    assert env.calls == env.city.calls == ["8.8.8.8"]
    assert env.client.execute(
        "SELECT count() FROM corpscout.ip_enrichment_results FINAL"
    ) == [(1,)]


def test_non_aligned_rdap_range_saves_exact_matching_segment(environment, monkeypatch):
    env = environment
    monkeypatch.setattr(
        enrichment.RdapClient,
        "lookup_ip",
        lambda self, ip: response(
            ip,
            start="8.8.8.1",
            end="8.8.8.10",
        ),
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

    monkeypatch.setattr(enrichment.RdapClient, "lookup_ip", direct)
    monkeypatch.setattr(enrichment.RdapClient, "lookup_up_url", parent)
    result = run(env, select(env, ["8.8.8.8"]))
    assert result.success
    metadata = result.asset_materializations_for_node("ip_enrichment_results")[
        0
    ].metadata
    assert metadata["parent_lookup_failures"].value == 1
    assert metadata["rdap_requests"].value == 2
    assert env.client.execute(
        "SELECT rdap_lookup_status, rdap_matched_cidr FROM corpscout.ip_enrichment_current"
    ) == [("found", "8.8.8.0/24")]


def test_resume_rejects_different_task_or_lookup_policy(environment):
    env = environment
    first_task = select(env, ["8.8.8.8"])
    second_task = select(env, ["1.1.1.1"])
    first = run(env, first_task)
    assert first.success
    assert not run(env, second_task, execution_id=first.run_id).success
    assert not run(env, first_task, execution_id=first.run_id, force_rdap=True).success
    assert env.calls == ["8.8.8.8"]


def test_workflow_freezes_and_processes_the_same_task(environment):
    env = environment
    task = str(uuid4())
    defs = dg.Definitions(
        assets=[ip_enrichment_input, results.ip_enrichment_results],
        jobs=[results.ip_enrichment_workflow],
        resources={
            "clickhouse": env.resource,
            "processing": ProcessingResource(postgres_url=env.dsn),
            "maxmind_geoip": env.maxmind,
        },
    )
    result = defs.resolve_job_def("ip_enrichment_workflow").execute_in_process(
        instance=env.instance,
        run_config={
            "ops": {
                "ip_enrichment_input": {
                    "config": {
                        "task_id": task,
                        "ips": ["8.8.8.8", "9.9.9.9", "127.0.0.1"],
                    }
                },
                "ip_enrichment_results": {
                    "config": {
                        "task_id": task,
                        "max_requests": None,
                        "request_delay_seconds": 0,
                    }
                },
            }
        },
    )
    assert result.success
    assert len(result.asset_materializations_for_node("ip_enrichment_input")) == 1
    assert len(result.asset_materializations_for_node("ip_enrichment_results")) == 1
    assert env.client.execute(
        "SELECT ip FROM corpscout.ip_enrichment_results WHERE task_id=%(task)s ORDER BY ip",
        {"task": task},
    ) == [("127.0.0.1",), ("8.8.8.8",), ("9.9.9.9",)]
    assert sorted(env.calls) == ["8.8.8.8", "9.9.9.9"]


def test_registry_level_registration_answers_only_the_queried_ip(
    environment, monkeypatch
):
    env = environment
    seed_reference_data(env.client)
    monkeypatch.setattr(
        enrichment.RdapClient,
        "lookup_ip",
        lambda self, ip: (
            env.calls.append(ip),
            response(ip, start="103.0.0.0", end="103.255.255.255"),
        )[1],
    )
    first = run(env, select(env, ["103.35.64.49"]))
    assert first.success
    assert (
        first.asset_materializations_for_node("ip_enrichment_results")[0]
        .metadata["registry_level_responses"]
        .value
        == 1
    )
    assert env.client.execute(
        "SELECT rdap_lookup_status, rdap_matched_cidr, rdap_start_address FROM corpscout.ip_enrichment_current"
    ) == [("found", "103.0.0.0/8", "103.0.0.0")]
    assert env.client.execute(
        "SELECT network_key, registry_class, covered_rir_blocks, iana_rir, special_status FROM corpscout.rdap_network_registry_class_current"
    ) == [("arin:TEST-103.35.64.49", "registry_level", 1, "apnic", "")]
    env.client.execute("SYSTEM RELOAD DICTIONARY corpscout.rdap_network_trie")
    assert env.client.execute(
        "SELECT count() FROM corpscout.rdap_network_segments_current"
    ) == [(0,)]
    # Another address of the block is looked up, not served from the /8 (neither trie nor in-run cache).
    assert run(env, select(env, ["103.15.66.50", "103.15.66.51"]), batch_size=1).success
    assert env.calls == ["103.35.64.49", "103.15.66.50", "103.15.66.51"]
    # A holder registration is classified reusable and serves its neighbours as before.
    monkeypatch.setattr(
        enrichment.RdapClient,
        "lookup_ip",
        lambda self, ip: (
            env.calls.append(ip),
            response(ip, start="103.35.64.0", end="103.35.67.255"),
        )[1],
    )
    assert run(env, select(env, ["103.35.64.1", "103.35.64.2"]), batch_size=1).success
    assert env.calls == ["103.35.64.49", "103.15.66.50", "103.15.66.51", "103.35.64.1"]
    assert env.client.execute(
        "SELECT registry_class FROM corpscout.rdap_network_registry_class_current WHERE network_key = 'arin:TEST-103.35.64.1'"
    ) == [("reusable",)]
    env.client.execute("SYSTEM RELOAD DICTIONARY corpscout.rdap_network_trie")
    assert env.client.execute(
        "SELECT dictGetOrDefault('corpscout.rdap_network_trie', 'network_key', tuple(toIPv4('103.35.65.9')), '')"
    ) == [("arin:TEST-103.35.64.1",)]
