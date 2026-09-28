import json
import threading
from contextlib import contextmanager
from datetime import UTC, date, datetime
from http.server import BaseHTTPRequestHandler, HTTPServer

import dagster as dg
import pytest

from dagster_v3.defs.dns_detect import assets, sql
from dagster_v3.defs.dns_detect.resource import DnsDetectError, DnsDetectResource

RECORD = ["0" * 31 + "1", "example.se", "example.se", "NS", "ns1.loopia.se.", "2026-08-01", "2026-09-20"]


def line(record_id: str, analyzer: str, results: list[dict], findings: list[dict] | None = None) -> dict:
    return {"record_id": record_id, "analyzer": analyzer, "results": results, "findings": findings or []}


def result(record_id: str, provider: str = "loopia") -> dict:
    return {"record_id": record_id, "root_domain": "example.se", "record_name": "example.se", "record_type": "NS", "analyzer": "ns",
            "subject": "ns1.loopia.se", "service_type": "dns", "provider_key": provider, "provider_slug": provider,
            "service_key": f"{provider}.dns", "rule_id": "rule", "confidence": 1, "fallback": False,
            "valid_from": "2026-08-01", "valid_to": "2026-09-20"}


def test_rows_for_builds_resolutions_and_results() -> None:
    records = [assets.record_dict(RECORD), assets.record_dict(["0" * 31 + "2", "example.se", "example.se", "A", "192.0.2.1", "2026-08-01", "2026-09-20"])]
    at = datetime(2026, 9, 28, tzinfo=UTC).replace(tzinfo=None)
    res, svc = assets.rows_for(records, [
        line(records[0]["record_id"], "ns", [result(records[0]["record_id"])]),
        line(records[1]["record_id"], "ip", [], [{"record_id": records[1]["record_id"], "analyzer": "ip", "code": "x", "detail": "y"}]),
    ], "R", "I", at)
    assert res[0] == (bytes.fromhex(RECORD[0]), "example.se", "example.se", "NS", "ns", "R", "I", 1, [],
                      date(2026, 8, 1), date(2026, 9, 20), at)
    assert res[1][4] == "ip" and res[1][7] == 0 and res[1][8] == [("x", "y")]
    assert svc == [(bytes.fromhex(RECORD[0]), "example.se", "example.se", "NS", "ns", "ns1.loopia.se", "dns", "loopia", "loopia",
                    "loopia.dns", "rule", 1.0, 0, date(2026, 8, 1), date(2026, 9, 20), at)]


def test_rows_for_keys_both_tables_on_the_input_record() -> None:
    records = [assets.record_dict(RECORD)]
    odd = result(records[0]["record_id"]) | {"root_domain": "EXAMPLE.se", "record_name": "Example.SE", "record_type": "ns"}
    _, svc = assets.rows_for(records, [line(records[0]["record_id"], "ns", [odd])], "R", "I", datetime(2026, 9, 28))
    assert svc[0][1:4] == ("example.se", "example.se", "NS")


def test_rows_for_refuses_an_answer_without_its_analyzer() -> None:
    records = [assets.record_dict(RECORD)]
    old_service = {"record_id": records[0]["record_id"], "results": [], "findings": []}
    with pytest.raises(ValueError, match="analyzer"):
        assets.rows_for(records, [old_service], "R", "I", datetime(2026, 9, 28))


def test_rows_for_refuses_a_response_out_of_order() -> None:
    records = [assets.record_dict(RECORD)]
    with pytest.raises(ValueError, match="out of order"):
        assets.rows_for(records, [line("f" * 32, "ns", [])], "R", "I", datetime(2026, 9, 28))


class _Handler(BaseHTTPRequestHandler):
    status = 200

    def log_message(self, *args):
        pass

    def do_GET(self):
        body = json.dumps({"rules_version": "R", "ip_version": "I", "documents": 37}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        records = [json.loads(x) for x in self.rfile.read(int(self.headers["Content-Length"])).decode().splitlines() if x]
        if _Handler.status != 200:
            self.send_response(_Handler.status)
            self.end_headers()
            self.wfile.write(b'{"error":"record 2: bad"}')
            return
        self.send_response(200)
        self.send_header("X-Rules-Version", "R")
        self.send_header("X-IP-Version", "I")
        self.end_headers()
        for r in records:
            self.wfile.write((json.dumps(line(r["record_id"], "ns", [])) + "\n").encode())


@contextmanager
def fake_service(status: int = 200):
    _Handler.status = status
    server = HTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield DnsDetectResource(api_url=f"http://127.0.0.1:{server.server_port}")
    finally:
        server.shutdown()


def test_resource_round_trip_and_errors() -> None:
    with fake_service() as svc:
        assert svc.knowledge()["rules_version"] == "R"
        lines, rules, ip = svc.resolve([assets.record_dict(RECORD)])
        assert (rules, ip) == ("R", "I") and lines[0]["record_id"] == RECORD[0]
    with fake_service(400) as svc, pytest.raises(DnsDetectError, match="record 2"):
        svc.resolve([assets.record_dict(RECORD)])


class FakeReader:
    def __init__(self, rows):
        self.rows = rows
        self.queries: list[tuple[str, dict]] = []

    def execute_iter(self, query, params=None, **_):
        self.queries.append((query, params))
        yield from self.rows

    def execute(self, query, params=None, **_):
        return [(name,) for name in assets.REQUIRED_TABLES] if "system.tables" in query else []


class FakeWriter:
    def __init__(self):
        self.inserts: list[tuple[str, int]] = []

    def execute(self, query, rows=None, **_):
        if "system.tables" in query:
            return [(name,) for name in assets.REQUIRED_TABLES]
        if query.startswith("INSERT"):
            self.inserts.append((query.split("`")[3], len(rows)))
        return []


class FakeService:
    def __init__(self, fail_on: int | None = None):
        self.calls: list[int] = []
        self.fail_on = fail_on

    def knowledge(self):
        return {"rules_version": "R", "ip_version": "I"}

    def resolve(self, records):
        self.calls.append(len(records))
        if self.fail_on is not None and len(self.calls) == self.fail_on:
            raise DnsDetectError("boom")
        return [line(r["record_id"], "ns", [result(r["record_id"])]) for r in records], "R", "I"


def rows(n: int) -> list[list[str]]:
    return [[f"{i:032x}"] + RECORD[1:] for i in range(n)]


def test_resolve_partition_chunks_and_writes_results_before_resolutions() -> None:
    reader, writer, service = FakeReader(rows(5)), FakeWriter(), FakeService()
    counts = assets.resolve_partition(reader, writer, service, 7, dg.get_dagster_logger(), chunk_size=2, workers=2)
    assert sorted(service.calls) == [1, 2, 2]  # concurrent: completion order varies
    assert counts == {"records": 5, "results": 5, "findings": 0, "chunks": 3}
    # Per chunk: results first, then resolutions.
    assert [t for t, _ in writer.inserts] == [sql.SERVICES_TABLE, sql.RESOLUTIONS_TABLE] * 3
    assert "% 128 = 7" in reader.queries[0][0] and reader.queries[0][1] == {"rules_version": "R", "ip_version": "I"}


def test_resolve_partition_with_nothing_to_do() -> None:
    counts = assets.resolve_partition(FakeReader([]), FakeWriter(), FakeService(), 7, dg.get_dagster_logger())
    assert counts == {"records": 0, "results": 0, "findings": 0, "chunks": 0}


def test_resolve_partition_fails_without_writing_the_failed_chunk() -> None:
    writer = FakeWriter()
    with pytest.raises(DnsDetectError):
        assets.resolve_partition(FakeReader(rows(5)), writer, FakeService(fail_on=2), 7, dg.get_dagster_logger(), chunk_size=2, workers=1)
    assert writer.inserts == [(sql.SERVICES_TABLE, 2), (sql.RESOLUTIONS_TABLE, 2)]


class FakeKnowledge:
    def __init__(self, rules: str, ip: str):
        self.value = {"rules_version": rules, "ip_version": ip}

    def knowledge(self):
        return self.value


def evaluate_sensor(rules: str, ip: str, cursor: str | None, running: bool = False):
    with dg.instance_for_test() as instance:
        if running:
            instance.get_run_records = lambda **_: [object()]
        context = dg.build_sensor_context(instance=instance, cursor=cursor)
        return assets.dns_detect_knowledge_sensor(context, dns_detect=FakeKnowledge(rules, ip))


def test_sensor_baseline_change_and_busy() -> None:
    first = evaluate_sensor("R", "I", None)
    assert first.cursor == json.dumps({"rules_version": "R", "ip_version": "I"}) and not first.run_requests
    assert isinstance(evaluate_sensor("R", "I", first.cursor), dg.SkipReason)
    changed = evaluate_sensor("R", "I2", first.cursor)
    assert len(changed.run_requests) == sql.PARTITION_COUNT and changed.cursor != first.cursor
    assert isinstance(evaluate_sensor("R", "I2", first.cursor, running=True), dg.SkipReason)


def test_daily_schedule_requests_every_partition() -> None:
    with dg.instance_for_test() as instance:
        context = dg.build_schedule_context(instance=instance, scheduled_execution_time=datetime(2026, 10, 1, 5, 20))
        requests = assets.dns_record_services_daily(context)
    assert len(requests) == sql.PARTITION_COUNT and requests[3].run_key == "daily-20261001-hash_003"
