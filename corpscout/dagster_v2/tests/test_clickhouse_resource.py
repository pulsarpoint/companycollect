from dagster_corpscout.resources.clickhouse import ClickHouseResource


def test_clickhouse_resource_builds_client(monkeypatch):
    calls = []

    def fake_get_client(**kwargs):
        calls.append(kwargs)
        return object()

    monkeypatch.setattr("clickhouse_connect.get_client", fake_get_client)

    resource = ClickHouseResource(
        host="companycollect",
        port="8123",
        username="default",
        password="secret",
        database="corpscout_sources",
        secure="false",
    )

    resource.client()

    assert calls == [
        {
            "host": "companycollect",
            "port": 8123,
            "username": "default",
            "password": "secret",
            "database": "corpscout_sources",
            "secure": False,
        }
    ]


def test_clickhouse_resource_reuses_supplied_client_for_inserts():
    class FakeClient:
        def __init__(self):
            self.commands = []
            self.inserts = []

        def command(self, sql):
            self.commands.append(sql)

        def insert(self, table, data, column_names):
            self.inserts.append((table, data, column_names))

    client = FakeClient()
    resource = ClickHouseResource(
        host="companycollect",
        port="8123",
        username="default",
        password="secret",
        database="corpscout_sources",
        secure="false",
    )

    resource.truncate_tables(client, ["fi_prhytj_statuses"])
    resource.insert_rows(
        client,
        "fi_prhytj_statuses",
        ["business_id", "status"],
        [{"business_id": "1234567-8", "status": "1"}],
    )

    assert client.commands == ["TRUNCATE TABLE IF EXISTS fi_prhytj_statuses"]
    assert client.inserts == [
        (
            "fi_prhytj_statuses",
            [["1234567-8", "1"]],
            ["business_id", "status"],
        )
    ]
