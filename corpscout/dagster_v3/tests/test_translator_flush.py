from translator.flush import build_flush_select_sql, flush_translations
from translator.queue import FlushTranslationRow
from translator.registry import get_source_config


def test_build_flush_select_sql_computes_hash_in_clickhouse():
    sql = build_flush_select_sql("corpscout.stage_abc")
    assert "INSERT INTO corpscout.text_translations" in sql
    assert "cityHash64(source_text)" in sql
    assert "FROM corpscout.stage_abc" in sql
    assert "{slug:String}" in sql and "{lang:String}" in sql and "{version:UInt64}" in sql


class _FakeClient:
    def __init__(self):
        self.commands: list[str] = []
        self.inserts: list[tuple] = []

    def command(self, sql, parameters=None):
        self.commands.append(sql)

    def insert(self, table, data, column_names=None):
        self.inserts.append((table, data, tuple(column_names or ())))


def test_flush_skips_empty_and_writes_rows():
    client = _FakeClient()
    config = get_source_config("norway_brreg")
    rows = [
        FlushTranslationRow("company_description", "Holdingselskap", "Holding company"),
        FlushTranslationRow("activity_text", "Tomt", ""),  # empty -> skipped
    ]
    written = flush_translations(
        client, config, rows, provider="prov", model="model", version=123, run_id="run-1"
    )
    assert written == 1
    assert any("CREATE TABLE" in c and "ENGINE = Memory" in c for c in client.commands)
    assert client.inserts and client.inserts[0][1] == [["company_description", "Holdingselskap", "Holding company"]]
    assert any("INSERT INTO corpscout.text_translations" in c for c in client.commands)
    assert any(c.startswith("DROP TABLE") for c in client.commands)


def test_flush_no_rows_is_noop():
    client = _FakeClient()
    config = get_source_config("norway_brreg")
    assert flush_translations(client, config, [], provider="p", model="m", version=1, run_id="r") == 0
    assert client.commands == []
