import dagster as dg
from moto import mock_aws

from dagster_corpscout.resources.clickhouse import ClickHouseResource
from dagster_corpscout.resources.rustfs import RustFSResource
from dagster_corpscout.sources.finland.prh_xbrl import spec, tables
from dagster_corpscout.sources.finland.prh_xbrl.assets.external import source_system
from dagster_corpscout.sources.finland.prh_xbrl.assets.parsed import statement_tables
from dagster_corpscout.sources.finland.prh_xbrl.assets.raw import raw_xml_documents
from tests.test_finland_prh_xbrl_parser import SAMPLE_XML


class _RecordingClient:
    def __init__(self):
        self.inserts = []

    def insert(self, table, data, column_names):
        self.inserts.append((table, len(data)))


class FakeClickHouseResource(ClickHouseResource):
    def client(self):
        return _recorder


_recorder = _RecordingClient()


@mock_aws
def test_statement_tables_parses_listing_documents_into_clickhouse():
    _recorder.inserts.clear()
    rustfs = RustFSResource(endpoint_url="", access_key="test", secret_key="test")
    rustfs.ensure_bucket(spec.BUCKET)
    rustfs.put_bytes(spec.BUCKET, "companies/0176460-0/2023-09-30.xml", SAMPLE_XML)
    rustfs.put_json(
        spec.BUCKET,
        spec.window_listing_object_key("2025-01-01"),
        {
            "registered_date_start": "2025-01-01",
            "registered_date_end": "2025-01-31",
            "documents": [
                {
                    "business_id": "0176460-0",
                    "financial_date": "2023-09-30",
                    "registration_date": "2025-01-23",
                    "object_key": "companies/0176460-0/2023-09-30.xml",
                    "source_url": "https://example.test/financial",
                    "xml_sha256": "ignored",
                    "xml_size_bytes": len(SAMPLE_XML),
                }
            ],
        },
    )

    result = dg.materialize(
        [source_system, raw_xml_documents, statement_tables],
        selection=[statement_tables],
        partition_key="2025-01-01",
        resources={
            "rustfs": rustfs,
            "clickhouse": FakeClickHouseResource(host="test", password="test"),
        },
    )

    assert result.success
    inserted = dict(_recorder.inserts)
    assert inserted[tables.STATEMENT_DOCUMENTS_TABLE] == 1
    assert inserted[tables.CONTEXTS_TABLE] == 3
    assert inserted[tables.UNITS_TABLE] == 1
    assert inserted[tables.FACTS_TABLE] == 6
