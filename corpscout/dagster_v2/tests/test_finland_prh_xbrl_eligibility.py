from types import SimpleNamespace

from dagster_corpscout.resources.clickhouse import ClickHouseResource
from dagster_corpscout.sources.finland.prh_xbrl.eligibility import (
    COMPANY_CACHE_ASSET_KEY,
    fetch_eligible_business_ids,
)


class _QueryRecordingClient:
    def __init__(self):
        self.rows = []
        self.queries = []

    def query(self, sql):
        self.queries.append(sql)
        return SimpleNamespace(result_rows=list(self.rows))


_query_client = _QueryRecordingClient()


class FakeClickHouseResource(ClickHouseResource):
    def client(self):
        return _query_client


def test_fetch_eligible_business_ids_returns_id_set_from_active_website_query():
    _query_client.rows = [("0176460-0",), ("0100037-9",)]
    _query_client.queries.clear()

    ids = fetch_eligible_business_ids(FakeClickHouseResource(host="x", password="x"))

    assert ids == {"0176460-0", "0100037-9"}
    [sql] = _query_client.queries
    assert "fi_prhytj_company_explorer_cache" in sql
    assert "lifecycle_status = 'active'" in sql
    assert "website != ''" in sql


def test_company_cache_asset_key_points_at_prh_ytj_serving_asset():
    assert COMPANY_CACHE_ASSET_KEY == ["sources", "finland", "prh_ytj", "company_explorer_cache"]
