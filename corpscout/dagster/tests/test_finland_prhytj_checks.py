import dagster as dg

SOURCE_PREFIX = ["sources", "finland", "prh_ytj"]

from dagster_corpscout.sources.finland.prh_ytj.checks import (
    check_company_explorer_cache_matches_view,
    check_industry_nace_mappings_have_rows,
)


class FakeQueryResult:
    def __init__(self, rows):
        self.result_rows = rows


class FakeClient:
    def __init__(self, results):
        self.results = list(results)
        self.queries = []

    def query(self, sql):
        self.queries.append(sql)
        return FakeQueryResult([self.results.pop(0)])


class FakeClickHouse:
    database = "corpscout_sources"

    def __init__(self, results):
        self.client_object = FakeClient(results)

    def client(self):
        return self.client_object


def test_industry_nace_mapping_check_requires_rows_and_mapped_rows():
    result = check_industry_nace_mappings_have_rows(FakeClickHouse([(12, 10, 2)]))

    assert result.passed is True
    assert result.metadata["rows"].value == 12
    assert result.metadata["mapped_rows"].value == 10
    assert result.metadata["unmapped_rows"].value == 2


def test_industry_nace_mapping_check_fails_when_no_mapped_rows():
    result = check_industry_nace_mappings_have_rows(FakeClickHouse([(12, 0, 12)]))

    assert result.passed is False


def test_company_explorer_cache_check_requires_matching_nonzero_counts():
    result = check_company_explorer_cache_matches_view(FakeClickHouse([(819443,), (819443,)]))

    assert result.passed is True
    assert result.metadata["explorer_rows"].value == 819443
    assert result.metadata["cache_rows"].value == 819443


def test_company_explorer_cache_check_fails_on_count_mismatch():
    result = check_company_explorer_cache_matches_view(FakeClickHouse([(819443,), (819400,)]))

    assert result.passed is False


def test_finland_prhytj_asset_checks_are_registered():
    from dagster_corpscout.definitions import defs

    mapping_check = defs.get_asset_checks_def(
        dg.AssetCheckKey(
            dg.AssetKey([*SOURCE_PREFIX, "industry_nace_mappings"]),
            "mapping_rows_present",
        )
    )
    cache_check = defs.get_asset_checks_def(
        dg.AssetCheckKey(
            dg.AssetKey([*SOURCE_PREFIX, "company_explorer_cache"]),
            "cache_matches_explorer_view",
        )
    )

    assert mapping_check is not None
    assert cache_check is not None
