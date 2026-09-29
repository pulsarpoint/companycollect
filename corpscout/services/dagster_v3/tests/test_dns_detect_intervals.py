import pytest

from dagster_v3.defs.dns_detect import assets, intervals, sql


class FakeClient:
    def __init__(self, staged: int, current: int) -> None:
        self.statements: list[tuple[str, dict | None]] = []
        self.staged, self.current = staged, current

    def execute(self, query: str, params=None, **_):
        self.statements.append((query, params))
        if query.startswith("SELECT count() FROM ("):
            return [[self.current]]
        if query.startswith("SELECT count(), countIf(is_current = 1)"):
            return [[self.staged, self.staged, 2, 1]]
        return []


class Log:
    def info(self, *a) -> None: ...


def kinds(client: FakeClient) -> list[str]:
    return [q.split("(")[0].split(" `")[0].strip() for q, _ in client.statements]


def test_rebuild_swaps_both_tables_after_both_inserts_and_drops_stages() -> None:
    c = FakeClient(staged=10, current=10)
    out = intervals.rebuild_bucket(c, "corpscout", 3, Log(), stage_suffix="x")
    qs = [q for q, _ in c.statements]
    swap_i = next(i for i, q in enumerate(qs) if "REPLACE PARTITION 3" in q and sql.INTERVALS_TABLE in q.split("FROM")[0])
    counts_insert_i = next(i for i, q in enumerate(qs) if q.startswith("INSERT INTO") and "provider_service_counts_stage_x" in q)
    assert counts_insert_i < swap_i
    assert sum("REPLACE PARTITION 3" in q for q in qs) == 2
    assert qs[-2:] == ["DROP TABLE IF EXISTS `corpscout`.`domain_service_intervals_stage_x`",
                       "DROP TABLE IF EXISTS `corpscout`.`provider_service_counts_stage_x`"]
    assert all(p == {} for q, p in c.statements if "%%" in q)
    assert out == {"intervals": 10, "current": 10, "providers": 2, "unmapped_keys": 1}


def test_rebuild_refuses_an_empty_stage_when_the_bucket_has_results() -> None:
    c = FakeClient(staged=0, current=5)
    with pytest.raises(ValueError, match="bucket 3"):
        intervals.rebuild_bucket(c, "corpscout", 3, Log(), stage_suffix="x")
    assert not any("REPLACE PARTITION" in q for q, _ in c.statements)
    assert c.statements[-1][0].startswith("DROP TABLE IF EXISTS")


def test_rebuild_accepts_an_empty_bucket_without_results() -> None:
    c = FakeClient(staged=0, current=0)
    intervals.rebuild_bucket(c, "corpscout", 3, Log(), stage_suffix="x")
    assert sum("REPLACE PARTITION 3" in q for q, _ in c.statements) == 2


def test_intervals_asset_is_in_the_resolver_job_after_the_resolver() -> None:
    both = [assets.dns_record_services_clickhouse, assets.domain_service_intervals_clickhouse]
    selected = assets.dns_record_services_job.selection.resolve(both)
    assert selected == {dg_key("dns_record_services_clickhouse"), dg_key("domain_service_intervals_clickhouse")}
    spec = assets.domain_service_intervals_clickhouse
    assert spec.partitions_def == assets.PARTITIONS
    assert dg_key("dns_record_services_clickhouse") in spec.asset_deps[dg_key("domain_service_intervals_clickhouse")]


def dg_key(name: str):
    import dagster as dg
    return dg.AssetKey(name)
