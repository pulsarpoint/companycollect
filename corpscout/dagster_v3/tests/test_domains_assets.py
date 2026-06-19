import importlib.util

from dagster_v3.defs.domains import tables


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.query_results: dict[str, list[tuple[object, ...]]] = {
            "domains": [(2,)],
            "company_website_domains": [(3,)],
        }

    def execute(
        self,
        sql: str,
        params: object | None = None,
    ) -> list[tuple[object, ...]]:
        self.statements.append(sql)
        if "count()" in sql and f"`{tables.DOMAINS_TABLE}`" in sql:
            return self.query_results["domains"]
        if "count()" in sql and f"`{tables.COMPANY_WEBSITE_DOMAINS_TABLE}`" in sql:
            return self.query_results["company_website_domains"]
        return []


def test_domain_defs_package_uses_domain_name() -> None:
    assert importlib.util.find_spec("dagster_v3.defs.domains") is not None
    assert importlib.util.find_spec("dagster_v3.defs.country_domains") is None


def test_replace_domain_clickhouse_tables_uses_stage_exchange(
    monkeypatch,
) -> None:
    from dagster_v3.defs.domains import assets

    stage_names = iter(["domain_dim", "links"])
    monkeypatch.setattr(
        assets.uuid,
        "uuid4",
        lambda: type("U", (), {"hex": next(stage_names)})(),
    )
    client = FakeClickHouseClient()

    counts = assets.replace_domain_clickhouse_tables(client)

    assert counts == {
        "domains": 2,
        "company_website_domains": 3,
    }
    assert client.statements[0] == (
        "CREATE TABLE `corpscout`.`_tmp_domains_domain_dim` AS "
        "`corpscout`.`domains`"
    )
    assert client.statements[1] == (
        "CREATE TABLE `corpscout`.`_tmp_company_website_domains_links` AS "
        "`corpscout`.`company_website_domains`"
    )
    assert any(
        "INSERT INTO `corpscout`.`_tmp_company_website_domains_links`" in statement
        and "`corpscout`.`fi_websites`" in statement
        and "`corpscout`.`no_websites`" in statement
        and "`corpscout`.`wikidata_company_websites`" in statement
        and "source_website_table" in statement
        and "source_website_id" in statement
        for statement in client.statements
    )
    assert any(
        "INSERT INTO `corpscout`.`_tmp_domains_domain_dim`" in statement
        and "`corpscout`.`_tmp_company_website_domains_links`" in statement
        and "country_count" in statement
        for statement in client.statements
    )
    assert (
        "EXCHANGE TABLES `corpscout`.`_tmp_domains_domain_dim` "
        "AND `corpscout`.`domains`"
    ) in client.statements
    assert (
        "EXCHANGE TABLES `corpscout`.`_tmp_company_website_domains_links` "
        "AND `corpscout`.`company_website_domains`"
    ) in client.statements
    assert client.statements[-2:] == [
        "DROP TABLE IF EXISTS `corpscout`.`_tmp_company_website_domains_links`",
        "DROP TABLE IF EXISTS `corpscout`.`_tmp_domains_domain_dim`",
    ]
