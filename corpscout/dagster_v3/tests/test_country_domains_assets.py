from dagster_v3.defs.country_domains import tables
from dagster_v3.defs.country_domains.assets import replace_country_domain_clickhouse_tables


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.query_results: dict[str, list[tuple[object, ...]]] = {
            "country_domains": [(2,)],
            "company_website_domains": [(3,)],
        }

    def execute(
        self,
        sql: str,
        params: object | None = None,
    ) -> list[tuple[object, ...]]:
        self.statements.append(sql)
        if "count()" in sql and tables.COUNTRY_DOMAINS_TABLE in sql:
            return self.query_results["country_domains"]
        if "count()" in sql and tables.COMPANY_WEBSITE_DOMAINS_TABLE in sql:
            return self.query_results["company_website_domains"]
        return []


def test_replace_country_domain_clickhouse_tables_uses_stage_exchange(
    monkeypatch,
) -> None:
    from dagster_v3.defs.country_domains import assets

    stage_names = iter(["domains", "links"])
    monkeypatch.setattr(
        assets.uuid,
        "uuid4",
        lambda: type("U", (), {"hex": next(stage_names)})(),
    )
    client = FakeClickHouseClient()

    counts = replace_country_domain_clickhouse_tables(client)

    assert counts == {
        "country_domains": 2,
        "company_website_domains": 3,
    }
    assert client.statements[0] == (
        "CREATE TABLE `corpscout`.`_tmp_country_domains_domains` AS "
        "`corpscout`.`country_domains`"
    )
    assert client.statements[1] == (
        "CREATE TABLE `corpscout`.`_tmp_company_website_domains_links` AS "
        "`corpscout`.`company_website_domains`"
    )
    assert any(
        "INSERT INTO `corpscout`.`_tmp_company_website_domains_links`" in statement
        and "`corpscout`.`fi_websites`" in statement
        and "`corpscout`.`no_websites`" in statement
        for statement in client.statements
    )
    assert any(
        "INSERT INTO `corpscout`.`_tmp_country_domains_domains`" in statement
        and "`corpscout`.`_tmp_company_website_domains_links`" in statement
        for statement in client.statements
    )
    assert (
        "EXCHANGE TABLES `corpscout`.`_tmp_country_domains_domains` "
        "AND `corpscout`.`country_domains`"
    ) in client.statements
    assert (
        "EXCHANGE TABLES `corpscout`.`_tmp_company_website_domains_links` "
        "AND `corpscout`.`company_website_domains`"
    ) in client.statements
    assert client.statements[-2:] == [
        "DROP TABLE IF EXISTS `corpscout`.`_tmp_company_website_domains_links`",
        "DROP TABLE IF EXISTS `corpscout`.`_tmp_country_domains_domains`",
    ]
