import importlib.util

from dagster import AssetKey

from dagster_v3.defs.czech_ares import contacts as czech_ares_contacts
from dagster_v3.defs.domains import tables
from dagster_v3.defs.latvia_ur import contacts as latvia_ur_contacts

LEGACY_WEBSITE_TABLES = (
    "fi_websites",
    "no_websites",
    "wikidata_company_websites",
    "br_websites",
    "wikidata_companies",
)

EXPECTED_DOMAINS_DEPS = {
    "czech_ares_clickhouse_company_contacts",
    "latvia_ur_clickhouse_company_contacts",
    "estonia_ar_clickhouse_company_domains",
    "brazil_comp_rfb_clickhouse_company_domains",
    "norway_brreg_clickhouse_canonical_contacts",
    "finland_ytj_clickhouse_canonical_contacts",
    "wikidata_clickhouse_canonical_contacts",
}


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


def test_canonical_domain_sources_config() -> None:
    tables_by_name = {
        source["table"]: source for source in tables.CANONICAL_DOMAIN_SOURCES
    }
    assert set(tables_by_name) == {
        "cz_company_domains",
        "lv_company_domains",
        "ee_company_domains",
        "br_company_domains",
        "no_company_domains",
        "fi_company_domains",
        "wikidata_company_domains",
    }
    assert tables_by_name["cz_company_domains"] == {
        "table": "cz_company_domains",
        "registry_id_type": "ico",
        "source_slug": "czech_ares",
    }
    assert tables_by_name["lv_company_domains"] == {
        "table": "lv_company_domains",
        "registry_id_type": "regcode",
        "source_slug": "latvia_ur",
    }
    assert tables_by_name["ee_company_domains"] == {
        "table": "ee_company_domains",
        "registry_id_type": "reg_code",
        "source_slug": "estonia_ar",
    }
    assert tables_by_name["br_company_domains"] == {
        "table": "br_company_domains",
        "registry_id_type": "cnpj_basico",
        "source_slug": "brazil_rfb",
    }
    assert tables_by_name["no_company_domains"] == {
        "table": "no_company_domains",
        "registry_id_type": "org_number",
        "source_slug": "norway_brreg",
    }
    assert tables_by_name["fi_company_domains"] == {
        "table": "fi_company_domains",
        "registry_id_type": "business_id",
        "source_slug": "finland_ytj",
    }
    assert tables_by_name["wikidata_company_domains"] == {
        "table": "wikidata_company_domains",
        "registry_id_type": "wikidata_id",
        "source_slug": "wikidata",
    }


def test_canonical_domain_sources_cross_checked_against_registry_id_type() -> None:
    tables_by_name = {
        source["table"]: source for source in tables.CANONICAL_DOMAIN_SOURCES
    }
    assert (
        tables_by_name["cz_company_domains"]["registry_id_type"]
        == czech_ares_contacts.REGISTRY_ID_TYPE
    )
    assert (
        tables_by_name["lv_company_domains"]["registry_id_type"]
        == latvia_ur_contacts.REGISTRY_ID_TYPE
    )


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
        "CREATE TABLE `corpscout`.`_tmp_domains_domain_dim` AS `corpscout`.`domains`"
    )
    assert client.statements[1] == (
        "CREATE TABLE `corpscout`.`_tmp_company_website_domains_links` AS "
        "`corpscout`.`company_website_domains`"
    )

    insert_statement = next(
        statement
        for statement in client.statements
        if "INSERT INTO `corpscout`.`_tmp_company_website_domains_links`" in statement
    )

    for table in (
        "cz_company_domains",
        "lv_company_domains",
        "ee_company_domains",
        "br_company_domains",
        "no_company_domains",
        "fi_company_domains",
        "wikidata_company_domains",
    ):
        assert f"FROM `corpscout`.`{table}`" in insert_statement

    for legacy_table in LEGACY_WEBSITE_TABLES:
        assert legacy_table not in insert_statement

    for source in tables.CANONICAL_DOMAIN_SOURCES:
        assert f"'{source['source_slug']}' AS source_slug" in insert_statement
        assert f"'{source['registry_id_type']}' AS company_id_type" in insert_statement

    assert "websites.registry_id AS company_id" in insert_statement
    assert "source_website_table" in insert_statement
    assert "source_website_id" in insert_statement
    assert "domain_source" in insert_statement

    assert any(
        "INSERT INTO `corpscout`.`_tmp_domains_domain_dim`" in statement
        and "`corpscout`.`_tmp_company_website_domains_links`" in statement
        and "country_count" in statement
        for statement in client.statements
    )

    links_exchange = (
        "EXCHANGE TABLES `corpscout`.`_tmp_company_website_domains_links` "
        "AND `corpscout`.`company_website_domains`"
    )
    domains_exchange = (
        "EXCHANGE TABLES `corpscout`.`_tmp_domains_domain_dim` "
        "AND `corpscout`.`domains`"
    )
    assert links_exchange in client.statements
    assert domains_exchange in client.statements
    assert client.statements.index(links_exchange) < client.statements.index(
        domains_exchange
    )

    assert client.statements[-2:] == [
        "DROP TABLE IF EXISTS `corpscout`.`_tmp_company_website_domains_links`",
        "DROP TABLE IF EXISTS `corpscout`.`_tmp_domains_domain_dim`",
    ]


def test_domains_clickhouse_deps_are_the_seven_canonical_producers() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    parents = {
        parent.path[-1]
        for parent in repo.asset_graph.get(AssetKey("domains_clickhouse")).parent_keys
    }

    assert parents == EXPECTED_DOMAINS_DEPS
