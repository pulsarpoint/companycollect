"""Prepare the migration-owned claim tables for disposable source-adapter tests."""

from tests.domain_sources_schema import MIGRATIONS, central_schema_sql


def claim_schema_sql():
    migration = (
        MIGRATIONS / "000466_corpscout_domain_result_references.up.sql"
    ).read_text()
    return (
        central_schema_sql()
        + migration[
            migration.index(
                "CREATE TABLE IF NOT EXISTS corpscout.se_company_domain_sources"
            ) : migration.index("-- Refresh view schemas")
        ]
    )
