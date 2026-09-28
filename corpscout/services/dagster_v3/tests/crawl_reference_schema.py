"""Apply actual reference migration fragments to focused disposable fixtures."""

import re

from tests.domain_sources_schema import MIGRATIONS, central_schema_sql, execute_sql


def upgrade_references(client):
    execute_sql(client, central_schema_sql())
    sql = (MIGRATIONS / "000466_corpscout_domain_result_references.up.sql").read_text()
    for statement in sql.split(";"):
        match = re.search(r"ALTER TABLE corpscout\.(\w+)", statement)
        if match and client.execute(f"EXISTS TABLE corpscout.{match[1]}") == [(1,)]:
            client.execute(statement)
        match = re.search(r"CREATE OR REPLACE VIEW corpscout\.(website_\w+_requests_current)", statement)
        if match and client.execute(f"EXISTS TABLE corpscout.{match[1].removesuffix('_current')}") == [(1,)]:
            client.execute(statement)
