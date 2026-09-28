"""Company filters use the country summary and live review rules, not provenance."""

from datetime import UTC, datetime

import pytest

from tests.identity_registration_support import identity_postgres as identity_postgres
from tests.test_domain_sources import (
    association,
    insert_company,
    publish,
)
from tests.test_domain_sources import (
    compact_database as compact_database,
)
from tests.test_domain_sources import (
    database as database,
)
from tests.test_domain_sources import (
    server as server,
)
from tests.test_processing_store import (
    processing_postgres_url as processing_postgres_url,
)

pytestmark = pytest.mark.usefixtures("identity_postgres")


def refresh(client):
    client.execute("SYSTEM REFRESH VIEW corpscout.domains_company_filter")
    client.execute("SYSTEM WAIT VIEW corpscout.domains_company_filter")
    return dict(client.execute("SELECT root_domain,company_count FROM corpscout.domains_company_filter"))


def test_company_count_does_not_depend_on_contribution_membership(compact_database):
    client = compact_database
    insert_company(client, association())
    insert_company(client, association(company="5569999999"))
    # Before index publication the relationship is already available.
    assert refresh(client)["new.se"] == 2
    publish(client)
    assert client.execute("SELECT count() FROM corpscout.domains_sources WHERE domain_id=lower(hex(SHA256('new.se')))") == [(1,)]
    # Unsupported contributor names cannot invent a company association.
    client.execute("INSERT INTO corpscout.domains_sources SELECT * REPLACE ('no_company_domain' AS source_table) FROM corpscout.domains_sources")
    assert refresh(client)["new.se"] == 2


@pytest.mark.parametrize("join_use_nulls", [0, 1])
def test_review_changes_filter_without_rewriting_index(compact_database, join_use_nulls):
    client = compact_database
    client.execute("SET join_use_nulls=%(value)s", {"value":join_use_nulls})
    before = client.execute("SELECT * FROM corpscout.domains_sources ORDER BY domain_id,source_table")
    for i, (action, active) in enumerate((("rejected",False),("confirmed_related",True),("unreviewed",True))):
        client.execute("INSERT INTO corpscout.se_company_domain_rule VALUES", [
            ("5561552760","legacy.se",action,int(action=="unreviewed"),"test","","",datetime(2026,9,28,0,0,i,tzinfo=UTC))])
        assert ("legacy.se" in refresh(client)) == active
    assert client.execute("SELECT * FROM corpscout.domains_sources ORDER BY domain_id,source_table") == before


def test_withdrawal_removes_company_count_but_retains_contribution(compact_database):
    client = compact_database
    row = association()
    insert_company(client, row)
    publish(client)
    assert refresh(client)["new.se"] == 1
    insert_company(client, {**row,"active":0,"association":"not_connected","folded_at":datetime(2026,9,29,tzinfo=UTC)})
    publish(client)
    assert "new.se" not in refresh(client)
    assert client.execute("SELECT count() FROM corpscout.domains_sources WHERE domain_id=lower(hex(SHA256('new.se')))") == [(1,)]
