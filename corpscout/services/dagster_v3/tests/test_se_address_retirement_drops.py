"""The slice-4c drop scripts say exactly what the spec's handoff list says, in its order.

The scripts live in corpscout/clickhouse/operations/ beside the ledger they retire from, and
they are owner-run: nothing in this repo executes them, so this file is the only thing
standing between a typo and a dropped production table. It reads the SQL, parses the object
names out, and compares them to the list -- as WHOLE names, because
corpscout.se_company_address (the entity, kept for good) is a prefix of five entries here.
"""

import re
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[3] / "clickhouse" / "operations"
DROPS = SCRIPTS / "se_address_retirement_drops.sql"
PRECHECK = SCRIPTS / "se_address_retirement_precheck.sql"
POSTCHECK = SCRIPTS / "se_address_retirement_postcheck.sql"

# Spec section 9, the slice-4b shipped record's handoff order, plus the serving view 000392
# parked under _retired. Each entry is dropped only after everything that reads it:
# se_address_geocodes_served LEFT JOINs se_addresses_current, and it reads
# se_address_geocodes_current as its precise base, so the view goes between them.
DROP_ORDER = (
    ("TABLE", "se_companies_serving_retired"),
    ("TABLE", "se_company_address_legacy"),
    ("TABLE", "se_company_address_scb"),
    ("TABLE", "se_company_address_bolagsverket"),
    ("TABLE", "se_company_address_correction"),
    ("TABLE", "se_company_addresses"),
    ("TABLE", "se_company_addresses_current"),
    ("TABLE", "se_company_address_members_current"),
    ("VIEW", "se_address_geocodes_served"),
    ("TABLE", "se_addresses_current"),
    ("TABLE", "se_company_address_links_current"),
    ("TABLE", "se_address_geocodes_current"),
)

# Never droppable. se_company_address is the entity itself; the three below it are the
# geocode cache and the two centroid references the spec keeps "for good".
KEPT = (
    "se_company_address",
    "se_company_address_suggestion",
    "se_company_address_normalized",
    "se_company_address_history",
    "se_company_address_rule",
    "se_company_address_precedence",
    "se_address_geocodes",
    "se_postcode_centroids",
    "se_city_centroids",
    "se_companies_serving",
    "se_company_basic_info",
    "se_scb_companies",
    "se_bolagsverket_companies",
)

_DROP = re.compile(
    r"^DROP (TABLE|VIEW) IF EXISTS corpscout\.(\w+);$", re.MULTILINE
)


def _statements(path: Path) -> list[tuple[str, str]]:
    return [(kind, name) for kind, name in _DROP.findall(path.read_text(encoding="utf-8"))]


def test_the_drop_script_drops_exactly_the_handoff_list_in_order() -> None:
    assert _statements(DROPS) == list(DROP_ORDER)


def test_the_drop_script_names_no_kept_object() -> None:
    """Whole-name comparison. A substring check would call se_company_address_legacy a hit
    on the entity and refuse a legitimate drop -- or, written the other way round, would
    call the entity safe because a longer name containing it is on the drop list."""
    dropped = {name for _, name in _statements(DROPS)}
    assert dropped.isdisjoint(KEPT)
    assert len(dropped) == len(DROP_ORDER)


def test_the_served_overlay_is_the_only_drop_view() -> None:
    """se_address_geocodes_served is a plain VIEW (000325, replaced in place by 000327).
    se_address_geocodes_current is a REFRESHABLE MATERIALIZED VIEW since 000320, and
    DROP TABLE is what removes one -- migration 000392 did exactly that to
    se_companies_serving_retired on production, successfully."""
    views = [name for kind, name in _statements(DROPS) if kind == "VIEW"]
    assert views == ["se_address_geocodes_served"]


def test_no_script_uses_sync() -> None:
    """SYNC turns each drop into a blocking wait for a full data removal. These are large
    tables and the owner runs the file as one pipe; the default async drop is what the
    480-second UNDROP window is measured against."""
    for path in (DROPS, PRECHECK, POSTCHECK):
        assert " SYNC" not in path.read_text(encoding="utf-8").upper(), path.name


def test_the_precheck_and_postcheck_cover_the_same_objects() -> None:
    names = [name for _, name in DROP_ORDER]
    for path in (PRECHECK, POSTCHECK):
        sql = path.read_text(encoding="utf-8")
        for name in names:
            assert f"'{name}'" in sql, f"{path.name} does not cover {name}"


def test_the_precheck_reports_the_engine_and_the_row_count() -> None:
    """The engine tells the owner whether a name is a table, a plain view or a refreshable
    MV before the drop statement assumes it; the row count is what goes in the ledger."""
    sql = PRECHECK.read_text(encoding="utf-8")
    assert "engine" in sql
    assert "total_rows" in sql
    assert "FROM system.tables" in sql


def test_the_precheck_counts_the_plain_view_itself() -> None:
    """system.tables.total_rows is NULL for a plain View, so the served overlay would be the
    one object whose size the ledger could not record. It gets its own SELECT count()."""
    sql = PRECHECK.read_text(encoding="utf-8")
    assert "SELECT count() AS se_address_geocodes_served_rows" in sql
    assert "FROM corpscout.se_address_geocodes_served" in sql


def test_the_postcheck_asserts_absence_rather_than_reporting_it() -> None:
    sql = POSTCHECK.read_text(encoding="utf-8")
    assert "count() = 0 AS all_dropped" in sql
    assert "groupArray(name) AS still_present" in sql
