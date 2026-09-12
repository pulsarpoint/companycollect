"""The person slice-0 drop scripts say exactly what the spec's retirement list says, in
dependency order.

The scripts live in corpscout/clickhouse/operations/ beside the ledger they retire from, and
they are owner-run: nothing in this repo executes them, so this file is the only thing
standing between a typo and a dropped production table. It reads the SQL, parses the object
names out, and compares them as WHOLE names -- corpscout.se_company_person is a prefix of
five entries here AND of the six tables the entity keeps, and company_person_role prefixes
the role catalog that stays.

BOTH SCRIPTS ARE SPENT: they ran on prod on 2026-09-09, and migrations 000398 and 000402
have since given TWO of their DROP names -- corpscout.se_company_person and
corpscout.se_company_person_role -- to live objects. Re-running
se_person_retirement_drops.sql would destroy them. See REUSED_NAMES below.
"""

import re
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[3] / "clickhouse" / "operations"
DROPS = SCRIPTS / "se_person_retirement_drops.sql"
PRECHECK = SCRIPTS / "se_person_retirement_precheck.sql"
POSTCHECK = SCRIPTS / "se_person_retirement_postcheck.sql"

# Spec section 8's retirement list, ordered so every object is dropped after everything that
# reads it. The three source views read se_financial_report_signatories, esef_document_people
# and the wikidata tables -- all KEPT -- so they only have to precede nothing in particular,
# but they go first because the retired assets read THEM. company_management_current is a
# dbt-built table whose model went in Task 6; company_management_current_build is its dbt
# build target, orphaned the same way Task 6 left the model without a writer (controller
# ruling 2026-09-09: nothing in the ledger names it, since dbt created it, not a migration),
# and follows it immediately. company_management_observations is company_management_current's
# history twin, which the same contract wrote and which nothing has read since (controller
# ruling 2026-09-09: it joins the list rather than being left an orphan). se_company_person
# is last: the role, draft, correction, observation and collision tables all key off it.
DROP_ORDER = (
    ("VIEW", "se_company_person_bolagsverket"),
    ("VIEW", "se_company_person_esef"),
    ("VIEW", "se_company_person_wikidata"),
    ("TABLE", "company_management_current"),
    ("TABLE", "company_management_current_build"),
    ("TABLE", "company_management_observations"),
    ("TABLE", "se_company_person_collision_candidate"),
    ("TABLE", "se_company_person_enrichment_observation"),
    ("TABLE", "se_company_person_correction"),
    ("TABLE", "se_company_person_role_draft"),
    ("TABLE", "se_company_person_role"),
    ("TABLE", "se_company_person_v1_role_baseline"),
    ("TABLE", "se_company_person"),
)

# Never droppable, at the names the entity carries TODAY: migration 000398 renamed
# se_company_person_v2 to se_company_person, so the main table joins its five siblings here
# under the live name. Also the role catalog Serbia shares, the raw sources every extractor
# reads, and the serving view.
KEPT = (
    "se_company_person",
    "se_company_person_suggestion",
    "se_company_person_normalized",
    "se_company_person_history",
    "se_company_person_rule",
    "se_company_person_precedence",
    # The slice-5 roles view (migration 000402), living under the name this script's
    # eleventh DROP took away on 2026-09-09.
    "se_company_person_role",
    "company_person_role_type",
    "se_financial_report_signatories",
    "esef_document_people",
    "wikidata_company_people",
    "wikidata_persons",
    "wikidata_company_identifiers",
    "se_companies_serving",
)

# The TWO names on both lists, and the reason this file exists. corpscout.se_company_person
# was the 2026-08-19 table this script dropped on 2026-09-09, and migration 000398 gave the
# freed name to the entity's main table a day later. corpscout.se_company_person_role was
# that same model's role table, dropped in the same run, and migration 000402 gave ITS name
# to the slice-5 roles view. THE SCRIPT IS SPENT -- running it again today would destroy
# 1.1M published persons and the view built over them. It stays in the repo as history
# under the ledger policy and must never be run a second time.
REUSED_NAMES = frozenset({"se_company_person", "se_company_person_role"})

_DROP = re.compile(r"^DROP (TABLE|VIEW) IF EXISTS corpscout\.(\w+);$", re.MULTILINE)


def _statements(path: Path) -> list[tuple[str, str]]:
    return [(kind, name) for kind, name in _DROP.findall(path.read_text(encoding="utf-8"))]


def test_the_drop_script_drops_exactly_the_retirement_list_in_order() -> None:
    assert _statements(DROPS) == list(DROP_ORDER)


def test_the_drop_script_names_no_kept_object() -> None:
    """Whole-name comparison. A substring check would call se_company_person_role a hit on
    the entity's se_company_person_rule, or -- written the other way round -- would call
    company_person_role_type unsafe because company_person_role is being dropped.

    REUSED_NAMES is excluded rather than the check being weakened: two of this script's
    DROPs and two LIVE objects spell the same name for different objects -- dropped on
    2026-09-09, recreated by 000398's rename a day later and by 000402's view on
    2026-09-12."""
    dropped = {name for _, name in _statements(DROPS)}
    assert dropped.isdisjoint(set(KEPT) - REUSED_NAMES)
    assert len(dropped) == len(DROP_ORDER)
    assert "se_company_person_v2" not in dropped
    assert dropped & set(KEPT) == REUSED_NAMES


def test_the_three_source_views_are_the_only_drop_views() -> None:
    """se_company_person_bolagsverket/_esef/_wikidata are plain VIEWs (000330, replaced in
    place by 000331). Everything else on the list is a table, including the dbt-built
    company_management_current -- confirm each engine in the precheck before running."""
    views = [name for kind, name in _statements(DROPS) if kind == "VIEW"]
    assert views == [
        "se_company_person_bolagsverket",
        "se_company_person_esef",
        "se_company_person_wikidata",
    ]


def test_no_script_uses_sync() -> None:
    """SYNC turns each drop into a blocking wait for a full data removal. The owner runs the
    file as one pipe; the default async drop is what the 480-second UNDROP window is
    measured against."""
    for path in (DROPS, PRECHECK, POSTCHECK):
        assert " SYNC" not in path.read_text(encoding="utf-8").upper(), path.name


def test_the_precheck_and_postcheck_cover_the_same_objects() -> None:
    names = [name for _, name in DROP_ORDER]
    for path in (PRECHECK, POSTCHECK):
        sql = path.read_text(encoding="utf-8")
        for name in names:
            assert f"'{name}'" in sql, f"{path.name} does not cover {name}"


def test_the_precheck_reports_the_engine_and_the_row_count() -> None:
    """The engine tells the owner whether a name is a table or a plain view before the drop
    statement assumes it; the row count is what goes in the ledger. se_company_person_v1_role_baseline
    is asset-created and may not exist at all -- its absence from the engine listing is fine."""
    sql = PRECHECK.read_text(encoding="utf-8")
    assert "engine" in sql
    assert "total_rows" in sql
    assert "FROM system.tables" in sql


def test_the_precheck_counts_the_three_plain_views_itself() -> None:
    """system.tables.total_rows is NULL for a plain View, so the three source views would be
    the objects whose size the ledger could not record. They get their own SELECT count()."""
    sql = PRECHECK.read_text(encoding="utf-8")
    for view in ("bolagsverket", "esef", "wikidata"):
        assert f"SELECT count() AS se_company_person_{view}_rows" in sql
        assert f"FROM corpscout.se_company_person_{view}" in sql


def test_the_precheck_gates_on_the_serving_view_being_repointed() -> None:
    """The one reader that had to be off the old tables before they went. Migration 000396
    did that, and this gate proves it landed. The literal stays se_company_person_v2 on
    purpose: the precheck ran on 2026-09-09, before 000398 renamed the entity, and a spent
    operations script is history like a migration file."""
    sql = PRECHECK.read_text(encoding="utf-8")
    assert "se_company_person_v2" in sql
    assert "system.view_refreshes" in sql


def test_the_postcheck_asserts_absence_rather_than_reporting_it() -> None:
    sql = POSTCHECK.read_text(encoding="utf-8")
    assert "count() = 0 AS all_dropped" in sql
    assert "groupArray(name) AS still_present" in sql
