"""Shared by the se_company tests: reads the migration DDL, never a registry."""
import re
from functools import lru_cache
from pathlib import Path

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
MIGRATION = "000297_corpscout_se_company_info.up.sql"
ADDRESS_MIGRATION = "000307_corpscout_se_company_address.up.sql"
ENVELOPE = ("company_id", "source_record_uid", "observed_at", "source_run_id", "evidence_hash")
FINAL_PROVENANCE = ("source_record_uids", "evidence_hashes", "evidence_set_hash", "correction_ids",
                    "suggestion_id", "model_provider", "model_name", "prompt_version", "source_run_id", "resolved_at")


def _sql() -> str:
    return (MIGRATIONS_DIR / MIGRATION).read_text(encoding="utf-8")


@lru_cache(maxsize=None)
def _migration_for(table: str) -> str:
    """The migration file whose CREATE TABLE declares `table`.

    The se_company layer no longer lives in one migration (000297 declares the info
    tables, 000307 the address ones), so the helpers below locate the creating file
    instead of reading a single constant -- every existing caller keeps its signature.
    Exactly one migration may create a given table; two would mean a rename-swap, which
    these helpers do not model and which would break the ALTER replay below.
    """
    matches = [
        path.name
        for path in sorted(MIGRATIONS_DIR.glob("[0-9]*.up.sql"))
        if f"CREATE TABLE IF NOT EXISTS corpscout.{table}\n" in path.read_text(encoding="utf-8")
    ]
    if len(matches) != 1:
        raise AssertionError(f"expected one migration creating corpscout.{table}, found {matches}")
    return matches[0]


def table_block(table: str) -> str:
    """The one CREATE TABLE statement for `table`, up to and including its terminating semicolon."""
    sql = (MIGRATIONS_DIR / _migration_for(table)).read_text(encoding="utf-8")
    start = sql.index(f"CREATE TABLE IF NOT EXISTS corpscout.{table}\n")
    return sql[start : sql.index(";", start) + 1]


def _column_changes(table: str) -> list[tuple[str, str, str | None]]:
    """``(op, column, after)`` for every ADD/DROP COLUMN a migration later than 000297
    aims at `table`, in ledger order -- ``op`` is ``"add"`` or ``"drop"``, and ``after``
    is only ever set on an add.

    The pilot's tables outgrew their first migration (000300 adds
    ``se_company_info_scb.activity_description_en``; 000304 adds
    ``se_company_info.llm_enhanced`` and drops ``description_source``), and what these
    tests pin is the *deployed* column list, not 000297's snapshot of it -- so
    ``declared_columns`` replays the later ALTERs rather than each test hard-coding what
    they changed. Migration file names sort in ledger order (zero-padded), and only ADD
    COLUMN / DROP COLUMN are replayed: ADD CONSTRAINT (000299) and MODIFY COLUMN (000300)
    change no column list. Order is preserved ACROSS the two kinds, not just within one:
    000304 positions its new column against a column its own next statement removes.

    Format this relies on, for whoever writes the next ALTER: one clause per line, the
    line starting with ``ADD COLUMN`` (optionally ``IF NOT EXISTS``) or ``DROP COLUMN``
    (optionally ``IF EXISTS``) followed by the column name, and -- for an add whose
    position matters -- ``AFTER <column>`` ending that same line (a trailing comma is
    fine). A clause wrapped across lines, or an ``AFTER`` that is not last on its line,
    is read as "appended at the end" and the layout test will say so. ``DROP
    CONSTRAINT`` (000299) is not ``DROP COLUMN`` and is ignored, as is anything in a
    ``.down.sql`` file: only ``*.up.sql`` is replayed.
    """
    changes: list[tuple[str, str, str | None]] = []
    created = _migration_for(table)
    for path in sorted(MIGRATIONS_DIR.glob("[0-9]*.up.sql")):
        if path.name <= created:
            continue
        for raw in path.read_text(encoding="utf-8").split(";"):
            statement = "\n".join(line for line in raw.splitlines() if not line.strip().startswith("--"))
            if not re.search(rf"ALTER TABLE corpscout\.{table}\b", statement):
                continue
            for line in statement.splitlines():
                added = re.match(r"\s*ADD COLUMN(?: IF NOT EXISTS)? ([a-z_0-9]+) ", line)
                if added:
                    after = re.search(r" AFTER ([a-z_0-9]+),?\s*$", line)
                    changes.append(("add", added.group(1), after.group(1) if after else None))
                    continue
                dropped = re.match(r"\s*DROP COLUMN(?: IF EXISTS)? ([a-z_0-9]+),?\s*$", line)
                if dropped:
                    changes.append(("drop", dropped.group(1), None))
    return changes


def declared_columns(table: str) -> list[str]:
    """Column names in deployed DDL order: 000297's own, then later ALTERs replayed in place.

    000297's are the lines indented by exactly four spaces before the CONSTRAINT/engine part;
    a later ADD COLUMN lands right after the column its ``AFTER`` names (last, without one),
    and a later DROP COLUMN removes one. Both are replayed in ledger order, so an add
    positioned against a column a later statement drops still lands where it belongs.
    """
    names = []
    for line in table_block(table).splitlines():
        match = re.match(r"^    ([a-z_0-9]+) ", line)
        if match:
            names.append(match.group(1))
    for op, column, after in _column_changes(table):
        if op == "drop":
            if column in names:
                names.remove(column)
        elif column not in names:  # IF NOT EXISTS: a re-added column keeps its place
            names.insert(names.index(after) + 1 if after in names else len(names), column)
    return names


def artifact_tables() -> list[str]:
    return sorted(set(re.findall(r"CREATE TABLE IF NOT EXISTS corpscout\.(se_company_info_(?!correction|enrichment)[a-z0-9_]+)\n", _sql())))


def address_artifact_tables() -> list[str]:
    """The address datatype's artifact tables (the final and the ledger are not artifacts)."""
    sql = (MIGRATIONS_DIR / ADDRESS_MIGRATION).read_text(encoding="utf-8")
    return sorted(set(re.findall(
        r"CREATE TABLE IF NOT EXISTS corpscout\.(se_company_address_(?!correction)[a-z0-9_]+)\n", sql)))


def projection_aliases(sql: str) -> list[str]:
    """The ordered `AS <name>` aliases of an artifact SELECT constant's outermost
    (trailing, unindented) projection -- the one that actually determines the
    order `publish_with_stage` binds to the positional insert-column list, so a
    swapped pair of same-typed columns here would insert transposed values with
    an otherwise-green suite. Every SE_COMPANY_INFO_*_SQL constant ends in a
    top-level `SELECT ... FROM ...` (no leading indent) after its last CTE
    closes; CTE-internal SELECTs are indented, so `\\nSELECT\\n` unambiguously
    finds only the trailing one. That projection must alias every column
    (`col AS col` where the name doesn't otherwise change) so this stays a
    simple regex rather than a real SQL parser.
    """
    trailing_select = sql.rindex("\nSELECT\n")
    projection_end = sql.index("\nFROM ", trailing_select)
    projection = sql[trailing_select:projection_end]
    return re.findall(r"AS (\w+)", projection)
