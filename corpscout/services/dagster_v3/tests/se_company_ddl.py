"""Shared by the se_company tests: reads the migration DDL, never a registry."""
import re
from pathlib import Path

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
MIGRATION = "000297_corpscout_se_company_info.up.sql"
ENVELOPE = ("company_id", "source_record_uid", "observed_at", "source_run_id", "evidence_hash")
FINAL_PROVENANCE = ("source_record_uids", "evidence_hashes", "evidence_set_hash", "correction_ids",
                    "suggestion_id", "model_provider", "model_name", "prompt_version", "source_run_id", "resolved_at")


def _sql() -> str:
    return (MIGRATIONS_DIR / MIGRATION).read_text(encoding="utf-8")


def table_block(table: str) -> str:
    """The one CREATE TABLE statement for `table`, up to and including its terminating semicolon."""
    sql = _sql()
    start = sql.index(f"CREATE TABLE IF NOT EXISTS corpscout.{table}\n")
    return sql[start : sql.index(";", start) + 1]


def _added_columns(table: str) -> list[tuple[str, str | None]]:
    """``(column, after)`` for every ``ADD COLUMN`` a migration later than 000297 aims at `table`.

    The pilot's tables outgrew their first migration (000300 adds
    ``se_company_info_scb.activity_description_en``), and what these tests pin is the
    *deployed* column list, not 000297's snapshot of it -- so ``declared_columns``
    replays later ADD COLUMNs rather than each test hard-coding what they added.
    Migration file names sort in ledger order (zero-padded), and only ADD COLUMN is
    replayed: ADD CONSTRAINT (000299) and MODIFY COLUMN (000300) change no column list.
    """
    added: list[tuple[str, str | None]] = []
    for path in sorted(MIGRATIONS_DIR.glob("[0-9]*.up.sql")):
        if path.name <= MIGRATION:
            continue
        for raw in path.read_text(encoding="utf-8").split(";"):
            statement = "\n".join(line for line in raw.splitlines() if not line.strip().startswith("--"))
            if not re.search(rf"ALTER TABLE corpscout\.{table}\b", statement):
                continue
            for line in statement.splitlines():
                name = re.match(r"\s*ADD COLUMN(?: IF NOT EXISTS)? ([a-z_0-9]+) ", line)
                if name:
                    after = re.search(r" AFTER ([a-z_0-9]+),?\s*$", line)
                    added.append((name.group(1), after.group(1) if after else None))
    return added


def declared_columns(table: str) -> list[str]:
    """Column names in deployed DDL order: 000297's own, then later ADD COLUMNs in place.

    000297's are the lines indented by exactly four spaces before the CONSTRAINT/engine part;
    a later ADD COLUMN lands right after the column its ``AFTER`` names (last, without one).
    """
    names = []
    for line in table_block(table).splitlines():
        match = re.match(r"^    ([a-z_0-9]+) ", line)
        if match:
            names.append(match.group(1))
    for column, after in _added_columns(table):
        if column in names:
            continue
        names.insert(names.index(after) + 1 if after in names else len(names), column)
    return names


def artifact_tables() -> list[str]:
    return sorted(set(re.findall(r"CREATE TABLE IF NOT EXISTS corpscout\.(se_company_info_(?!correction|enrichment)[a-z0-9_]+)\n", _sql())))


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
