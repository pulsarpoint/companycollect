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


def declared_columns(table: str) -> list[str]:
    """Column names in DDL order: lines indented by exactly four spaces before the CONSTRAINT/engine part."""
    names = []
    for line in table_block(table).splitlines():
        match = re.match(r"^    ([a-z_0-9]+) ", line)
        if match:
            names.append(match.group(1))
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
