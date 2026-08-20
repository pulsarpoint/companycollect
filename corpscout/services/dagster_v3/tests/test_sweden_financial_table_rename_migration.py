from pathlib import Path


MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"


def test_bolagsverket_financial_metrics_rename_is_metadata_only() -> None:
    up_sql = _migration_sql(
        "000285_corpscout_se_bolagsverket_financial_metrics_rename.up.sql"
    )
    down_sql = _migration_sql(
        "000285_corpscout_se_bolagsverket_financial_metrics_rename.down.sql"
    )

    assert (
        "RENAME TABLE corpscout.se_financial_metrics TO "
        "corpscout.se_bolagsverket_financial_metrics"
    ) in _normalize_sql(up_sql)
    assert (
        "RENAME TABLE corpscout.se_bolagsverket_financial_metrics TO "
        "corpscout.se_financial_metrics"
    ) in _normalize_sql(down_sql)

    for sql in (up_sql, down_sql):
        normalized = _normalize_sql(sql).upper()
        for forbidden_statement in (
            "CREATE TABLE",
            "INSERT INTO",
            "TRUNCATE TABLE",
            "DROP TABLE",
        ):
            assert forbidden_statement not in normalized


def _migration_sql(file_name: str) -> str:
    return (MIGRATIONS_DIR / file_name).read_text(encoding="utf-8")


def _normalize_sql(sql: str) -> str:
    return " ".join(sql.replace(";", "").split())
