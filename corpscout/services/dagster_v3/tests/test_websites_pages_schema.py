"""Inventory identities and constraints against a real, disposable ClickHouse."""

import hashlib
import json
import subprocess
from pathlib import Path

import pytest

from tests.clickhouse_local import clickhouse_local_command


def migration_sql(direction: str) -> str:
    path = Path(__file__).resolve().parents[3] / "clickhouse/migrations"
    return (path / f"000442_corpscout_websites_and_pages.{direction}.sql").read_text(
        encoding="utf-8"
    )


def test_inventory_identity_evidence_and_migration_roundtrip() -> None:
    sql = """
    SELECT count() FROM corpscout.websites;
    SELECT count() FROM corpscout.pages;
    INSERT INTO corpscout.websites
      (root_domain,website_origin,sources,first_seen_at,last_seen_at,last_observed_at)
    VALUES
      ('example.com','https://example.com',['se_company_domain'],'2026-09-24','2026-09-24',NULL),
      ('example.com','https://shop.example.com',['commoncrawl'],'2026-09-24','2026-09-24','2026-01-01'),
      ('example.com','http://example.com',['commoncrawl'],'2026-09-24','2026-09-24','2026-01-02');
    INSERT INTO corpscout.pages
      (root_domain,website_origin,page_url,sources,first_seen_at,last_seen_at,last_observed_at)
    VALUES
      ('example.com','https://example.com','https://example.com/',['se_company_domain'],'2026-09-24','2026-09-24',NULL),
      ('example.com','https://shop.example.com','https://shop.example.com/products?a=1',['commoncrawl'],'2026-09-24','2026-09-24','2026-01-01'),
      ('example.com','https://shop.example.com','https://shop.example.com/products?a=2',['commoncrawl'],'2026-09-24','2026-09-24','2026-01-01');
    SELECT uniqExact(website_id) FROM corpscout.websites;
    SELECT uniqExact(page_id) FROM corpscout.pages;
    SELECT count() FROM corpscout.pages p INNER JOIN corpscout.websites w
      ON p.website_id = w.website_id AND p.root_domain = w.root_domain;
    SELECT page_id,website_id,evidence_status,last_successful_fetch_at
      FROM corpscout.pages ORDER BY page_url FORMAT JSONCompactEachRow;
    """
    result = subprocess.run(
        clickhouse_local_command(),
        input=migration_sql("up") + migration_sql("up") + sql
        + migration_sql("down") + migration_sql("down")
        + "EXISTS TABLE corpscout.pages; EXISTS TABLE corpscout.websites;",
        capture_output=True, text=True, check=False, timeout=60,
    )
    assert result.returncode == 0, result.stderr
    lines = result.stdout.strip().splitlines()
    assert lines[:5] == ["0", "0", "3", "3", "3"]
    for row, url, origin, status in zip(
        lines[5:8],
        ["https://example.com/", "https://shop.example.com/products?a=1", "https://shop.example.com/products?a=2"],
        ["https://example.com", "https://shop.example.com", "https://shop.example.com"],
        ["assumed", "observed", "observed"],
        strict=True,
    ):
        assert json.loads(row) == [
            hashlib.sha256(url.encode()).hexdigest(),
            hashlib.sha256(origin.encode()).hexdigest(), status, None,
        ]
    assert lines[8:] == ["0", "0"]


@pytest.mark.parametrize(
    ("origin", "page", "sources", "constraint"),
    [
        ("https://other.com", "https://other.com/", "['commoncrawl']", "valid_origin"),
        ("https://example.com/", "https://example.com//", "['commoncrawl']", "valid_origin"),
        ("https://example.com", "https://other.com/", "['commoncrawl']", "valid_page"),
        ("https://example.com", "https://example.com/#fragment", "['commoncrawl']", "valid_page"),
        ("https://example.com", "https://example.com/", "[]", "valid_sources"),
    ],
)
def test_invalid_inventory_rows_are_rejected(
    origin: str, page: str, sources: str, constraint: str,
) -> None:
    # Test-controlled SQL literals only.
    sql = f"""
    INSERT INTO corpscout.pages
      (root_domain,website_origin,page_url,sources,first_seen_at,last_seen_at)
    VALUES ('example.com','{origin}','{page}',{sources},'2026-09-24','2026-09-24');
    """
    result = subprocess.run(
        clickhouse_local_command(), input=migration_sql("up") + sql,
        capture_output=True, text=True, check=False, timeout=60,
    )
    assert result.returncode != 0
    assert constraint in result.stderr
    assert "violated" in result.stderr
