"""Execute the selection SQL and Copy interception against their real runtimes."""

import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest
from cloakbrowser import launch
from cloakbrowser.config import get_binary_path
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

from dagster_v3.defs.company_domains.assets import PENDING_COMPANIES_SQL, RESULT_COLUMNS
from dagster_v3.defs.company_domains.browser import copy_brave_answer
from tests.clickhouse_local import clickhouse_local_command, render

pytestmark = pytest.mark.integration
MIGRATION = (
    Path(__file__).parents[3]
    / "clickhouse/migrations/000409_corpscout_company_brave_search_results.up.sql"
)


def test_real_clickhouse_selects_only_pending_active_named_companies():
    schema = """
CREATE TABLE corpscout.se_company_basic_info
    (company_id String, legal_name Nullable(String), status String)
    ENGINE=ReplacingMergeTree ORDER BY company_id;
INSERT INTO corpscout.se_company_basic_info VALUES
    ('1','Fresh AB','active'), ('2','Renamed AB','active'), ('3','Retry AB','active'),
    ('4','Inactive AB','inactive'), ('5','   ','active'), ('6',NULL,'active'),
    ('7','Prompt AB','active'), ('8','Expired AB','active'), ('9','Other country AB','active');
INSERT INTO corpscout.company_brave_search_results
    (country_code,company_id,company_name,prompt_version,status,fetched_at,source_run_id)
VALUES
    ('SE','1','Fresh AB','company-info-v1','success','2026-09-14 00:00:00','a'),
    ('SE','2','Old AB','company-info-v1','success','2026-09-14 00:00:00','a'),
    ('SE','3','Retry AB','company-info-v1','error','2026-09-14 00:00:00','a'),
    ('SE','7','Prompt AB','old-prompt','success','2026-09-14 00:00:00','a'),
    ('SE','8','Expired AB','company-info-v1','success','2026-07-01 00:00:00','a'),
    ('NO','9','Other country AB','company-info-v1','success','2026-09-14 00:00:00','a');
"""
    params = {
        "prompt_version": "company-info-v1",
        "freshness_cutoff": datetime(2026, 8, 16, tzinfo=UTC),
        "after_company_id": "",
        "all_companies": True,
        "company_ids": ("",),
        "page_size": 100,
    }
    statements = [MIGRATION.read_text(), schema]
    expectations = [
        (
            {},
            [
                ["2", "Renamed AB"],
                ["3", "Retry AB"],
                ["7", "Prompt AB"],
                ["8", "Expired AB"],
                ["9", "Other country AB"],
            ],
        ),
        (
            {"after_company_id": "3", "page_size": 2},
            [["7", "Prompt AB"], ["8", "Expired AB"]],
        ),
        (
            {"all_companies": False, "company_ids": ("1", "3", "9")},
            [["3", "Retry AB"], ["9", "Other country AB"]],
        ),
    ]
    expected = []
    for overrides, rows in expectations:
        statements.append(
            render(PENDING_COMPANIES_SQL, {**params, **overrides})
            + " FORMAT JSONCompactEachRow;"
        )
        expected.extend(rows)
    statements.append(
        "SELECT name FROM system.columns WHERE database='corpscout' AND table='company_brave_search_results' ORDER BY position FORMAT JSONCompactEachRow;"
    )
    expected.extend([[column] for column in RESULT_COLUMNS])
    result = subprocess.run(
        clickhouse_local_command(),
        input="\n".join(statements),
        capture_output=True,
        text=True,
        timeout=90,
    )
    assert result.returncode == 0, result.stderr
    assert [
        json.loads(line) for line in result.stdout.splitlines() if line.strip()
    ] == expected


COPY_PAGE = """<!doctype html><html><body>
<input data-testid="searchbox">
<button id="more" hidden>More</button><button id="copy" hidden>Copy</button>
<script>
const input = document.querySelector('input');
input.addEventListener('keydown', (event) => {
    if (event.key === 'Enter') document.querySelector('#more').hidden = false;
});
document.querySelector('#more').onclick = () => { document.querySelector('#copy').hidden = false; };
document.querySelector('#copy').onclick = async () => {
    await navigator.clipboard.writeText(input.value === 'empty' ? '' : 'Answer: ' + input.value + '\\nhttps://example.se/');
};
</script></body></html>"""


def test_copy_capture_is_per_page_and_empty_answers_never_reuse_previous_text(
    monkeypatch,
):
    if not get_binary_path().exists():
        pytest.skip("CloakBrowser binary is not installed")
    monkeypatch.setenv("CLOAKBROWSER_AUTO_UPDATE", "false")
    browser = launch(headless=True)
    try:
        context = browser.new_context()
        # All browser traffic stays on this fixture; no requests reach Brave.
        context.route(
            "**/*",
            lambda route: route.fulfill(content_type="text/html", body=COPY_PAGE),
        )
        first, second = context.new_page(), context.new_page()
        assert (
            copy_brave_answer(first, "+1 Kommunikationsbyrå AB", timeout_ms=5_000)
            == "Answer: +1 Kommunikationsbyrå AB\nhttps://example.se/"
        )
        assert (
            copy_brave_answer(second, "Skanska AB", timeout_ms=5_000)
            == "Answer: Skanska AB\nhttps://example.se/"
        )
        assert (
            first.evaluate("() => window.__companyBraveCopiedText")
            == "Answer: +1 Kommunikationsbyrå AB\nhttps://example.se/"
        )
        with pytest.raises(PlaywrightTimeoutError):
            copy_brave_answer(first, "empty", timeout_ms=500)
        context.close()
    finally:
        browser.close()
