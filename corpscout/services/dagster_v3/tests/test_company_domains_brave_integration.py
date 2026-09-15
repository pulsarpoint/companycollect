"""Execute the selection SQL and Copy interception against their real runtimes."""

import json
import subprocess
from pathlib import Path

import pytest
from cloakbrowser import launch
from cloakbrowser.config import get_binary_path
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

from dagster_v3.defs.company_domains.assets import EXPORT_COLUMNS
from dagster_v3.defs.company_domains.browser import copy_brave_answer
from tests.clickhouse_local import clickhouse_local_command

pytestmark = pytest.mark.integration
MIGRATION = (
    Path(__file__).parents[3]
    / "clickhouse/migrations/000410_corpscout_company_brave_info.up.sql"
)


def test_named_input_view_filters_companies_and_replayed_results_are_deduplicated():
    sql = (
        """
CREATE DATABASE corpscout;
CREATE TABLE corpscout.se_company_basic_info
    (company_id String, legal_name Nullable(String), status String)
    ENGINE=ReplacingMergeTree ORDER BY company_id;
INSERT INTO corpscout.se_company_basic_info VALUES
    ('1',' Active AB ','active'),('2','Inactive','inactive'),('3',NULL,'active'),('4','   ','active');
"""
        + MIGRATION.read_text()
        + """
SELECT input_id,company_id,company_name,country_code FROM corpscout.se_company_brave_input FORMAT JSONCompactEachRow;
INSERT INTO corpscout.company_brave_info (result_id,task_id,answer_text) VALUES ('result-1','task-1','Full copied response');
INSERT INTO corpscout.company_brave_info (result_id,task_id,answer_text) VALUES ('result-1','task-1','Full copied response');
SELECT result_id,answer_text FROM corpscout.company_brave_info_deduplicated FORMAT JSONCompactEachRow;
SELECT name FROM system.columns WHERE database='corpscout' AND table='company_brave_info' ORDER BY position FORMAT JSONCompactEachRow;
"""
    )
    result = subprocess.run(
        clickhouse_local_command(),
        input=sql,
        capture_output=True,
        text=True,
        timeout=90,
    )
    assert result.returncode == 0, result.stderr
    assert [
        json.loads(line) for line in result.stdout.splitlines() if line.strip()
    ] == [
        ["1", "1", "Active AB", "SE"],
        ["result-1", "Full copied response"],
        *[[column] for column in EXPORT_COLUMNS],
    ]


COPY_PAGE = """<!doctype html><html><body>
<button aria-label="Copy" id="question-copy"></button>
<button id="copy" aria-label="Copy"> Copy</button>
<button id="retry" hidden>Try again</button>
<script>
const query = new URL(location.href).searchParams.get('q');
let answer = 'Incomplete streamed answer';
document.querySelector('#question-copy').onclick = () => navigator.clipboard.writeText(query);
document.querySelector('#copy').onclick = async () => {
    await navigator.clipboard.writeText(answer);
};
if (query !== 'never_finished') setTimeout(() => {
    answer = query === 'empty' ? '' : 'Answer: ' + query + '\\nhttps://example.se/';
    document.querySelector('#retry').hidden = false;
}, 50);
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
            copy_brave_answer(
                first, "+1 Kommunikationsbyrå AB & Co? #1", timeout_ms=5_000
            )
            == "Answer: +1 Kommunikationsbyrå AB & Co? #1\nhttps://example.se/"
        )
        assert first.url.startswith("https://search.brave.com/ask?")
        assert (
            copy_brave_answer(second, "Skanska AB", timeout_ms=5_000)
            == "Answer: Skanska AB\nhttps://example.se/"
        )
        assert (
            first.evaluate("() => window.__companyBraveCopiedText")
            == "Answer: +1 Kommunikationsbyrå AB & Co? #1\nhttps://example.se/"
        )
        with pytest.raises(PlaywrightTimeoutError):
            copy_brave_answer(first, "empty", timeout_ms=500)
        with pytest.raises(PlaywrightTimeoutError):
            copy_brave_answer(first, "never_finished", timeout_ms=500)
        context.close()
    finally:
        browser.close()
