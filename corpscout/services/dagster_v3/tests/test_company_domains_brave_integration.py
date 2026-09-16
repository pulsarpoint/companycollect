"""Execute the selection SQL and Copy interception against their real runtimes."""

import json
import subprocess
from pathlib import Path

import pytest
from cloakbrowser import launch
from cloakbrowser.config import get_binary_path

from dagster_v3.defs.company_domains.publication import EXPORT_COLUMNS
from dagster_v3.defs.company_domains.browser import BraveStepError, copy_brave_answer
from tests.clickhouse_local import clickhouse_local_command

pytestmark = pytest.mark.integration
MIGRATION = (
    Path(__file__).parents[3]
    / "clickhouse/migrations/000414_corpscout_se_company_brave_domains.up.sql"
)


def test_current_answers_keep_latest_success_per_company_and_query_type():
    # The S3 engine is exercised separately against a real server and object store.
    sql = (
        MIGRATION.read_text().split("-- Full immutable responses", 1)[0]
        + """
INSERT INTO corpscout.se_company_brave_domains (result_id,company_id,country_code,status,query_type,answer_text,completed_at)
VALUES ('new','1','SE','success','website','Latest','2026-09-15 12:00:00'),
('old','1','SE','success','website','Previous','2026-09-14 12:00:00'),
('owner','1','SE','success','owner','Owner answer','2026-09-14 12:00:00');
SELECT result_id,answer_text FROM corpscout.se_company_brave_domains FINAL ORDER BY query_type FORMAT JSONCompactEachRow;
SELECT name FROM system.columns WHERE database='corpscout' AND table='se_company_brave_domains' ORDER BY position FORMAT JSONCompactEachRow;
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
        ["owner", "Owner answer"],
        ["new", "Latest"],
        *[[column] for column in EXPORT_COLUMNS],
        ["archive_path"],
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
}, query === 'slow' ? 900 : 50);
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
                first,
                "+1 Kommunikationsbyrå AB & Co? #1",
                timeout_ms=5_000,
                answer_timeout_ms=5_000,
            )
            == "Answer: +1 Kommunikationsbyrå AB & Co? #1\nhttps://example.se/"
        )
        assert first.url.startswith("https://search.brave.com/ask?")
        assert (
            copy_brave_answer(
                second, "Skanska AB", timeout_ms=5_000, answer_timeout_ms=5_000
            )
            == "Answer: Skanska AB\nhttps://example.se/"
        )
        assert (
            first.evaluate("() => window.__companyBraveCopiedText")
            == "Answer: +1 Kommunikationsbyrå AB & Co? #1\nhttps://example.se/"
        )
        with pytest.raises(BraveStepError) as caught:
            copy_brave_answer(first, "empty", timeout_ms=500, answer_timeout_ms=500)
        assert caught.value.stage == "copy"
        assert caught.value.error_type == "TimeoutError"
        with pytest.raises(BraveStepError) as caught:
            copy_brave_answer(
                first, "never_finished", timeout_ms=500, answer_timeout_ms=500
            )
        assert caught.value.stage == "answer_generation"
        assert caught.value.error_type == "TimeoutError"
        # A short normal action timeout must not truncate a longer answer wait.
        with pytest.raises(BraveStepError) as caught:
            copy_brave_answer(first, "slow", timeout_ms=500, answer_timeout_ms=200)
        assert caught.value.stage == "answer_generation"
        assert (
            copy_brave_answer(first, "slow", timeout_ms=500, answer_timeout_ms=2_000)
            == "Answer: slow\nhttps://example.se/"
        )
        context.close()
    finally:
        browser.close()
