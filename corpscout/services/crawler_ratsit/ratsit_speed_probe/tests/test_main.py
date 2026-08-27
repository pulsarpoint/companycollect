import asyncio
import io
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path

import pytest
from ratsit_speed_probe.main import load_company_ids, probe_company_ids, ratsit_url


@dataclass
class FakeResponse:
    status: int
    body: bytes = b"<main class='main-inner'>company</main>"
    url: str = "https://www.ratsit.se/result"
    headers: dict[str, str] = field(default_factory=dict)

    def css(self, selector: str) -> list[object]:
        return [object()] if selector == ".main-inner" and self.body else []


def test_load_company_ids_accepts_ten_and_twelve_digit_ids(tmp_path: Path) -> None:
    input_path = tmp_path / "companies.txt"
    input_path.write_text("5562434182\n191234567890\n", encoding="utf-8")

    assert load_company_ids(input_path, limit=5_000) == [
        "5562434182",
        "191234567890",
    ]
    assert ratsit_url("195562434182") == "https://www.ratsit.se/5562434182"


def test_load_company_ids_rejects_duplicate_ratsit_paths(tmp_path: Path) -> None:
    input_path = tmp_path / "companies.json"
    input_path.write_text(json.dumps(["5562434182", "195562434182"]), encoding="utf-8")

    with pytest.raises(ValueError, match="resolve to Ratsit path"):
        load_company_ids(input_path, limit=5_000)


def test_load_company_ids_reads_newline_input_from_stdin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sys, "stdin", io.StringIO("5562434182\n5560000001\n"))

    assert load_company_ids(Path("-"), limit=5_000) == [
        "5562434182",
        "5560000001",
    ]


def test_probe_stops_on_first_429_and_writes_results(tmp_path: Path) -> None:
    responses = iter(
        [
            FakeResponse(status=200),
            FakeResponse(status=404, body=b"not found"),
            FakeResponse(status=429, headers={"Retry-After": "60"}),
            FakeResponse(status=200),
        ]
    )

    async def fetch(_url: str) -> FakeResponse:
        return next(responses)

    output_path = tmp_path / "probe.jsonl"
    summary = asyncio.run(
        probe_company_ids(
            ["5562434182", "5560000001", "5560000002", "5560000003"],
            fetch=fetch,
            output_path=output_path,
        )
    )

    rows = [json.loads(line) for line in output_path.read_text().splitlines()]
    assert [row["status"] for row in rows] == [200, 404, 429]
    assert rows[-1]["retry_after"] == "60"
    assert summary["attempted"] == 3
    assert summary["stop_reason"] == "rate_limited"
    assert summary["first_429_sequence"] == 3
    assert summary["status_counts"] == {"200": 1, "404": 1, "429": 1}
    assert output_path.with_suffix(".summary.json").exists()


def test_probe_records_fetch_error_and_stops(tmp_path: Path) -> None:
    async def fetch(_url: str) -> FakeResponse:
        raise TimeoutError("navigation timed out")

    output_path = tmp_path / "probe.jsonl"
    summary = asyncio.run(
        probe_company_ids(
            ["5562434182", "5560000001"],
            fetch=fetch,
            output_path=output_path,
        )
    )

    row = json.loads(output_path.read_text())
    assert row["status"] is None
    assert row["error_type"] == "TimeoutError"
    assert summary["attempted"] == 1
    assert summary["stop_reason"] == "fetch_error"
