from types import SimpleNamespace

import dagster as dg

from dagster_v3.defs.common import alerts


def test_alert_sensors_are_registered_and_running_by_default() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    for sensor_name in ("run_failure_alert_sensor", "stale_run_alert_sensor"):
        sensor = repo.get_sensor_def(sensor_name)
        assert sensor.default_status == dg.DefaultSensorStatus.RUNNING


def test_format_run_failure_message_names_job_run_and_error() -> None:
    message = alerts.format_run_failure_message(
        job_name="latvia_ur_full_refresh_job",
        run_id="abcd1234-0000-0000-0000-000000000000",
        error="ValueError: DuckDB table latvia_ur.companies has 0 rows",
    )
    assert "latvia_ur_full_refresh_job" in message
    assert "abcd1234" in message
    assert "0 rows" in message


def test_format_stale_run_message_reports_event_inactivity() -> None:
    message = alerts.format_stale_run_message(
        job_name="wikidata_company_seed_weekly_job",
        run_id="abcd1234-0000-0000-0000-000000000000",
        idle_seconds=3700,
    )
    assert "wikidata_company_seed_weekly_job" in message
    assert "abcd1234" in message
    assert "idle=61m" in message


def test_latest_run_activity_uses_latest_event_timestamp() -> None:
    event = SimpleNamespace(timestamp=1234.5)
    instance = SimpleNamespace(
        get_records_for_run=lambda *args, **kwargs: SimpleNamespace(records=[event])
    )

    assert alerts.latest_run_activity_timestamp(instance, "run-id", 1000.0) == 1234.5


def test_latest_run_activity_falls_back_when_run_has_no_events() -> None:
    instance = SimpleNamespace(
        get_records_for_run=lambda *args, **kwargs: SimpleNamespace(records=[])
    )

    assert alerts.latest_run_activity_timestamp(instance, "run-id", 1000.0) == 1000.0


def test_post_alert_sends_slack_compatible_payload(monkeypatch) -> None:
    calls: list[tuple[str, dict, int]] = []

    class FakeResponse:
        def raise_for_status(self) -> None:
            return None

    def fake_post(url, *, json, timeout):
        calls.append((url, json, timeout))
        return FakeResponse()

    monkeypatch.setattr(alerts.requests, "post", fake_post)

    alerts.post_alert("https://hooks.example.com/T000/B000", "run failed")

    assert calls == [
        ("https://hooks.example.com/T000/B000", {"text": "run failed"}, 10)
    ]
