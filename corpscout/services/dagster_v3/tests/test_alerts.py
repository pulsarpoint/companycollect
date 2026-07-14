import dagster as dg

from dagster_v3.defs.common import alerts


def test_run_failure_alert_sensor_is_registered_and_running_by_default() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    sensor = repo.get_sensor_def("run_failure_alert_sensor")
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
