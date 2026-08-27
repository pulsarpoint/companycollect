from pathlib import Path

import pytest

from crawler_ratsit.config import ProcessSettings, TemporalSettings, WorkerSettings


def test_temporal_starter_settings_do_not_require_storage_credentials() -> None:
    settings = TemporalSettings.from_environment({})

    assert settings.temporal_address == "127.0.0.1:7233"
    assert settings.temporal_namespace == "default"
    assert settings.temporal_task_queue == "ratsit-crawler"


def test_worker_requires_s3_credentials() -> None:
    with pytest.raises(ValueError, match="CORPSCOUT_S3_ENDPOINT is required"):
        WorkerSettings.from_environment({})


def test_worker_defaults_to_one_activity() -> None:
    settings = WorkerSettings.from_environment(
        {
            "CORPSCOUT_S3_ENDPOINT": "http://rustfs:9000",
            "CORPSCOUT_S3_ACCESS_KEY": "access",
            "CORPSCOUT_S3_SECRET_KEY": "secret",
        }
    )

    assert settings.max_concurrent_activities == 1
    assert settings.cloakbrowser_license_key is None
    assert settings.clickhouse_http_port == 8123


def test_process_config_loads_enabled_direct_and_proxy_browsers(
    tmp_path: Path,
) -> None:
    config_path = _private_config(
        tmp_path,
        """
[process]
state_directory = "/var/lib/ratsit-process"
headless = false

[limits]
per_browser_activities_per_second = 0.2
task_queue_activities_per_second = 0.4

[[browsers]]
id = "direct"
enabled = true

[[browsers]]
id = "proxy1"
proxy_url = "http://user:password@proxy1:8080"

[[browsers]]
id = "disabled"
enabled = false
""",
    )

    settings = ProcessSettings.from_file(config_path)

    assert settings.state_directory == Path("/var/lib/ratsit-process")
    assert settings.headless is False
    assert settings.per_browser_activities_per_second == 0.2
    assert settings.task_queue_activities_per_second == 0.4
    assert [browser.browser_id for browser in settings.browsers] == [
        "direct",
        "proxy1",
    ]
    assert settings.browsers[0].proxy_url is None
    assert settings.browsers[1].proxy_url == (
        "http://user:password@proxy1:8080"
    )


@pytest.mark.parametrize("value", ["0", "-1", '"not-a-number"'])
def test_process_config_rejects_invalid_activity_rate(
    tmp_path: Path,
    value: str,
) -> None:
    config_path = _private_config(
        tmp_path,
        f"""
[process]
state_directory = "/var/lib/ratsit-process"
headless = true

[limits]
per_browser_activities_per_second = {value}
task_queue_activities_per_second = 0.2

[[browsers]]
id = "direct"
""",
    )

    with pytest.raises(ValueError, match="per_browser_activities_per_second"):
        ProcessSettings.from_file(config_path)


def test_process_config_rejects_duplicate_browser_ids(tmp_path: Path) -> None:
    config_path = _private_config(
        tmp_path,
        """
[process]
state_directory = "/var/lib/ratsit-process"
headless = true

[limits]
per_browser_activities_per_second = 0.2
task_queue_activities_per_second = 0.2

[[browsers]]
id = "proxy1"

[[browsers]]
id = "proxy1"
""",
    )

    with pytest.raises(ValueError, match="duplicated"):
        ProcessSettings.from_file(config_path)


def test_process_config_must_not_be_readable_by_other_users(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "process.toml"
    config_path.write_text("", encoding="utf-8")
    config_path.chmod(0o644)

    with pytest.raises(ValueError, match="mode 0600 or stricter"):
        ProcessSettings.from_file(config_path)


def _private_config(tmp_path: Path, content: str) -> Path:
    config_path = tmp_path / "process.toml"
    config_path.write_text(content, encoding="utf-8")
    config_path.chmod(0o600)
    return config_path
