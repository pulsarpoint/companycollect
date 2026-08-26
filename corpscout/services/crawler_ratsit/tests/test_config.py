import pytest

from crawler_ratsit.config import TemporalSettings, WorkerSettings


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
    assert settings.clickhouse_http_port == 8123
