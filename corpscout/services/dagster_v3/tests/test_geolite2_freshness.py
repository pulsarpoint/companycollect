"""GeoLite2 freshness with fake readers; no MaxMind traffic, no real .mmdb files."""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from dagster_v3.defs.commoncrawl_geoip import freshness
from dagster_v3.defs.commoncrawl_geoip.resources import MaxMindDatabaseResource


class Reader:
    def __init__(self, path, build_epoch):
        self.path, self.build_epoch = path, build_epoch

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def metadata(self):
        return SimpleNamespace(
            database_type="GeoLite2-City", build_epoch=self.build_epoch
        )


def test_build_times_are_read_per_edition_from_the_resource_paths(tmp_path):
    for edition in freshness.EDITIONS:
        (tmp_path / f"{edition}.mmdb").touch()
    resource = MaxMindDatabaseResource(database_directory=str(tmp_path))
    opened = []

    def opener(path):
        opened.append(path.name)
        return Reader(path, 1_790_000_000)

    times = freshness.database_build_times(resource, opener=opener)
    assert opened == ["GeoLite2-City.mmdb", "GeoLite2-ASN.mmdb"]
    assert times == dict.fromkeys(
        freshness.EDITIONS, datetime.fromtimestamp(1_790_000_000, UTC)
    )


def test_freshness_check_fails_after_fourteen_days():
    now = datetime(2026, 9, 25, tzinfo=UTC)
    fresh = now - timedelta(days=3)
    stale = now - timedelta(days=20)
    passed = freshness.freshness({"GeoLite2-City": fresh, "GeoLite2-ASN": fresh}, now)
    assert passed.passed and passed.metadata["max_age_days"].value == 14
    assert passed.metadata["GeoLite2-City_build"].value == fresh.isoformat()
    result = freshness.freshness({"GeoLite2-City": fresh, "GeoLite2-ASN": stale}, now)
    assert not result.passed and "GeoLite2-ASN built 2026-09-05" in result.description
    boundary = freshness.freshness(
        {"GeoLite2-City": now - freshness.MAX_AGE, "GeoLite2-ASN": fresh}, now
    )
    assert boundary.passed  # exactly 14 days old is still current


def test_check_targets_the_results_asset_and_the_job_selects_only_the_check():
    assert freshness.geolite2_databases_fresh.check_key == freshness.CHECK_KEY
    assert freshness.CHECK_KEY.asset_key == freshness.RESULTS_ASSET
    assert freshness.geolite2_freshness_job.name == "geolite2_freshness_job"
