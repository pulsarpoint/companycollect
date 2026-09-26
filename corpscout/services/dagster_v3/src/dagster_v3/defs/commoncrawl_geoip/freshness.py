"""GeoLite2 freshness: the files are uploaded by hand, this check says when it is due.

MaxMind publishes GeoLite2 twice a week; there is no MaxMind account here, so nothing
downloads. The check reads the build epoch from each installed file's metadata and
fails when either GeoLite2-City.mmdb or GeoLite2-ASN.mmdb is older than 14 days.
ip_enrichment_results opens the files per run through MaxMindDatabaseResource, so a
file installed by geolite2_install_job (install.py, an atomic rename) is used by the
next run without a restart; see docs/operations/ip-enrichment-draft-queue.md.
"""

from datetime import UTC, datetime, timedelta

import dagster as dg
import maxminddb

from dagster_v3.defs.commoncrawl_geoip.resources import MaxMindDatabaseResource

EDITIONS = ("GeoLite2-City", "GeoLite2-ASN")
MAX_AGE = timedelta(days=14)
RESULTS_ASSET = dg.AssetKey("ip_enrichment_results")
CHECK_KEY = dg.AssetCheckKey(RESULTS_ASSET, "geolite2_databases_fresh")


def database_build_times(
    resource: MaxMindDatabaseResource, *, opener=maxminddb.open_database
) -> dict[str, datetime]:
    """Build time per edition, read from the installed files' metadata."""
    times = {}
    for edition, path in zip(EDITIONS, resource.database_paths(), strict=True):
        with opener(path) as reader:
            times[edition] = datetime.fromtimestamp(reader.metadata().build_epoch, UTC)
    return times


def freshness(times: dict[str, datetime], now: datetime) -> dg.AssetCheckResult:
    stale = {
        edition: built for edition, built in times.items() if now - built > MAX_AGE
    }
    return dg.AssetCheckResult(
        passed=not stale,
        severity=dg.AssetCheckSeverity.ERROR,
        description=(
            "GeoLite2 databases are current."
            if not stale
            else "Stale GeoLite2 databases, upload new files on the backoffice GeoLite2 page: "
            + ", ".join(
                f"{edition} built {built.date().isoformat()}"
                for edition, built in stale.items()
            )
        ),
        metadata={
            **{
                f"{edition}_build": built.isoformat()
                for edition, built in times.items()
            },
            "max_age_days": MAX_AGE.days,
        },
    )


@dg.asset_check(
    asset=RESULTS_ASSET,
    name=CHECK_KEY.name,
    description="Fails when GeoLite2-City.mmdb or GeoLite2-ASN.mmdb in "
    "MAXMIND_DATABASE_DIRECTORY was built more than 14 days ago (MaxMind publishes "
    "twice a week; new files are uploaded on the backoffice GeoLite2 page).",
    blocking=False,
)
def geolite2_databases_fresh(
    maxmind_geoip: MaxMindDatabaseResource,
) -> dg.AssetCheckResult:
    return freshness(database_build_times(maxmind_geoip), datetime.now(UTC))


# Runs the check alone (Dagster UI or dg launch); no schedule, per the owner's decision.
geolite2_freshness_job = dg.define_asset_job(
    "geolite2_freshness_job", selection=dg.AssetSelection.checks(CHECK_KEY)
)
