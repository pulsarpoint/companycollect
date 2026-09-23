"""Execute the import, deletion guard, and recovery against real ClickHouse."""

import json
import subprocess
from pathlib import Path

from tests.clickhouse_local import clickhouse_local_command

MIGRATIONS = Path(__file__).resolve().parents[3] / "clickhouse/migrations"


def migration(name: str, direction: str = "up") -> str:
    return (MIGRATIONS / f"{name}.{direction}.sql").read_text()


def setup() -> str:
    legacy = migration("000115_corpscout_commoncrawl_ip_geoip")
    return (
        migration("000433_corpscout_ip_enrichment")
        + legacy[
            legacy.index("CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_ip_geoip") :
        ]
        + """
    INSERT INTO corpscout.commoncrawl_ip_geoip
      (bucket,ip,ip_version,ip_scope,city_lookup_status,asn_lookup_status,country_iso_code,
       city_name,asn,subdivision_names,latitude,city_network,enriched_at)
    VALUES
      (toUInt16(cityHash64('8.8.8.8')%256),'8.8.8.8',4,'global','found','found','US',NULL,15169,['California'],37.4,'8.8.8.0/24','2026-07-12'),
      (toUInt16(cityHash64('::ffff:808:808')%256),'::ffff:808:808',6,'global','found','not_found','US','Old mapped city',NULL,[],NULL,NULL,'2026-07-12'),
      (toUInt16(cityHash64('127.0.0.1')%256),'127.0.0.1',4,'loopback','not_global','not_global',NULL,NULL,NULL,[],NULL,NULL,'2026-07-12');
    INSERT INTO corpscout.ip_enrichment_results
      (ip,result_id,completed_at,city_checked_at,city_lookup_status,city_name,rdap_lookup_status,rdap_checked_at,rdap_name)
    VALUES ('8.8.8.8','00000000-0000-0000-0000-000000000009','2026-09-01','2026-09-01','found','New city','found','2026-09-01','Existing RDAP');
    """
    )


def test_import_preserves_payload_is_repeatable_and_does_not_replace_newer_results() -> (
    None
):
    copy = migration("000434_corpscout_import_legacy_geoip")
    result = subprocess.run(
        clickhouse_local_command(),
        input=setup()
        + copy
        + copy
        + """
    SELECT count() FROM corpscout.ip_enrichment_results FINAL FORMAT JSONCompactEachRow;
    SELECT city_name,asn,rdap_name FROM corpscout.ip_enrichment_current WHERE ip='8.8.8.8' FORMAT JSONCompactEachRow;
    SELECT ip,city_name,rdap_lookup_status,toString(city_checked_at) FROM corpscout.ip_enrichment_current WHERE ip LIKE '::ffff:%' FORMAT JSONCompactEachRow;
    """
        + migration("000435_corpscout_retire_legacy_geoip")
        + """
    SELECT count() FROM system.tables WHERE database='corpscout' AND name IN ('commoncrawl_ip_geoip','commoncrawl_ip_geoip_current','ip_enrichment_legacy_geoip_import') FORMAT JSONCompactEachRow;
    """
        + migration("000435_corpscout_retire_legacy_geoip", "down")
        + """
    SELECT count() FROM corpscout.commoncrawl_ip_geoip FINAL FORMAT JSONCompactEachRow;
    SELECT ip,city_name,subdivision_names,latitude FROM corpscout.commoncrawl_ip_geoip FINAL WHERE ip='8.8.8.8' FORMAT JSONCompactEachRow;
    """,
        text=True,
        capture_output=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    # The deletion guard itself emits 0 on success.
    assert [json.loads(line) for line in result.stdout.splitlines()] == [
        [4],
        ["New city", 15169, "Existing RDAP"],
        [
            "::ffff:8.8.8.8",
            "Old mapped city",
            "not_attempted",
            "2026-07-12 00:00:00.000000",
        ],
        0,
        [0],
        [3],
        ["8.8.8.8", None, ["California"], 37.4],
    ]


def test_retirement_refuses_to_drop_uncopied_data() -> None:
    copy = migration("000434_corpscout_import_legacy_geoip")
    view_only = copy[: copy.index("INSERT INTO corpscout.ip_enrichment_results")]
    result = subprocess.run(
        clickhouse_local_command(),
        input=setup() + view_only + migration("000435_corpscout_retire_legacy_geoip"),
        text=True,
        capture_output=True,
        timeout=120,
        check=False,
    )
    assert result.returncode != 0
    assert "refusing to drop the source" in result.stderr
