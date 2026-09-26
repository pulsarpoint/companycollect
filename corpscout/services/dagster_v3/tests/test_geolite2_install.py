"""geolite2_databases against a disposable RustFS and MaxMind's official test databases.

No MaxMind traffic and no production store: the uploads live in a throwaway RustFS
container and the install directory is a pytest tmp_path. The builds are varied by
rewriting the 4-byte build_epoch in the fixture's metadata section, so every file is a
real, readable MMDB.
"""

import hashlib
import io
import os
import subprocess
import tarfile
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import dagster as dg
import maxminddb
import pytest

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.commoncrawl_geoip import install as install_module
from dagster_v3.defs.commoncrawl_geoip.install import (
    BUCKET,
    geolite2_databases,
    geolite2_install_job,
)
from dagster_v3.defs.commoncrawl_geoip.resources import MaxMindDatabaseResource

FIXTURES = Path(__file__).parent / "fixtures" / "geolite2"
NOW = int(datetime.now(UTC).timestamp())
OLD = NOW - int(timedelta(days=30).total_seconds())
NEW = NOW - int(timedelta(days=1).total_seconds())


@pytest.fixture(scope="module")
def s3():
    import boto3

    name = "geolite2-s3-" + uuid4().hex
    subprocess.run(
        [
            "docker",
            "run",
            "-d",
            "--rm",
            "--name",
            name,
            "-p",
            "127.0.0.1::9000",
            "-e",
            "RUSTFS_ACCESS_KEY=geolite2_test",
            "-e",
            "RUSTFS_SECRET_KEY=geolite2_test_secret",
            "rustfs/rustfs:latest",
        ],
        check=True,
        capture_output=True,
    )
    try:
        port = int(
            subprocess.check_output(["docker", "port", name, "9000"], text=True)
            .strip()
            .rsplit(":", 1)[1]
        )
        endpoint = f"http://127.0.0.1:{port}"
        client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id="geolite2_test",
            aws_secret_access_key="geolite2_test_secret",
            region_name="us-east-1",
        )
        deadline = time.monotonic() + 30
        while True:
            try:
                client.list_buckets()
                break
            except Exception:
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.2)
        client.create_bucket(Bucket=BUCKET)
        yield client, endpoint
    finally:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True, check=False)


def mmdb_bytes(edition: str, build_epoch: int) -> bytes:
    """The official test database of an edition with its build_epoch rewritten."""
    data = bytearray((FIXTURES / f"GeoLite2-{edition}-Test.mmdb").read_bytes())
    at = data.rfind(b"\x4bbuild_epoch") + 12
    assert data[at : at + 2] == b"\x04\x02"  # uint64 of 4 bytes
    data[at + 2 : at + 6] = build_epoch.to_bytes(4, "big")
    return bytes(data)


def tar_gz(members: dict[str, bytes], *, special: bytes | None = None) -> bytes:
    """An archive of regular members plus, optionally, one non-regular
    ``GeoLite2-City.mmdb`` member of tar type ``special`` (link, device, ...)."""
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
        for name, body in members.items():
            info = tarfile.TarInfo(name)
            info.size = len(body)
            tar.addfile(info, io.BytesIO(body))
        if special is not None:
            info = tarfile.TarInfo("GeoLite2-City_20260925/GeoLite2-City.mmdb")
            info.type = special
            if special in (tarfile.SYMTYPE, tarfile.LNKTYPE):
                info.linkname = (
                    "/etc/passwd"
                    if special == tarfile.SYMTYPE
                    else "GeoLite2-City_20260925/COPYRIGHT.txt"
                )
            else:
                info.devmajor, info.devminor = 1, 3
            tar.addfile(info)
    return buffer.getvalue()


def archive(edition: str, build_epoch: int, day: str = "20260925") -> bytes:
    folder = f"GeoLite2-{edition}_{day}"
    return tar_gz(
        {
            f"{folder}/COPYRIGHT.txt": b"test",
            f"{folder}/GeoLite2-{edition}.mmdb": mmdb_bytes(edition, build_epoch),
        }
    )


def upload(s3, name: str, body: bytes) -> str:
    client, _ = s3
    key = f"uploads/{uuid4()}/{name}"
    client.put_object(Bucket=BUCKET, Key=key, Body=body)
    return key


def install(s3, directory: Path, uploads: list[tuple[str, str]]):
    client, endpoint = s3
    return dg.materialize(
        [geolite2_databases],
        resources={
            "geolite2_object_store": ObjectStoreResource(
                bucket=BUCKET,
                endpoint_url=endpoint,
                access_key="geolite2_test",
                secret_key="geolite2_test_secret",
            ),
            "maxmind_geoip": MaxMindDatabaseResource(database_directory=str(directory)),
        },
        run_config={
            "ops": {
                "geolite2_databases": {
                    "config": {
                        "uploads": [
                            {"edition": edition, "key": key} for edition, key in uploads
                        ]
                    }
                }
            }
        },
        raise_on_error=False,
    )


def metadata(result) -> dict:
    (event,) = result.asset_materializations_for_node("geolite2_databases")
    return {key: value.value for key, value in event.metadata.items()}


def failure(result) -> str:
    assert not result.success
    (event,) = [e for e in result.all_events if e.is_step_failure]
    return event.step_failure_data.error.message


def build_of(path: Path) -> int:
    with maxminddb.open_database(path) as reader:
        return reader.metadata().build_epoch


def preinstall(directory: Path, city: int, asn: int) -> None:
    (directory / "GeoLite2-City.mmdb").write_bytes(mmdb_bytes("City", city))
    (directory / "GeoLite2-ASN.mmdb").write_bytes(mmdb_bytes("ASN", asn))


def snapshot(directory: Path) -> dict[str, bytes]:
    return {path.name: path.read_bytes() for path in sorted(directory.iterdir())}


def test_the_job_selects_the_asset_never_retries_and_runs_one_at_a_time():
    assert geolite2_install_job.name == "geolite2_install_job"
    assert geolite2_install_job.tags["dagster/max_retries"] == "0"
    # dagster.yaml defaults every pool to limit 1: installs are serialized.
    assert geolite2_databases.op.pool == "geolite2_install"


def test_installs_maxmind_archives_and_reports_both_editions(s3, tmp_path):
    city = upload(s3, "GeoLite2-City_20260925.tar.gz", archive("City", NEW))
    asn = upload(s3, "GeoLite2-ASN_20260925.tar.gz", archive("ASN", NEW))

    result = install(s3, tmp_path, [("City", city), ("ASN", asn)])

    assert result.success
    assert sorted(path.name for path in tmp_path.iterdir()) == [
        "GeoLite2-ASN.mmdb",
        "GeoLite2-City.mmdb",
    ]
    assert build_of(tmp_path / "GeoLite2-City.mmdb") == NEW
    assert oct((tmp_path / "GeoLite2-City.mmdb").stat().st_mode & 0o777) == "0o644"
    values = metadata(result)
    assert values["installed"] == ["City", "ASN"]
    assert values["source_keys"] == [city, asn]
    assert values["city_build"] == datetime.fromtimestamp(NEW, UTC).isoformat()
    assert values["asn_build"] == datetime.fromtimestamp(NEW, UTC).isoformat()
    assert values["city_sha256"] == hashlib.sha256(mmdb_bytes("City", NEW)).hexdigest()
    assert values["asn_sha256"] == hashlib.sha256(mmdb_bytes("ASN", NEW)).hexdigest()
    assert values["fresh"] is True
    assert values["lifecycle_applied"] is True
    assert values["city_age_days"] == 1 and values["asn_age_days"] == 1


def test_bare_mmdb_replaces_one_edition_atomically(s3, tmp_path):
    preinstall(tmp_path, OLD, OLD)
    installed = tmp_path / "GeoLite2-City.mmdb"
    old_inode = installed.stat().st_ino
    key = upload(s3, "GeoLite2-City.mmdb", mmdb_bytes("City", NEW))

    # A running enrichment has the old file mapped for its whole run.
    with maxminddb.open_database(installed) as running:
        result = install(s3, tmp_path, [("City", key)])
        assert result.success
        assert running.metadata().build_epoch == OLD
        assert running.get("81.2.69.160") is not None  # still reads the old inode

    assert installed.stat().st_ino != old_inode
    assert build_of(installed) == NEW
    assert build_of(tmp_path / "GeoLite2-ASN.mmdb") == OLD
    assert not [p for p in tmp_path.iterdir() if p.name.endswith(".tmp")]
    values = metadata(result)
    assert values["installed"] == ["City"]
    assert values["asn_build"] == datetime.fromtimestamp(OLD, UTC).isoformat()
    assert values["fresh"] is False and values["asn_age_days"] == 30


def test_older_build_is_refused_and_nothing_is_replaced(s3, tmp_path):
    preinstall(tmp_path, NEW, NEW)
    before = snapshot(tmp_path)
    key = upload(s3, "GeoLite2-City_20260801.tar.gz", archive("City", OLD, "20260801"))

    message = failure(install(s3, tmp_path, [("City", key)]))

    assert "older than the installed build" in message
    assert snapshot(tmp_path) == before


def test_same_build_is_an_idempotent_no_op(s3, tmp_path):
    preinstall(tmp_path, NEW, NEW)
    inode = (tmp_path / "GeoLite2-City.mmdb").stat().st_ino
    key = upload(s3, "GeoLite2-City.mmdb", mmdb_bytes("City", NEW))

    result = install(s3, tmp_path, [("City", key)])

    assert result.success and metadata(result)["installed"] == []
    assert (tmp_path / "GeoLite2-City.mmdb").stat().st_ino == inode


def test_edition_mismatch_is_refused(s3, tmp_path):
    preinstall(tmp_path, OLD, OLD)
    before = snapshot(tmp_path)
    # A City database uploaded as the ASN edition.
    key = upload(s3, "GeoLite2-ASN.mmdb", mmdb_bytes("City", NEW))

    message = failure(install(s3, tmp_path, [("ASN", key)]))

    assert "Expected a GeoLite2-ASN database" in message
    assert snapshot(tmp_path) == before


@pytest.mark.parametrize(
    ("body", "reason"),
    [
        (tar_gz({"../GeoLite2-City.mmdb": b"x"}), "absolute paths, '..' and links"),
        (tar_gz({"/tmp/GeoLite2-City.mmdb": b"x"}), "absolute paths, '..' and links"),
        (tar_gz({}, special=tarfile.SYMTYPE), "absolute paths, '..' and links"),
        (
            tar_gz(
                {"GeoLite2-City_20260925/COPYRIGHT.txt": b"x"},
                special=tarfile.LNKTYPE,
            ),
            "absolute paths, '..' and links",
        ),
        (tar_gz({}, special=tarfile.CHRTYPE), "absolute paths, '..' and links"),
        (tar_gz({}, special=tarfile.BLKTYPE), "absolute paths, '..' and links"),
        (tar_gz({}, special=tarfile.FIFOTYPE), "absolute paths, '..' and links"),
        (
            tar_gz({"a/GeoLite2-City.mmdb": b"x", "b/GeoLite2-City.mmdb": b"y"}),
            "exactly one GeoLite2-City.mmdb, found 2",
        ),
        (b"not a tarball", "not a readable .tar.gz archive"),
    ],
)
def test_unsafe_or_malformed_archives_are_refused(s3, tmp_path, body, reason):
    preinstall(tmp_path, OLD, OLD)
    before = snapshot(tmp_path)
    key = upload(s3, "GeoLite2-City_20260925.tar.gz", body)

    message = failure(install(s3, tmp_path, [("City", key)]))

    assert reason in message
    assert snapshot(tmp_path) == before
    assert not (tmp_path.parent / "GeoLite2-City.mmdb").exists()


def test_both_editions_are_validated_before_either_is_installed(s3, tmp_path):
    preinstall(tmp_path, OLD, NEW)
    before = snapshot(tmp_path)
    city = upload(s3, "GeoLite2-City_20260925.tar.gz", archive("City", NEW))
    stale_asn = upload(s3, "GeoLite2-ASN.mmdb", mmdb_bytes("ASN", OLD))

    message = failure(install(s3, tmp_path, [("City", city), ("ASN", stale_asn)]))

    assert "GeoLite2-ASN upload" in message
    assert snapshot(tmp_path) == before  # the valid, newer City was not installed


def test_config_is_checked_before_anything_is_downloaded(s3, tmp_path):
    key = upload(s3, "GeoLite2-City.mmdb", mmdb_bytes("City", NEW))
    assert "At most one upload per edition" in failure(
        install(s3, tmp_path, [("City", key), ("City", key)])
    )
    assert "Unknown GeoLite2 edition" in failure(
        install(s3, tmp_path, [("Country", key)])
    )
    assert "No uploads given" in failure(install(s3, tmp_path, []))
    assert list(tmp_path.iterdir()) == []


def test_upload_prefix_expires_after_ninety_days(s3, tmp_path):
    client, _ = s3
    key = upload(s3, "GeoLite2-City.mmdb", mmdb_bytes("City", NEW))
    assert install(s3, tmp_path, [("City", key)]).success
    rules = client.get_bucket_lifecycle_configuration(Bucket=BUCKET)["Rules"]
    (rule,) = [rule for rule in rules if rule["ID"] == "geolite2-uploads-expiry"]
    assert rule["Expiration"]["Days"] == 90
    assert rule["Filter"]["Prefix"] == "uploads/"
    # The upload object itself is kept.
    assert client.head_object(Bucket=BUCKET, Key=key)["ContentLength"] > 0


def temp_files(directory: Path) -> list[str]:
    return sorted(path.name for path in directory.glob(".GeoLite2-*.tmp"))


def test_missing_upload_object_is_a_clean_failure(s3, tmp_path):
    preinstall(tmp_path, OLD, OLD)
    before = snapshot(tmp_path)
    key = f"uploads/{uuid4()}/GeoLite2-City.mmdb"  # never uploaded

    message = failure(install(s3, tmp_path, [("City", key)]))

    assert f"Upload {key!r} is not in bucket geolite2" in message
    assert snapshot(tmp_path) == before


def test_failed_staged_verification_leaves_no_temp_file(s3, tmp_path, monkeypatch):
    preinstall(tmp_path, OLD, OLD)
    before = snapshot(tmp_path)
    key = upload(s3, "GeoLite2-City.mmdb", mmdb_bytes("City", NEW))
    real = install_module.sha256_of

    def corrupting(path: Path) -> str:
        return "0" * 64 if path.name.endswith(".tmp") else real(path)

    monkeypatch.setattr(install_module, "sha256_of", corrupting)

    message = failure(install(s3, tmp_path, [("City", key)]))

    assert "does not match the upload" in message
    assert temp_files(tmp_path) == []
    assert snapshot(tmp_path) == before


def test_stale_temp_files_are_swept_and_fresh_ones_kept(s3, tmp_path):
    preinstall(tmp_path, OLD, OLD)
    stale = tmp_path / ".GeoLite2-City.mmdb.dead-run.tmp"
    fresh = tmp_path / ".GeoLite2-ASN.mmdb.live-run.tmp"
    stale.write_bytes(b"x")
    fresh.write_bytes(b"y")
    two_hours_ago = time.time() - 7200
    os.utime(stale, (two_hours_ago, two_hours_ago))
    key = upload(s3, "GeoLite2-City.mmdb", mmdb_bytes("City", NEW))

    assert install(s3, tmp_path, [("City", key)]).success

    assert temp_files(tmp_path) == [fresh.name]


def test_a_newer_concurrent_install_is_not_overwritten(s3, tmp_path, monkeypatch):
    preinstall(tmp_path, OLD, OLD)
    key = upload(s3, "GeoLite2-City.mmdb", mmdb_bytes("City", NEW))
    newer = NOW - 3600
    real_stage = install_module.stage

    def stage_then_race(database, directory):
        staged = real_stage(database, directory)
        # Another install renames a newer build in while this one is staged.
        (directory / "GeoLite2-City.mmdb").write_bytes(mmdb_bytes("City", newer))
        return staged

    monkeypatch.setattr(install_module, "stage", stage_then_race)

    message = failure(install(s3, tmp_path, [("City", key)]))

    assert "newer than this upload" in message
    assert "Replaced in this run before stopping: none" in message
    assert build_of(tmp_path / "GeoLite2-City.mmdb") == newer
    assert temp_files(tmp_path) == []


def test_lifecycle_failure_is_only_a_warning(s3, tmp_path, monkeypatch):
    def refuse(self, rules, bucket=None):
        raise RuntimeError("lifecycle not supported")

    monkeypatch.setattr(ObjectStoreResource, "apply_lifecycle_rules", refuse)
    key = upload(s3, "GeoLite2-City.mmdb", mmdb_bytes("City", NEW))

    result = install(s3, tmp_path, [("City", key)])

    assert result.success
    assert metadata(result)["lifecycle_applied"] is False
    assert build_of(tmp_path / "GeoLite2-City.mmdb") == NEW


def test_an_unreadable_installed_edition_is_reported_not_raised(s3, tmp_path):
    (tmp_path / "GeoLite2-ASN.mmdb").write_bytes(b"not an mmdb")
    key = upload(s3, "GeoLite2-City.mmdb", mmdb_bytes("City", NEW))

    result = install(s3, tmp_path, [("City", key)])

    assert result.success
    values = metadata(result)
    assert values["installed"] == ["City"]
    assert values["asn_build"] == values["asn_sha256"] == "unreadable"
    assert values["fresh"] is False and "asn_age_days" not in values
