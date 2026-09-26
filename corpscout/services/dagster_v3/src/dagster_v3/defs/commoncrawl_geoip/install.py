"""Install uploaded GeoLite2 databases on the Dagster host.

The owner downloads GeoLite2 by hand and uploads the archives on the backoffice page
Admin -> Settings -> GeoLite2, which stores them unchanged in bucket ``geolite2`` under
``uploads/<uuid>/<file name>`` and launches ``geolite2_install_job`` with their keys.
This asset is the authority: it extracts the one ``GeoLite2-<edition>.mmdb`` member,
checks the database type and build, refuses an older build, validates every upload
before replacing anything, and swaps each file in with an atomic rename.

Files are never overwritten in place: ``ip_enrichment_results`` maps the installed
files for the whole run, so a running run keeps reading the old inode and the next run
opens the new file.
"""

import hashlib
import os
import shutil
import tarfile
import tempfile
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath

import dagster as dg
import maxminddb
from pydantic import Field

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.commoncrawl_geoip.freshness import freshness
from dagster_v3.defs.commoncrawl_geoip.resources import MaxMindDatabaseResource

BUCKET = "geolite2"
UPLOAD_PREFIX = "uploads/"
UPLOAD_RETENTION_DAYS = 90
EDITIONS = ("City", "ASN")
# A current GeoLite2-City.mmdb is ~60 MB; anything far larger is not a GeoLite2 file.
MAX_MMDB_BYTES = 1024**3


def database_type(edition: str) -> str:
    """The MMDB ``database_type`` of an edition (MaxMind's test databases match too)."""
    return f"GeoLite2-{edition}"


def installed_name(edition: str) -> str:
    return f"{database_type(edition)}.mmdb"


class GeoLite2Upload(dg.Config):
    edition: str = Field(description='"City" or "ASN".')
    key: str = Field(
        description="Object key in bucket geolite2 (uploads/<uuid>/<name>)."
    )


class GeoLite2InstallConfig(dg.Config):
    uploads: list[GeoLite2Upload] = Field(
        description="The uploaded GeoLite2 files, at most one per edition."
    )


@dataclass(frozen=True)
class ValidatedDatabase:
    edition: str
    key: str
    path: Path
    build_epoch: int
    sha256: str


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_build(path: Path, edition: str) -> int:
    """Open the file as an MMDB, require the edition's database type, return its build."""
    try:
        with maxminddb.open_database(path) as reader:
            metadata = reader.metadata()
    except (OSError, ValueError, maxminddb.InvalidDatabaseError) as error:
        raise dg.Failure(f"{path.name} is not a readable MaxMind database: {error}")
    if metadata.database_type != database_type(edition):
        raise dg.Failure(
            f"Expected a {database_type(edition)} database, the upload is "
            f"{metadata.database_type!r}; nothing was installed."
        )
    return int(metadata.build_epoch)


def extract_mmdb(archive: Path, edition: str, target: Path) -> Path:
    """Extract exactly the one ``GeoLite2-<edition>.mmdb`` member of a MaxMind archive."""
    wanted = installed_name(edition)
    try:
        with tarfile.open(archive, "r:gz") as tar:
            members = [
                member
                for member in tar.getmembers()
                if PurePosixPath(member.name).name == wanted
            ]
            if len(members) != 1:
                raise dg.Failure(
                    f"{archive.name} must contain exactly one {wanted}, found {len(members)}."
                )
            member = members[0]
            parts = PurePosixPath(member.name).parts
            if member.name.startswith("/") or ".." in parts or not member.isfile():
                raise dg.Failure(
                    f"Refusing archive member {member.name!r} of {archive.name}: "
                    "absolute paths, '..' and links are not accepted."
                )
            if member.size > MAX_MMDB_BYTES:
                raise dg.Failure(f"{member.name} is {member.size} bytes, too large.")
            try:
                tar.extract(member, target, filter="data")
            except tarfile.FilterError as error:
                raise dg.Failure(f"Refusing archive member {member.name!r}: {error}")
    except (tarfile.TarError, OSError, EOFError) as error:
        raise dg.Failure(f"{archive.name} is not a readable .tar.gz archive: {error}")
    return target / member.name


def fetch_and_validate(
    upload: GeoLite2Upload, store: ObjectStoreResource, workdir: Path
) -> ValidatedDatabase:
    name = PurePosixPath(upload.key).name
    if not upload.key.startswith(UPLOAD_PREFIX) or not name:
        raise dg.Failure(f"Upload key {upload.key!r} is not under {UPLOAD_PREFIX}.")
    local = workdir / upload.edition / name
    local.parent.mkdir(parents=True)
    store.download_file(upload.key, local, bucket=BUCKET)
    if name.endswith(".tar.gz"):
        mmdb = extract_mmdb(local, upload.edition, workdir / upload.edition / "x")
    elif name.endswith(".mmdb"):
        mmdb = local
    else:
        raise dg.Failure(f"{name} is neither a .tar.gz archive nor an .mmdb file.")
    return ValidatedDatabase(
        edition=upload.edition,
        key=upload.key,
        path=mmdb,
        build_epoch=read_build(mmdb, upload.edition),
        sha256=sha256_of(mmdb),
    )


def installed_state(directory: Path, edition: str) -> tuple[int, str] | None:
    """(build epoch, sha256) of the installed file, or None when there is none."""
    path = directory / installed_name(edition)
    if not path.exists():
        return None
    return read_build(path, edition), sha256_of(path)


def iso(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, UTC).isoformat()


def _fsync_directory(directory: Path) -> None:
    fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def stage(database: ValidatedDatabase, directory: Path) -> Path:
    """Copy into the install directory under a hidden temporary name, fsync, re-verify."""
    staged = directory / f".{installed_name(database.edition)}.{uuid.uuid4()}.tmp"
    try:
        with database.path.open("rb") as source, staged.open("xb") as target:
            shutil.copyfileobj(source, target, 1 << 20)
            target.flush()
            os.fsync(target.fileno())
        staged.chmod(0o644)
        if (
            read_build(staged, database.edition) != database.build_epoch
            or sha256_of(staged) != database.sha256
        ):
            raise dg.Failure(
                f"The staged copy {staged.name} does not match the upload."
            )
    except BaseException:
        staged.unlink(missing_ok=True)
        raise
    return staged


def install(databases: list[ValidatedDatabase], directory: Path, log) -> list[str]:
    """Refuse older builds, then stage every file before renaming any of them."""
    to_install = []
    for database in databases:
        try:
            current = installed_state(directory, database.edition)
        except dg.Failure as error:
            # A damaged installed file must not block its own replacement.
            log.warning(f"Replacing an unreadable installed file: {error.description}")
            current = None
        if current is not None:
            build, sha = current
            if database.build_epoch < build:
                raise dg.Failure(
                    f"{database_type(database.edition)} upload built "
                    f"{iso(database.build_epoch)} is older than the installed build "
                    f"{iso(build)}; nothing was installed."
                )
            if database.build_epoch == build and database.sha256 == sha:
                log.info(f"{installed_name(database.edition)} is already installed.")
                continue
        to_install.append(database)
    staged: list[tuple[ValidatedDatabase, Path]] = []
    try:
        for database in to_install:
            staged.append((database, stage(database, directory)))
    except BaseException:
        for _, path in staged:
            path.unlink(missing_ok=True)
        raise
    for database, path in staged:
        os.replace(path, directory / installed_name(database.edition))
        log.info(
            f"Installed {installed_name(database.edition)} built {iso(database.build_epoch)}."
        )
    if staged:
        _fsync_directory(directory)
    return [database.edition for database, _ in staged]


def upload_retention_rule() -> dict[str, object]:
    return {
        "ID": "geolite2-uploads-expiry",
        "Status": "Enabled",
        "Filter": {"Prefix": UPLOAD_PREFIX},
        "Expiration": {"Days": UPLOAD_RETENTION_DAYS},
    }


@dg.asset(
    group_name="commoncrawl_geoip",
    kinds={"python", "s3", "maxmind"},
    description="GeoLite2-City.mmdb and GeoLite2-ASN.mmdb in MAXMIND_DATABASE_DIRECTORY, "
    "installed from files uploaded on the backoffice GeoLite2 settings page (bucket "
    "geolite2). Validates the database type and build, refuses older builds, validates "
    "every upload before replacing anything and replaces each file by atomic rename.",
)
def geolite2_databases(
    context: dg.AssetExecutionContext,
    config: GeoLite2InstallConfig,
    geolite2_object_store: ObjectStoreResource,
    maxmind_geoip: MaxMindDatabaseResource,
) -> dg.MaterializeResult:
    editions = [upload.edition for upload in config.uploads]
    if not editions:
        raise dg.Failure("No uploads given; nothing to install.")
    unknown = sorted(set(editions) - set(EDITIONS))
    if unknown:
        raise dg.Failure(
            f"Unknown GeoLite2 edition(s) {unknown}; expected City or ASN."
        )
    if len(set(editions)) != len(editions):
        raise dg.Failure("At most one upload per edition.")
    directory = Path(maxmind_geoip.database_directory).expanduser()
    if not directory.is_dir():
        raise dg.Failure(f"MAXMIND_DATABASE_DIRECTORY {directory} does not exist.")

    geolite2_object_store.apply_lifecycle_rules(
        [upload_retention_rule()], bucket=BUCKET
    )
    with tempfile.TemporaryDirectory(prefix="geolite2-") as workdir:
        databases = [
            fetch_and_validate(upload, geolite2_object_store, Path(workdir))
            for upload in config.uploads
        ]
        installed = install(databases, directory, context.log)

    metadata: dict[str, object] = {
        "installed": installed,
        "source_keys": [upload.key for upload in config.uploads],
    }
    builds = {}
    for edition in EDITIONS:
        state = installed_state(directory, edition)
        prefix = edition.lower()
        metadata[f"{prefix}_build"] = iso(state[0]) if state else "missing"
        metadata[f"{prefix}_sha256"] = state[1] if state else "missing"
        if state:
            builds[database_type(edition)] = datetime.fromtimestamp(state[0], UTC)
    now = datetime.now(UTC)
    if len(builds) == len(EDITIONS):
        check = freshness(builds, now)
        metadata["fresh"] = check.passed
        if not check.passed:
            context.log.warning(check.description)
    else:
        metadata["fresh"] = False
    for edition, built in builds.items():
        metadata[f"{edition.removeprefix('GeoLite2-').lower()}_age_days"] = (
            now - built
        ).days
    return dg.MaterializeResult(metadata=metadata)


geolite2_install_job = dg.define_asset_job(
    "geolite2_install_job",
    selection=[geolite2_databases],
    tags={"dagster/max_retries": "0"},
    description="Install GeoLite2 files uploaded on the backoffice GeoLite2 page.",
)
