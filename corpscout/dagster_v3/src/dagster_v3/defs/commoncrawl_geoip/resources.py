from pathlib import Path

import dagster as dg


class MaxMindDatabaseResource(dg.ConfigurableResource):
    """Find installed GeoLite2 City and ASN files at execution time."""

    database_directory: str = dg.EnvVar("MAXMIND_DATABASE_DIRECTORY")

    def database_paths(self) -> tuple[Path, Path]:
        directory = Path(self.database_directory).expanduser().resolve(strict=True)
        return (
            _latest_database(
                directory,
                directory_pattern="GeoLite2-City_*",
                filename="GeoLite2-City.mmdb",
            ),
            _latest_database(
                directory,
                directory_pattern="GeoLite2-ASN_*",
                filename="GeoLite2-ASN.mmdb",
            ),
        )


def _latest_database(directory: Path, *, directory_pattern: str, filename: str) -> Path:
    direct_database = directory / filename
    if direct_database.is_file():
        return direct_database.resolve(strict=True)

    candidates = sorted(directory.glob(f"{directory_pattern}/{filename}"))
    if not candidates:
        raise FileNotFoundError(
            "No MaxMind database matching "
            f"{filename} or {directory_pattern}/{filename} under {directory}"
        )
    return candidates[-1].resolve(strict=True)
