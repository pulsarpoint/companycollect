from pathlib import Path

import dagster as dg


class MaxMindDatabaseResource(dg.ConfigurableResource):
    """Find installed GeoLite2 City and ASN files at execution time."""

    database_directory: str = dg.EnvVar("MAXMIND_DATABASE_DIRECTORY")

    def database_paths(self) -> tuple[Path, Path]:
        directory = Path(self.database_directory).expanduser()
        return (
            (directory / "GeoLite2-City.mmdb").resolve(strict=True),
            (directory / "GeoLite2-ASN.mmdb").resolve(strict=True),
        )
