"""Create the standard Dagster source package skeleton."""

from __future__ import annotations

import argparse
import re
from pathlib import Path

IDENTIFIER_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")


def scaffold_source(sources_root: Path, *, country: str, source: str) -> Path:
    """Create a source-owned Dagster package skeleton under sources_root."""
    _validate_identifier("country", country)
    _validate_identifier("source", source)

    package_dir = sources_root / country / source
    if package_dir.exists() and any(package_dir.iterdir()):
        raise FileExistsError(f"source package already exists and is not empty: {package_dir}")

    assets_dir = package_dir / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)

    files = {
        package_dir / "__init__.py": _init_template(country, source),
        package_dir / "spec.py": _spec_template(country, source),
        package_dir / "jobs.py": _jobs_template(country, source),
        package_dir / "schedules.py": _schedules_template(),
        assets_dir / "__init__.py": _assets_init_template(country, source),
        assets_dir / "external.py": _external_asset_template(country, source),
        assets_dir / "raw.py": _raw_asset_template(country, source),
    }
    for path, content in files.items():
        path.write_text(content)

    country_init = sources_root / country / "__init__.py"
    country_init.touch(exist_ok=True)
    return package_dir


def default_sources_root() -> Path:
    return Path(__file__).parent / "sources"


def main() -> None:
    parser = argparse.ArgumentParser(description="Create a Dagster source package skeleton.")
    parser.add_argument("country", help="Country package name, for example finland")
    parser.add_argument("source", help="Source package name, for example prh_ytj")
    parser.add_argument(
        "--sources-root",
        type=Path,
        default=default_sources_root(),
        help="Path to dagster_corpscout/sources",
    )
    args = parser.parse_args()

    package_dir = scaffold_source(args.sources_root, country=args.country, source=args.source)
    print(package_dir)


def _validate_identifier(label: str, value: str) -> None:
    if not IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(f"{label} must match {IDENTIFIER_PATTERN.pattern}: {value}")


def _init_template(country: str, source: str) -> str:
    module = f"dagster_corpscout.sources.{country}.{source}"
    return f'''from dagster_corpscout.source_bundle import SourceBundle
from {module} import spec
from {module}.assets import raw_snapshot, source_system
from {module}.jobs import pull_job

source_bundle = SourceBundle(
    source_name=spec.SOURCE_NAME,
    asset_key_prefix=tuple(spec.ASSET_KEY_PREFIX),
    assets=(source_system, raw_snapshot),
    jobs=(pull_job,),
)

__all__ = ["source_bundle"]
'''


def _spec_template(country: str, source: str) -> str:
    display_name = f"{country.replace('_', ' ').title()} {source.replace('_', ' ').upper()}"
    return f'''"""Declarative source config for {display_name}."""

SOURCE_NAME = "{country}_{source}"
COUNTRY = "{country}"
SOURCE_SLUG = "{source}"
DISPLAY_NAME = "{display_name}"
ASSET_KEY_PREFIX = ["sources", COUNTRY, SOURCE_SLUG]
GROUP_NAME = "sources_{country}_{source}"
'''


def _jobs_template(country: str, source: str) -> str:
    module = f"dagster_corpscout.sources.{country}.{source}"
    return f'''import dagster as dg

from {module}.assets import raw_snapshot
from {module} import spec

pull_job = dg.define_asset_job(
    name=f"{{spec.SOURCE_NAME}}_pull",
    selection=[raw_snapshot],
)
'''


def _schedules_template() -> str:
    return '''# Add source schedules here when the source is ready for automatic refresh.
'''


def _assets_init_template(country: str, source: str) -> str:
    module = f"dagster_corpscout.sources.{country}.{source}.assets"
    return f'''from {module}.external import source_system
from {module}.raw import raw_snapshot

__all__ = ["raw_snapshot", "source_system"]
'''


def _external_asset_template(country: str, source: str) -> str:
    module = f"dagster_corpscout.sources.{country}.{source}"
    return f'''import dagster as dg

from {module} import spec

source_system = dg.AssetSpec(
    key=dg.AssetKey([*spec.ASSET_KEY_PREFIX, "source_system"]),
    group_name=spec.GROUP_NAME,
    description=f"External source system for {{spec.DISPLAY_NAME}}.",
    metadata={{"country": spec.COUNTRY, "source": spec.DISPLAY_NAME}},
)
'''


def _raw_asset_template(country: str, source: str) -> str:
    module = f"dagster_corpscout.sources.{country}.{source}"
    return f'''import dagster as dg

from {module} import spec
from {module}.assets.external import source_system


@dg.asset(
    key_prefix=spec.ASSET_KEY_PREFIX,
    name="raw_snapshot",
    group_name=spec.GROUP_NAME,
    deps=[source_system],
    op_tags={{"dagster/concurrency_key": spec.SOURCE_NAME}},
)
def raw_snapshot() -> dg.MaterializeResult:
    raise NotImplementedError("Implement the source download before registering this package.")
'''


if __name__ == "__main__":
    main()
