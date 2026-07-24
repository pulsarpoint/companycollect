#!/usr/bin/env python3
"""Validate live DataCVR company-detail JSON for a bounded company sample.

By default, the command deterministically selects 50 companies from the local
Denmark CVR DuckDB database. It downloads all details through the production
resource, which opens one browser session for the complete sample, and checks
that every payload can be converted to the English-key JSON representation.

No JSON is written to DuckDB or object storage.
"""

import json
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import click
import duckdb

from dagster_v3.defs.denmark_cvr.company_details import (
    DenmarkCvrCompanyDetailHttpFailure,
    DenmarkCvrCompanyDetailKeyError,
    DenmarkCvrCompanyDetailRequestError,
    DenmarkCvrCompanyDetailResource,
    company_detail_unmapped_key_paths,
    translate_company_detail_keys,
)
from dagster_v3.defs.denmark_cvr.duckdb_asset import (
    DENMARK_CVR_COMPANIES_TABLE,
    DENMARK_CVR_DUCKDB_PATH,
    DENMARK_CVR_DUCKDB_SCHEMA,
)

DEFAULT_SAMPLE_SIZE = 50
DEFAULT_SAMPLE_SEED = "company-detail-smoke-v1"


@dataclass(frozen=True)
class CompanyDetailSmokeTestSummary:
    selected_company_count: int
    valid_company_count: int
    failures: tuple[str, ...]


def sample_company_cvrs(
    database_path: Path,
    *,
    sample_size: int,
    seed: str,
) -> tuple[str, ...]:
    if sample_size <= 0:
        raise ValueError("sample size must be positive")
    if not seed.strip():
        raise ValueError("sample seed must not be empty")

    resolved_database_path = database_path.expanduser().resolve()
    if not resolved_database_path.is_file():
        raise FileNotFoundError(
            f"Denmark CVR DuckDB database not found: {resolved_database_path}"
        )

    with duckdb.connect(str(resolved_database_path), read_only=True) as connection:
        rows = connection.execute(
            f"""
            SELECT cvr
            FROM (
                SELECT DISTINCT cvr
                FROM {DENMARK_CVR_DUCKDB_SCHEMA}.{DENMARK_CVR_COMPANIES_TABLE}
                WHERE regexp_full_match(cvr, '[0-9]{{8}}')
            )
            ORDER BY md5(cvr || ':' || ?)
            LIMIT ?
            """,
            [seed, sample_size],
        ).fetchall()

    cvrs = tuple(str(row[0]) for row in rows)
    if len(cvrs) != sample_size:
        raise ValueError(
            f"requested {sample_size} companies but DuckDB contains only "
            f"{len(cvrs)} valid distinct CVR numbers"
        )
    return cvrs


def validate_company_detail_sample(
    details: DenmarkCvrCompanyDetailResource,
    cvrs: Sequence[str],
    *,
    report_progress: Callable[[str], object],
) -> CompanyDetailSmokeTestSummary:
    selected_cvrs = tuple(cvrs)
    failures: list[str] = []
    downloaded_count = 0
    valid_count = 0

    for downloaded_count, download in enumerate(
        details.iter_company_details(selected_cvrs),
        start=1,
    ):
        if isinstance(download, DenmarkCvrCompanyDetailHttpFailure):
            raise DenmarkCvrCompanyDetailRequestError(
                "DataCVR company-detail smoke test returned HTTP "
                f"{download.status} for CVR {download.cvr}"
            )
        unknown_paths = company_detail_unmapped_key_paths(download.payload)
        if unknown_paths:
            failures.append(
                f"CVR {download.cvr}: unmapped keys: {', '.join(unknown_paths)}"
            )
            report_progress(
                f"[{downloaded_count}/{len(selected_cvrs)}] CVR {download.cvr}: failed"
            )
            continue

        try:
            translated = translate_company_detail_keys(download.payload)
        except DenmarkCvrCompanyDetailKeyError as error:
            failures.append(f"CVR {download.cvr}: {error}")
            report_progress(
                f"[{downloaded_count}/{len(selected_cvrs)}] CVR {download.cvr}: failed"
            )
            continue

        if tuple(_leaf_values(download.payload)) != tuple(_leaf_values(translated)):
            failures.append(f"CVR {download.cvr}: translation changed JSON values")
            report_progress(
                f"[{downloaded_count}/{len(selected_cvrs)}] CVR {download.cvr}: failed"
            )
            continue

        json.dumps(translated, ensure_ascii=False, separators=(",", ":"))
        valid_count += 1
        report_progress(
            f"[{downloaded_count}/{len(selected_cvrs)}] CVR {download.cvr}: valid"
        )

    if downloaded_count != len(selected_cvrs):
        raise DenmarkCvrCompanyDetailRequestError(
            f"DataCVR returned {downloaded_count} of {len(selected_cvrs)} requested companies"
        )

    return CompanyDetailSmokeTestSummary(
        selected_company_count=len(selected_cvrs),
        valid_company_count=valid_count,
        failures=tuple(failures),
    )


def _leaf_values(value: Any) -> Iterator[Any]:
    if isinstance(value, Mapping):
        for child in value.values():
            yield from _leaf_values(child)
        return
    if isinstance(value, list):
        for child in value:
            yield from _leaf_values(child)
        return
    yield value


@click.command()
@click.option(
    "--database",
    "database_path",
    type=click.Path(path_type=Path, dir_okay=False),
    default=str(DENMARK_CVR_DUCKDB_PATH),
    show_default=True,
    help="DuckDB database used to select the company sample.",
)
@click.option(
    "--sample-size",
    type=click.IntRange(min=1),
    default=DEFAULT_SAMPLE_SIZE,
    show_default=True,
)
@click.option(
    "--seed",
    default=DEFAULT_SAMPLE_SEED,
    show_default=True,
    help="Deterministic sample seed.",
)
@click.option(
    "--cvr",
    "explicit_cvrs",
    multiple=True,
    help="Explicit CVR to test; repeat to bypass DuckDB sampling.",
)
def main(
    database_path: Path,
    sample_size: int,
    seed: str,
    explicit_cvrs: tuple[str, ...],
) -> None:
    """Download and validate a sample of live company-detail responses."""
    try:
        cvrs = explicit_cvrs or sample_company_cvrs(
            database_path,
            sample_size=sample_size,
            seed=seed,
        )
        click.echo(
            f"Validating {len(cvrs)} companies in one DataCVR browser session..."
        )
        summary = validate_company_detail_sample(
            DenmarkCvrCompanyDetailResource(),
            cvrs,
            report_progress=click.echo,
        )
    except (
        DenmarkCvrCompanyDetailRequestError,
        OSError,
        ValueError,
        duckdb.Error,
    ) as error:
        click.echo(f"Company-detail smoke test could not run: {error}", err=True)
        raise SystemExit(1) from None

    if summary.failures:
        click.echo(
            f"FAILED: {summary.valid_company_count}/"
            f"{summary.selected_company_count} company details were valid.",
            err=True,
        )
        for failure in summary.failures:
            click.echo(f"  - {failure}", err=True)
        raise SystemExit(1)

    click.echo(
        f"PASSED: all {summary.valid_company_count} company details parsed, "
        "translated, and serialized successfully."
    )


if __name__ == "__main__":
    main()
