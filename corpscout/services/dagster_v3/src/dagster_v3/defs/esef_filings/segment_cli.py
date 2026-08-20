"""Parse and verify one ESEF report package from the command line."""

import argparse
import json
import re
import sys
from collections.abc import Sequence
from pathlib import Path

from dagster_v3.defs.esef_filings.fact_parity import build_fact_parity_report
from dagster_v3.defs.esef_filings.segment_parser import (
    EsefArtifactSource,
    artifact_json_bytes,
    artifact_object_key,
    compare_artifact_to_oim,
    parse_esef_report_package,
)


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _argument_parser().parse_args(argv)
    try:
        artifact = parse_esef_report_package(
            arguments.package,
            source=EsefArtifactSource(
                fxo_id=arguments.fxo_id,
                country=arguments.country,
                source_url=arguments.source_url,
                object_key=arguments.object_key,
                company_id=arguments.company_id,
                source_run_id=arguments.source_run_id,
                expected_package_sha256=arguments.expected_package_sha256,
            ),
            validate_esef=not arguments.skip_esef_validation,
        )
        output_bytes = artifact_json_bytes(artifact)
        arguments.output.parent.mkdir(parents=True, exist_ok=True)
        arguments.output.write_bytes(output_bytes)

        summary: dict[str, object] = {
            "artifact_path": str(arguments.output.resolve()),
            "artifact_bytes": len(output_bytes),
            "object_key": artifact_object_key(artifact.package_sha256),
            "package_sha256": artifact.package_sha256,
            "fact_count": artifact.quality.fact_count,
            "text_fact_count": artifact.quality.text_fact_count,
            "numeric_fact_count": artifact.quality.numeric_fact_count,
            "contact_candidate_counts": {
                kind: sum(
                    candidate.kind == kind for candidate in artifact.contact_candidates
                )
                for kind in ("email", "phone")
            },
            "website_domain_candidate_count": len(artifact.website_candidates),
            "segment_fact_counts": {
                name: len(references) for name, references in artifact.segments.items()
            },
            "visible_section_counts": {
                section_type: sum(
                    section.section_type == section_type
                    for section in artifact.visible_sections
                )
                for section_type in sorted(
                    {section.section_type for section in artifact.visible_sections}
                )
            },
            "error_count": artifact.quality.error_count,
            "warning_count": artifact.quality.warning_count,
        }
        exit_code = 0
        if arguments.reference_oim is not None:
            reference = json.loads(arguments.reference_oim.read_text(encoding="utf-8"))
            comparison = compare_artifact_to_oim(artifact, reference)
            summary["reference_comparison"] = comparison
            parity_report = build_fact_parity_report(
                artifact,
                reference,
                lei=arguments.lei or arguments.fxo_id.partition("-")[0],
                fxo_id=arguments.fxo_id,
                period_end=arguments.period_end
                or _period_end_from_fxo_id(arguments.fxo_id),
                sample_limit=arguments.parity_sample_limit,
            )
            summary["fact_parity"] = parity_report
            if not parity_report["ready_for_fact_cutover"]:
                exit_code = 1
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"ESEF parse failed: {exc}", file=sys.stderr)
        return 2

    print(json.dumps(summary, indent=2, ensure_ascii=False, sort_keys=True))
    return exit_code


def _argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Parse an ESEF report package into a versioned JSON fact and segment artifact."
        )
    )
    parser.add_argument("package", type=Path, help="Local ESEF report-package ZIP")
    parser.add_argument("output", type=Path, help="Destination artifact JSON path")
    parser.add_argument("--fxo-id", required=True, help="filings.xbrl.org filing ID")
    parser.add_argument(
        "--country",
        default="",
        help="ISO alpha-2 filing country used to normalize local phone numbers",
    )
    parser.add_argument("--source-url", default="")
    parser.add_argument("--object-key", default="")
    parser.add_argument("--company-id", default="")
    parser.add_argument("--source-run-id", default="")
    parser.add_argument("--expected-package-sha256", default="")
    parser.add_argument(
        "--lei",
        default="",
        help="Reporting entity LEI; defaults to the first component of --fxo-id",
    )
    parser.add_argument(
        "--period-end",
        default="",
        help="Filing period end (YYYY-MM-DD); defaults to the date in --fxo-id",
    )
    parser.add_argument(
        "--reference-oim",
        type=Path,
        help="Optional xBRL-JSON reference used for exact fact-set verification",
    )
    parser.add_argument(
        "--skip-esef-validation",
        action="store_true",
        help="Load XBRL without running the ESEF validation plugin",
    )
    parser.add_argument(
        "--parity-sample-limit",
        type=int,
        default=5,
        help="Maximum artifact-only and OIM-only fact samples in the parity report",
    )
    return parser


def _period_end_from_fxo_id(fxo_id: str) -> str:
    match = re.search(r"-(\d{4}-\d{2}-\d{2})-", fxo_id)
    if match is None:
        raise ValueError(
            "Cannot infer period_end from fxo_id; pass --period-end explicitly"
        )
    return match.group(1)


if __name__ == "__main__":
    raise SystemExit(main())
