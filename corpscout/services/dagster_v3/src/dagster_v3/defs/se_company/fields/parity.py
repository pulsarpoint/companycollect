"""Cutover parity (spec 12 step 4): the rebuilt se_company_info against a snapshot of the
old one.

The cutover plan creates corpscout.se_company_info_parity_snapshot with
``build_parity_snapshot_sql()`` from the OLD table before the rebuild (a scratch table,
direct SQL, not a ledger migration), runs the resolve with ``resolve_all``, then executes
this check on its own -- it is subtracted from both jobs (fields/jobs.py) because it is
meaningless on an ordinary sensor run and would show red forever.

Rules, per old row: legal facts and codes must be equal; a description copied from a
single source with no decision must be equal; a company with an applied decision
(``correction_ids`` non-empty) must equal its old text whatever wrote it; a modelled
description with no decision must equal the STORED OBSERVATION's text (the LLM
candidate reuses it by input_hash). A company published with several sources but no
suggestion is expected to change once the LLM extractor supplies a candidate -- reported
as description_model_pending_changed, never a failure. Under join_use_nulls = 1 every
rebuilt/observation column of a missing join is NULL, hence the ifNull everywhere.
"""

from collections.abc import Mapping, Sequence
from typing import Any

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.se_company.fields.resolve import DATABASE, PARITY_CHECK_NAME, RESOLVE_ASSET
from dagster_v3.defs.se_company.fields.tables import SE_COMPANY_FIELD, SE_COMPANY_INFO

PARITY_SNAPSHOT = "se_company_info_parity_snapshot"
OBSERVATION_TABLE = "se_company_info_enrichment_observation"
SAMPLE_SIZE = 20
ZERO_UUID = "00000000-0000-0000-0000-000000000000"
PRESENT = "ifNull(rebuilt.company_id, '') != ''"
NO_DECISION = "length(old.correction_ids) = 0"
OBSERVATION_PRESENT = f"ifNull(toString(observation.suggestion_id), '{ZERO_UUID}') != '{ZERO_UUID}'"


def _differs(column: str) -> str:
    return f"ifNull(toString(rebuilt.{column}), '') != ifNull(toString(old.{column}), '')"


# (name, condition over old/rebuilt/observation). A non-zero count fails the check.
MISMATCH_CONDITIONS: tuple[tuple[str, str], ...] = (
    ("legal_name", _differs("legal_name")),
    ("legal_form_code", _differs("legal_form_code")),
    ("status", _differs("status")),
    ("incorporation_date", _differs("incorporation_date")),
    ("primary_sni_code", _differs("primary_sni_code")),
    ("primary_nace_code", _differs("primary_nace_code")),
    ("description_copied",
     f"NOT old.llm_enhanced AND old.description_source_count <= 1 AND {NO_DECISION} AND {_differs('description')}"),
    ("description_sv_copied",
     f"NOT old.llm_enhanced AND old.description_source_count <= 1 AND {NO_DECISION} AND {_differs('description_sv')}"),
    ("description_decided",
     f"NOT ({NO_DECISION}) AND ({_differs('description')} OR {_differs('description_sv')})"),
    ("description_llm",
     f"old.llm_enhanced AND {NO_DECISION} AND {OBSERVATION_PRESENT} AND "
     "ifNull(rebuilt.description, '') != ifNull(observation.description, '')"),
    ("description_sv_llm",
     f"old.llm_enhanced AND {NO_DECISION} AND {OBSERVATION_PRESENT} AND "
     "ifNull(rebuilt.description_sv, '') != ifNull(observation.description_sv, '')"),
)
# Reported, never failing.
INFORMATIONAL_CONDITIONS: tuple[tuple[str, str], ...] = (
    ("description_model_pending_changed",
     f"NOT old.llm_enhanced AND old.description_source_count > 1 AND {NO_DECISION} AND {_differs('description')}"),
    ("llm_observation_missing", f"old.llm_enhanced AND {NO_DECISION} AND NOT ({OBSERVATION_PRESENT})"),
)
CONDITION_NAMES = tuple(name for name, _ in (*MISMATCH_CONDITIONS, *INFORMATIONAL_CONDITIONS))
# The parity SELECT's output order -- run_parity_check reads the one row by position.
PARITY_COLUMNS = ("companies_compared", "missing_after_rebuild", *CONDITION_NAMES,
                  *(f"{name}_samples" for name in CONDITION_NAMES))


def build_parity_snapshot_sql() -> str:
    """The cutover plan runs this against the OLD table before the rebuild."""
    return f"""CREATE TABLE IF NOT EXISTS {DATABASE}.{PARITY_SNAPSHOT}
ENGINE = MergeTree ORDER BY company_id AS
SELECT company_id, legal_name, legal_form_code, status, incorporation_date,
    description, description_sv, llm_enhanced, description_source_count, suggestion_id, correction_ids,
    primary_sni_code, primary_nace_code, resolved_at AS snapshot_resolved_at
FROM {SE_COMPANY_INFO} FINAL"""


def build_parity_sql() -> str:
    conditions = (*MISMATCH_CONDITIONS, *INFORMATIONAL_CONDITIONS)
    counts = ",\n    ".join(f"countIf({PRESENT} AND ({condition})) AS {name}" for name, condition in conditions)
    samples = ",\n    ".join(
        f"groupArrayIf({SAMPLE_SIZE})(old.company_id, {PRESENT} AND ({condition})) AS {name}_samples"
        for name, condition in conditions)
    return f"""WITH observation AS (
    SELECT suggestion_id,
        JSONExtractString(suggestion, 'description') AS description,
        JSONExtractString(suggestion, 'description_sv') AS description_sv
    FROM {DATABASE}.{OBSERVATION_TABLE}
),
rebuilt AS (
    SELECT company_id, legal_name, legal_form_code, status, incorporation_date, description, description_sv,
        primary_sni_code, primary_nace_code
    FROM {SE_COMPANY_INFO} FINAL
)
SELECT count() AS companies_compared,
    countIf(NOT ({PRESENT})) AS missing_after_rebuild,
    {counts},
    {samples}
FROM {DATABASE}.{PARITY_SNAPSHOT} AS old
LEFT JOIN rebuilt ON rebuilt.company_id = old.company_id
LEFT JOIN observation ON observation.suggestion_id = old.suggestion_id"""


def build_rows_per_field_source_sql() -> str:
    return f"""SELECT field, source, count() AS rows
FROM {SE_COMPANY_FIELD} FINAL
GROUP BY field, source
ORDER BY field, source"""


def parity_result(counts: Mapping[str, int], samples: Mapping[str, Sequence[str]],
                  rows_per_field_source: Sequence[tuple[str, str, int]]) -> dg.AssetCheckResult:
    """Pass iff no company is missing and every MISMATCH_CONDITIONS count is zero."""
    failing = {name: int(counts[name]) for name, _ in MISMATCH_CONDITIONS if counts[name]}
    if counts["missing_after_rebuild"]:
        failing = {"missing_after_rebuild": int(counts["missing_after_rebuild"]), **failing}
    metadata: dict[str, Any] = {
        "companies_compared": dg.MetadataValue.int(int(counts["companies_compared"])),
        "missing_after_rebuild": dg.MetadataValue.int(int(counts["missing_after_rebuild"])),
        **{name: dg.MetadataValue.int(int(counts[name])) for name in CONDITION_NAMES},
        "failing": dg.MetadataValue.json(failing),
        "samples": dg.MetadataValue.json({name: [str(c) for c in ids] for name, ids in samples.items() if ids}),
        "rows_per_field_per_source": dg.MetadataValue.json(
            [{"field": field, "source": source, "rows": int(rows)} for field, source, rows in rows_per_field_source]),
    }
    return dg.AssetCheckResult(passed=not failing, severity=dg.AssetCheckSeverity.ERROR, metadata=metadata)


def run_parity_check(client: Any) -> dg.AssetCheckResult:
    exists = client.execute(
        f"SELECT count() FROM system.tables WHERE database = '{DATABASE}' AND name = '{PARITY_SNAPSHOT}'")
    if int(exists[0][0]) == 0:
        return dg.AssetCheckResult(passed=False, metadata={"error": dg.MetadataValue.text(
            f"{DATABASE}.{PARITY_SNAPSHOT} does not exist: run build_parity_snapshot_sql() against the OLD "
            "table before the rebuild (cutover step 3)")})
    row = client.execute(build_parity_sql())[0]
    values = dict(zip(PARITY_COLUMNS, row, strict=True))
    counts = {name: int(values[name]) for name in PARITY_COLUMNS if not name.endswith("_samples")}
    samples = {name[: -len("_samples")]: [str(c) for c in values[name]]
               for name in PARITY_COLUMNS if name.endswith("_samples")}
    per_source = [(str(field), str(source), int(rows)) for field, source, rows in client.execute(build_rows_per_field_source_sql())]
    return parity_result(counts, samples, per_source)


@dg.asset_check(
    asset=dg.AssetKey(RESOLVE_ASSET),
    name=PARITY_CHECK_NAME,
    description=("Cutover parity of the rebuilt se_company_info against se_company_info_parity_snapshot: "
                 "legal facts, codes and descriptions per spec 12; run on demand, not by the jobs."),
)
def se_company_field_parity_check(clickhouse: ClickhouseResource) -> dg.AssetCheckResult:
    with clickhouse.get_connection() as client:
        return run_parity_check(client)


defs = dg.Definitions(asset_checks=[se_company_field_parity_check])
