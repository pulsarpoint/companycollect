"""Sweden XBRL concept vocabulary and authoritative taxonomy dictionary.

The facts table holds ~290M rows but only ~1.8k distinct Swedish concept
names (``Nettoomsattning``, ``RakenskapsarForstaDag``, ...). The feed asset
maintains ``corpscout.se_financial_facts_concepts`` (INSERT new concepts
only -- merge semantics, never replace) so the translator loader scans a
tiny table instead of the facts table.

The Swedish taxonomy publishes authoritative standard and documentation labels.
``se_financial_taxonomy_concepts`` preserves those fields exactly as published.
Machine translations of missing English text use the shared
``text_translations`` cache; migration-owned serving views resolve official
English, translated Swedish, and identifier fallbacks without rewriting source
taxonomy rows.
"""

from dataclasses import dataclass
from datetime import UTC, datetime
from importlib.metadata import version
from typing import Any

import dagster as dg
from arelle import Cntlr, ModelManager, XbrlConst
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource
from pydantic import Field

from dagster_v3.defs.translator_load.coverage import translation_coverage_result
from dagster_v3.defs.translator_load.loader import TranslationField, build_scan_sql
from dagster_v3.defs.translator_load.resource import TranslatorResource

GROUP_NAME = "sweden_financial"
QUALIFIED_SE_FINANCIAL_FACTS_CONCEPTS_TABLE = "corpscout.se_financial_facts_concepts"
QUALIFIED_SE_FINANCIAL_TAXONOMY_CONCEPTS_TABLE = (
    "corpscout.se_financial_taxonomy_concepts"
)
QUALIFIED_SE_FINANCIAL_TAXONOMY_LOADS_TABLE = "corpscout.se_financial_taxonomy_loads"
QUALIFIED_SE_FINANCIAL_TAXONOMY_CONCEPTS_CURRENT_VIEW = (
    "corpscout.se_financial_taxonomy_concepts_current"
)
QUALIFIED_SE_FINANCIAL_REPORTS_TABLE = "corpscout.se_financial_reports"

SOURCE_LANG = "sv"
TARGET_LANG = "en"
SOURCE_LANGUAGE_NAME = "Swedish"
TARGET_LANGUAGE_NAME = "English"
TAXONOMY_PARSER_VERSION = f"arelle-{version('arelle-release')}"

_TAXONOMY_CONCEPT_INSERT_COLUMNS = (
    "taxonomy_entrypoint",
    "concept_qname",
    "concept_namespace",
    "concept_local_name",
    "label_sv",
    "label_en",
    "description_sv",
    "description_en",
    "type_qname",
    "base_xsd_type",
    "period_type",
    "balance",
    "is_numeric",
    "is_abstract",
    "concept_source_url",
    "parser_version",
    "resolved_at",
)

TAXONOMY_TRANSLATION_FIELDS = (
    TranslationField(
        QUALIFIED_SE_FINANCIAL_TAXONOMY_CONCEPTS_CURRENT_VIEW,
        "label_sv",
        SOURCE_LANG,
        TARGET_LANG,
        "label_en = ''",
    ),
    TranslationField(
        QUALIFIED_SE_FINANCIAL_TAXONOMY_CONCEPTS_CURRENT_VIEW,
        "description_sv",
        SOURCE_LANG,
        TARGET_LANG,
        "description_en = ''",
    ),
)

_OFFICIAL_TRANSLATION_COLUMNS = (
    ("label_sv", "label_en"),
    ("description_sv", "description_en"),
)


def _official_taxonomy_translation_insert_sql(
    *,
    source_column: str,
    target_column: str,
) -> str:
    """Insert changed official Swedish→English taxonomy text into the cache."""
    return f"""
INSERT INTO corpscout.text_translations (
    source_table,
    source_column,
    source_text_hash,
    source_lang,
    target_lang,
    translated_text,
    provider,
    model,
    version
)
WITH candidates AS (
    SELECT
        cityHash64({source_column}) AS source_text_hash,
        argMax(
            {target_column},
            tuple(taxonomy_resolved_at, taxonomy_entrypoint)
        ) AS translated_text,
        toUInt64(toUnixTimestamp64Milli(max(taxonomy_resolved_at))) AS version
    FROM {QUALIFIED_SE_FINANCIAL_TAXONOMY_CONCEPTS_CURRENT_VIEW}
    WHERE {source_column} != '' AND {target_column} != ''
    GROUP BY source_text_hash
),
current AS (
    SELECT
        source_text_hash,
        argMax(translated_text, version) AS translated_text
    FROM corpscout.text_translations
    WHERE source_table = '{QUALIFIED_SE_FINANCIAL_TAXONOMY_CONCEPTS_CURRENT_VIEW}'
      AND source_column = '{source_column}'
      AND source_lang = '{SOURCE_LANG}'
      AND target_lang = '{TARGET_LANG}'
    GROUP BY source_text_hash
)
SELECT
    '{QUALIFIED_SE_FINANCIAL_TAXONOMY_CONCEPTS_CURRENT_VIEW}',
    '{source_column}',
    candidates.source_text_hash,
    '{SOURCE_LANG}',
    '{TARGET_LANG}',
    candidates.translated_text,
    'taxonomy',
    'bolagsverket-official-taxonomy',
    candidates.version
FROM candidates
LEFT JOIN current USING (source_text_hash)
WHERE current.source_text_hash = 0
   OR current.translated_text != candidates.translated_text
"""


class SwedenFinancialTaxonomyConceptsConfig(dg.Config):
    """Selection and idempotency controls for deterministic taxonomy loading."""

    taxonomy_entrypoints: list[str] = Field(default_factory=list)
    max_taxonomies: int = Field(default=25, ge=1, le=1_000)
    refresh_existing: bool = False


@dataclass(frozen=True)
class SwedenFinancialTaxonomyConcept:
    taxonomy_entrypoint: str
    concept_qname: str
    concept_namespace: str
    concept_local_name: str
    label_sv: str
    label_en: str
    description_sv: str
    description_en: str
    type_qname: str
    base_xsd_type: str
    period_type: str
    balance: str
    is_numeric: bool
    is_abstract: bool
    concept_source_url: str

    def clickhouse_row(self, *, resolved_at: datetime) -> tuple[object, ...]:
        return (
            self.taxonomy_entrypoint,
            self.concept_qname,
            self.concept_namespace,
            self.concept_local_name,
            self.label_sv,
            self.label_en,
            self.description_sv,
            self.description_en,
            self.type_qname,
            self.base_xsd_type,
            self.period_type,
            self.balance,
            self.is_numeric,
            self.is_abstract,
            self.concept_source_url,
            TAXONOMY_PARSER_VERSION,
            resolved_at,
        )


_INSERT_NEW_CONCEPTS_SQL = f"""
INSERT INTO {QUALIFIED_SE_FINANCIAL_FACTS_CONCEPTS_TABLE}
    (concept_local_name, concept_namespace)
SELECT DISTINCT
    f.concept_local_name,
    f.concept_namespace
FROM corpscout.se_financial_facts AS f
LEFT ANTI JOIN {QUALIFIED_SE_FINANCIAL_FACTS_CONCEPTS_TABLE} AS e
    ON e.concept_local_name = f.concept_local_name
   AND e.concept_namespace = f.concept_namespace
WHERE f.concept_local_name <> ''
"""


@dg.asset(
    deps=[
        dg.AssetDep(
            dg.AssetKey("sweden_financial_backfill_facts_clickhouse"),
            partition_mapping=dg.AllPartitionMapping(),
        ),
        "sweden_financial_current_facts_clickhouse",
    ],
    group_name=GROUP_NAME,
    kinds={"python", "clickhouse", "xbrl"},
    metadata={"table": QUALIFIED_SE_FINANCIAL_FACTS_CONCEPTS_TABLE},
    description=(
        "Maintains the distinct Swedish XBRL concept vocabulary table by "
        "inserting concepts newly seen in se_financial_facts -- never a "
        "replace, so translated concepts are never re-queued."
    ),
)
def se_financial_facts_concepts(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    with clickhouse.get_connection() as client:
        before = int(
            client.execute(
                f"SELECT count() FROM {QUALIFIED_SE_FINANCIAL_FACTS_CONCEPTS_TABLE}"
            )[0][0]
        )
        client.execute(_INSERT_NEW_CONCEPTS_SQL)
        after = int(
            client.execute(
                f"SELECT count() FROM {QUALIFIED_SE_FINANCIAL_FACTS_CONCEPTS_TABLE}"
            )[0][0]
        )
    context.log.info(
        "Sweden concept vocabulary: %d known, %d newly inserted", before, after - before
    )
    return dg.MaterializeResult(
        metadata={
            "concepts_total": after,
            "concepts_inserted": after - before,
        }
    )


def load_sweden_financial_taxonomy_concepts(
    taxonomy_entrypoint: str,
) -> list[SwedenFinancialTaxonomyConcept]:
    """Load one official taxonomy DTS and return its concept dictionary."""
    controller = Cntlr.Cntlr(logFileName="logToBuffer")
    model = None
    try:
        model_manager = ModelManager.initialize(controller)
        model = model_manager.load(taxonomy_entrypoint)
        if model is None or not model.qnameConcepts:
            raise ValueError(
                f"Arelle loaded no taxonomy concepts from {taxonomy_entrypoint}"
            )

        rows: list[SwedenFinancialTaxonomyConcept] = []
        for qname, concept in sorted(
            model.qnameConcepts.items(),
            key=lambda item: str(item[0]),
        ):
            model_document = getattr(concept, "modelDocument", None)
            rows.append(
                SwedenFinancialTaxonomyConcept(
                    taxonomy_entrypoint=taxonomy_entrypoint,
                    concept_qname=str(qname),
                    concept_namespace=str(qname.namespaceURI or ""),
                    concept_local_name=str(qname.localName or ""),
                    label_sv=_taxonomy_label(concept, language="sv"),
                    label_en=_taxonomy_label(concept, language="en"),
                    description_sv=_taxonomy_label(
                        concept,
                        language="sv",
                        preferred_label=XbrlConst.documentationLabel,
                    ),
                    description_en=_taxonomy_label(
                        concept,
                        language="en",
                        preferred_label=XbrlConst.documentationLabel,
                    ),
                    type_qname=str(getattr(concept, "typeQname", "") or ""),
                    base_xsd_type=str(getattr(concept, "baseXsdType", "") or ""),
                    period_type=str(getattr(concept, "periodType", "") or ""),
                    balance=str(getattr(concept, "balance", "") or ""),
                    is_numeric=bool(getattr(concept, "isNumeric", False)),
                    is_abstract=bool(getattr(concept, "isAbstract", False)),
                    concept_source_url=str(
                        getattr(model_document, "uri", "") or taxonomy_entrypoint
                    ),
                )
            )
        return rows
    finally:
        if model is not None:
            model.close()
        controller.close()


def _taxonomy_label(
    concept: Any,
    *,
    language: str,
    preferred_label: str | None = None,
) -> str:
    value = concept.label(
        preferredLabel=preferred_label,
        lang=language,
        fallbackToQname=False,
        strip=True,
    )
    return " ".join(str(value or "").split())


def _pending_taxonomy_entrypoints_sql(*, refresh_existing: bool) -> str:
    successful_load_filter = ""
    if not refresh_existing:
        successful_load_filter = "AND ifNull(loads.status, '') != 'success'"
    return f"""
SELECT DISTINCT reports.taxonomy_entrypoint
FROM {QUALIFIED_SE_FINANCIAL_REPORTS_TABLE} AS reports
LEFT JOIN (
    SELECT taxonomy_entrypoint, argMax(status, resolved_at) AS status
    FROM {QUALIFIED_SE_FINANCIAL_TAXONOMY_LOADS_TABLE}
    GROUP BY taxonomy_entrypoint
) AS loads
    ON loads.taxonomy_entrypoint = ifNull(reports.taxonomy_entrypoint, '')
WHERE ifNull(reports.taxonomy_entrypoint, '') != ''
  AND (
      empty(%(taxonomy_entrypoints)s)
      OR has(%(taxonomy_entrypoints)s, ifNull(reports.taxonomy_entrypoint, ''))
  )
  {successful_load_filter}
ORDER BY reports.taxonomy_entrypoint
LIMIT %(max_taxonomies)s
"""


@dg.asset(
    deps=[
        se_financial_facts_concepts,
        dg.AssetDep(
            dg.AssetKey("sweden_financial_backfill_reports_clickhouse"),
            partition_mapping=dg.AllPartitionMapping(),
        ),
        "sweden_financial_current_reports_clickhouse",
    ],
    group_name=GROUP_NAME,
    kinds={"python", "clickhouse", "xbrl"},
    pool="sweden_financial_taxonomy",
    metadata={"table": QUALIFIED_SE_FINANCIAL_TAXONOMY_CONCEPTS_TABLE},
    description=(
        "Loads each new Sweden annual-account taxonomy entrypoint with Arelle "
        "and stores authoritative Swedish/English labels, documentation "
        "labels, data types, period types, balances, and source URLs."
    ),
)
def se_financial_taxonomy_concepts(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
    config: SwedenFinancialTaxonomyConceptsConfig,
) -> dg.MaterializeResult:
    selection_parameters = {
        "taxonomy_entrypoints": sorted(
            {
                entrypoint.strip()
                for entrypoint in config.taxonomy_entrypoints
                if entrypoint.strip()
            }
        ),
        "max_taxonomies": config.max_taxonomies,
    }
    with clickhouse.get_connection() as client:
        entrypoints = [
            str(row[0])
            for row in client.execute(
                _pending_taxonomy_entrypoints_sql(
                    refresh_existing=config.refresh_existing
                ),
                selection_parameters,
            )
        ]

    context.log.info(
        "Selected %d Sweden financial taxonomy entrypoints", len(entrypoints)
    )
    concept_count = 0
    english_label_count = 0
    english_description_count = 0
    failures: list[tuple[str, str]] = []
    insert_columns = ", ".join(_TAXONOMY_CONCEPT_INSERT_COLUMNS)

    for entrypoint in entrypoints:
        resolved_at = datetime.now(UTC)
        try:
            rows = load_sweden_financial_taxonomy_concepts(entrypoint)
            rows_with_english_labels = sum(row.label_en != "" for row in rows)
            rows_with_english_descriptions = sum(
                row.description_en != "" for row in rows
            )
            with clickhouse.get_connection() as client:
                client.execute(
                    f"INSERT INTO {QUALIFIED_SE_FINANCIAL_TAXONOMY_CONCEPTS_TABLE} "
                    f"({insert_columns}) VALUES",
                    [row.clickhouse_row(resolved_at=resolved_at) for row in rows],
                )
                client.execute(
                    f"INSERT INTO {QUALIFIED_SE_FINANCIAL_TAXONOMY_LOADS_TABLE} "
                    "(taxonomy_entrypoint, status, concept_count, "
                    "concepts_with_english_labels, "
                    "concepts_with_english_descriptions, parser_version, "
                    "error_message, source_run_id, resolved_at) VALUES",
                    [
                        (
                            entrypoint,
                            "success",
                            len(rows),
                            rows_with_english_labels,
                            rows_with_english_descriptions,
                            TAXONOMY_PARSER_VERSION,
                            "",
                            context.run_id,
                            resolved_at,
                        )
                    ],
                )
            concept_count += len(rows)
            english_label_count += rows_with_english_labels
            english_description_count += rows_with_english_descriptions
            context.log.info(
                "Loaded taxonomy %s: %d concepts, %d English labels, "
                "%d English descriptions",
                entrypoint,
                len(rows),
                rows_with_english_labels,
                rows_with_english_descriptions,
            )
        except Exception as exc:
            error_message = str(exc)[:4_000]
            failures.append((entrypoint, error_message))
            with clickhouse.get_connection() as client:
                client.execute(
                    f"INSERT INTO {QUALIFIED_SE_FINANCIAL_TAXONOMY_LOADS_TABLE} "
                    "(taxonomy_entrypoint, status, concept_count, "
                    "concepts_with_english_labels, "
                    "concepts_with_english_descriptions, parser_version, "
                    "error_message, source_run_id, resolved_at) VALUES",
                    [
                        (
                            entrypoint,
                            "failed",
                            0,
                            0,
                            0,
                            TAXONOMY_PARSER_VERSION,
                            error_message,
                            context.run_id,
                            resolved_at,
                        )
                    ],
                )
            context.log.error(
                "Failed to load Sweden financial taxonomy %s: %s",
                entrypoint,
                error_message,
            )

    if failures:
        raise dg.Failure(
            description=(
                f"Failed to load {len(failures)} of {len(entrypoints)} "
                "Sweden financial taxonomies"
            ),
            metadata={
                "failures": dg.MetadataValue.json(
                    [
                        {"taxonomy_entrypoint": entrypoint, "error": error}
                        for entrypoint, error in failures
                    ]
                )
            },
        )

    return dg.MaterializeResult(
        metadata={
            "taxonomy_entrypoints_selected": len(entrypoints),
            "concepts_loaded": concept_count,
            "concepts_with_english_labels": english_label_count,
            "concepts_with_english_descriptions": english_description_count,
            "parser_version": TAXONOMY_PARSER_VERSION,
        }
    )


@dg.asset(
    deps=[se_financial_taxonomy_concepts],
    group_name=GROUP_NAME,
    kinds={"python", "clickhouse", "xbrl", "taxonomy"},
    description=(
        "Copies authoritative Swedish-to-English standard and documentation "
        "label pairs into the shared text_translations cache. Source taxonomy "
        "rows remain unchanged and retain the official evidence."
    ),
)
def se_financial_taxonomy_official_translations(
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    with clickhouse.get_connection() as client:
        for source_column, target_column in _OFFICIAL_TRANSLATION_COLUMNS:
            client.execute(
                _official_taxonomy_translation_insert_sql(
                    source_column=source_column,
                    target_column=target_column,
                )
            )
        [(official_pair_count,)] = client.execute(
            f"""
SELECT count()
FROM corpscout.text_translations
WHERE source_table = '{QUALIFIED_SE_FINANCIAL_TAXONOMY_CONCEPTS_CURRENT_VIEW}'
  AND source_lang = '{SOURCE_LANG}'
  AND target_lang = '{TARGET_LANG}'
  AND provider = 'taxonomy'
"""
        )
    return dg.MaterializeResult(
        metadata={"official_translation_pair_count": int(official_pair_count)}
    )


@dg.asset(
    deps=[se_financial_taxonomy_official_translations],
    group_name=GROUP_NAME,
    kinds={"python", "clickhouse"},
    description=(
        "Translate official Swedish taxonomy labels and documentation only "
        "when the taxonomy publishes no English equivalent. Generated text "
        "is stored in text_translations, never in the source taxonomy table."
    ),
)
def sweden_financial_taxonomy_translation_load(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
    translator: TranslatorResource,
) -> dg.MaterializeResult:
    baseline_failed = translator.queue_stats().failed
    enqueued_received = 0
    enqueued_inserted = 0
    workflow_start_warnings: list[str] = []
    scanned_by_column: dict[str, int] = {}

    with clickhouse.get_connection() as client:
        for field in TAXONOMY_TRANSLATION_FIELDS:
            untranslated_rows = client.execute(
                build_scan_sql(
                    field.table,
                    field.column,
                    source_lang=field.source_lang,
                    target_lang=field.target_lang,
                    extra_where=field.extra_where,
                )
            )
            scanned_by_column[field.column] = len(untranslated_rows)
            context.log.info(
                "scanned %d missing taxonomy translations for %s.%s",
                len(untranslated_rows),
                field.table,
                field.column,
            )
            enqueue_result = translator.enqueue_translation_rows(
                source_table=field.table,
                source_column=field.column,
                source_lang=SOURCE_LANG,
                target_lang=TARGET_LANG,
                source_language_name=SOURCE_LANGUAGE_NAME,
                target_language_name=TARGET_LANGUAGE_NAME,
                rows=untranslated_rows,
            )
            enqueued_received += enqueue_result.received
            enqueued_inserted += enqueue_result.inserted
            workflow_start_warnings.extend(enqueue_result.workflow_start_warnings)
            for warning in enqueue_result.workflow_start_warnings:
                context.log.warning("translator workflow start warning: %s", warning)

    if workflow_start_warnings:
        raise dg.Failure(
            description="translator accepted rows but failed to start its workflow",
            metadata={
                "warning_count": len(workflow_start_warnings),
                "warnings": dg.MetadataValue.json(workflow_start_warnings),
            },
        )

    if enqueued_received > 0:
        completion_stats = translator.wait_for_queue_completion(
            baseline_failed=baseline_failed
        )
        context.log.info(
            "translator queue completed: input=%d pending=%d output=%d failed=%d",
            completion_stats.input,
            completion_stats.pending,
            completion_stats.output,
            completion_stats.failed,
        )
    return dg.MaterializeResult(
        metadata={
            "labels_scanned": scanned_by_column.get("label_sv", 0),
            "descriptions_scanned": scanned_by_column.get("description_sv", 0),
            "enqueued_received": enqueued_received,
            "enqueued_inserted": enqueued_inserted,
        }
    )


@dg.asset_check(
    asset=sweden_financial_taxonomy_translation_load,
    name="translations_present",
)
def sweden_financial_taxonomy_translation_coverage(
    clickhouse: ClickhouseResource,
) -> dg.AssetCheckResult:
    """Coverage of missing official-English taxonomy labels and descriptions."""
    return translation_coverage_result(
        clickhouse,
        TAXONOMY_TRANSLATION_FIELDS,
    )
