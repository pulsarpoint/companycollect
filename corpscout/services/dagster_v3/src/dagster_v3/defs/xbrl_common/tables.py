"""Canonical XBRL row shapes shared by all national sources.

A source table = source identity columns + these canonical columns +
optionally appended source-specific derived columns (e.g. Finland's
mcy_member_code). Identity and extras are Utf8 unless the source overrides.
"""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl

XBRL_DOCUMENT_POLARS_SCHEMA = {
    "xml_sha256": pl.Utf8,
    "xml_size_bytes": pl.Int64,
    "root_name": pl.Utf8,
    "schema_refs": pl.Utf8,
    "taxonomy_entrypoint": pl.Utf8,
    "reported_entity_id": pl.Utf8,
    "reported_company_name": pl.Utf8,
    "reported_period_start": pl.Utf8,
    "reported_period_end": pl.Utf8,
    "contexts_count": pl.Int64,
    "units_count": pl.Int64,
    "facts_count": pl.Int64,
    "validation_warnings": pl.Utf8,
    "parser_version": pl.Utf8,
    "parsed_at": pl.Utf8,
}
XBRL_DOCUMENT_COLUMNS = tuple(XBRL_DOCUMENT_POLARS_SCHEMA)

XBRL_CONTEXT_POLARS_SCHEMA = {
    "context_id": pl.Utf8,
    "entity_identifier": pl.Utf8,
    "entity_scheme": pl.Utf8,
    "period_type": pl.Utf8,
    "instant_date": pl.Utf8,
    "period_start": pl.Utf8,
    "period_end": pl.Utf8,
    "dimensions": pl.Utf8,
    "is_comparative": pl.Boolean,
    "parser_version": pl.Utf8,
    "parsed_at": pl.Utf8,
}
XBRL_CONTEXT_COLUMNS = tuple(XBRL_CONTEXT_POLARS_SCHEMA)

XBRL_UNIT_POLARS_SCHEMA = {
    "unit_id": pl.Utf8,
    "measures": pl.Utf8,
    "numerator_measures": pl.Utf8,
    "denominator_measures": pl.Utf8,
    "is_divide": pl.Boolean,
    "currency": pl.Utf8,
    "parser_version": pl.Utf8,
    "parsed_at": pl.Utf8,
}
XBRL_UNIT_COLUMNS = tuple(XBRL_UNIT_POLARS_SCHEMA)

XBRL_FACT_POLARS_SCHEMA = {
    "fact_ordinal": pl.Int64,
    "concept_qname": pl.Utf8,
    "concept_namespace": pl.Utf8,
    "concept_local_name": pl.Utf8,
    "context_id": pl.Utf8,
    "unit_id": pl.Utf8,
    "currency": pl.Utf8,
    "decimals": pl.Utf8,
    "precision": pl.Utf8,
    "is_nil": pl.Boolean,
    "xml_lang": pl.Utf8,
    "value_kind": pl.Utf8,
    "raw_value": pl.Utf8,
    "numeric_value": pl.Utf8,
    "date_value": pl.Utf8,
    "text_value": pl.Utf8,
    "dimensions": pl.Utf8,
    "is_comparative": pl.Boolean,
    "parser_version": pl.Utf8,
    "parsed_at": pl.Utf8,
}
XBRL_FACT_COLUMNS = tuple(XBRL_FACT_POLARS_SCHEMA)

TAXONOMY_CONCEPT_COLUMNS = (
    "taxonomy_version",
    "concept_qname",
    "concept_namespace",
    "concept_local_name",
    "substitution_group",
    "is_abstract",
    "item_type",
    "balance",
    "period_type",
    "presentation_parent",
    "presentation_order",
    "presentation_role",
    "calculation_parent",
    "calculation_weight",
    "calculation_role",
    "loaded_at",
)
TAXONOMY_LABEL_COLUMNS = (
    "taxonomy_version",
    "concept_qname",
    "language",
    "label_role",
    "label",
    "loaded_at",
)


@dataclass(frozen=True)
class TableContract:
    columns: list[str]
    schema: dict[str, pl.DataType]


@dataclass(frozen=True)
class XbrlRowContract:
    documents: TableContract
    contexts: TableContract
    units: TableContract
    facts: TableContract

    @staticmethod
    def build(
        *,
        document_identity: tuple[str, ...],
        row_identity: tuple[str, ...],
        fact_identity: tuple[str, ...],
        context_extras: tuple[str, ...] = (),
        fact_extras: tuple[str, ...] = (),
    ) -> "XbrlRowContract":
        def _table(
            identity: tuple[str, ...],
            canonical: dict[str, pl.DataType],
            extras: tuple[str, ...] = (),
        ) -> TableContract:
            schema: dict[str, pl.DataType] = {name: pl.Utf8 for name in identity}
            schema.update(canonical)
            schema.update({name: pl.Utf8 for name in extras})
            return TableContract(columns=list(schema), schema=schema)

        return XbrlRowContract(
            documents=_table(document_identity, XBRL_DOCUMENT_POLARS_SCHEMA),
            contexts=_table(row_identity, XBRL_CONTEXT_POLARS_SCHEMA, context_extras),
            units=_table(row_identity, XBRL_UNIT_POLARS_SCHEMA),
            facts=_table(fact_identity, XBRL_FACT_POLARS_SCHEMA, fact_extras),
        )
