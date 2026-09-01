import polars as pl

from dagster_v3.defs.xbrl_common import tables as xt


def test_canonical_columns_match_schemas():
    assert list(xt.XBRL_DOCUMENT_COLUMNS) == list(xt.XBRL_DOCUMENT_POLARS_SCHEMA)
    assert list(xt.XBRL_CONTEXT_COLUMNS) == list(xt.XBRL_CONTEXT_POLARS_SCHEMA)
    assert list(xt.XBRL_UNIT_COLUMNS) == list(xt.XBRL_UNIT_POLARS_SCHEMA)
    assert list(xt.XBRL_FACT_COLUMNS) == list(xt.XBRL_FACT_POLARS_SCHEMA)


def test_canonical_fact_columns_exact():
    assert xt.XBRL_FACT_COLUMNS == (
        "fact_ordinal", "concept_qname", "concept_namespace", "concept_local_name",
        "context_id", "unit_id", "currency", "decimals", "precision", "is_nil",
        "xml_lang", "value_kind", "raw_value", "numeric_value", "date_value",
        "text_value", "dimensions", "is_comparative", "parser_version", "parsed_at",
    )


def test_row_contract_composes_identity_and_extras():
    contract = xt.XbrlRowContract.build(
        document_identity=("statement_key", "business_id"),
        row_identity=("statement_key",),
        fact_identity=("statement_key", "business_id"),
        context_extras=("mcy_member_code",),
        fact_extras=("mcy_member_code",),
    )
    assert contract.documents.columns[:2] == ["statement_key", "business_id"]
    assert contract.contexts.columns[0] == "statement_key"
    assert contract.contexts.columns[-1] == "mcy_member_code"
    assert contract.facts.columns[-1] == "mcy_member_code"
    assert contract.contexts.schema["mcy_member_code"] == pl.Utf8
    assert set(xt.XBRL_FACT_COLUMNS) <= set(contract.facts.columns)
