"""Raw -> structured Parquet.

prh_ytj (JSONL): native Polars — read NDJSON into nested structs/lists and
reshape with vectorized expressions. No Python row loop, no copied parser.
prh_xbrl (XML): reuse the copied lxml parser (XML is not tabular), then wrap
its rows into Polars.

Pure: bytes/statements in, dict[table_name -> polars.DataFrame] out. The
notebook handles S3 read and Parquet write at its edges. These functions are
the future structured-layer Dagster assets.
"""

from __future__ import annotations

import datetime as dt

import polars as pl

from conformance._vendor.prh_xbrl_parser import parse_statement_xml
from conformance._vendor import prh_xbrl_tables as xbrl_tables


def ytj_structured_from_ndjson(ndjson: bytes) -> dict[str, pl.DataFrame]:
    """prh_ytj JSONL -> structured frames, idiomatic Polars. Domain rules
    (the status='2' liveness pitfall, current-primary name, URL normalization)
    are expressed as Polars expressions."""
    df = pl.read_ndjson(ndjson, infer_schema_length=None).with_columns(
        pl.col("businessId").struct.field("value").alias("business_id")
    )

    statuses = (
        df.select(
            "business_id",
            pl.col("tradeRegisterStatus").alias("trade_register_status"),
            pl.col("registrationDate").alias("registration_date"),
            pl.col("endDate").fill_null("").alias("end_date"),
        )
        .with_columns(
            pl.when((pl.col("end_date") != "") | (pl.col("trade_register_status") == "3"))
            .then(pl.lit("ceased")).otherwise(pl.lit("active")).alias("lifecycle_status")
        )
        .with_columns((pl.col("lifecycle_status") == "active").alias("is_active"))
    )

    names = (
        df.select("business_id", "names")
        .explode("names").drop_nulls("names").unnest("names")
        .select(
            "business_id", "name",
            pl.col("type").alias("name_type_code"),
            pl.col("endDate").is_null().alias("is_current"),
            (pl.col("type") == "1").alias("is_primary"),
        )
    )

    _website_cols = ["business_id", "url", "normalized_url", "host", "is_current", "is_primary"]
    if "website" not in df.columns:
        websites = pl.DataFrame(schema={c: pl.Utf8 for c in _website_cols}).with_columns(
            pl.col("is_current").cast(pl.Boolean),
            pl.col("is_primary").cast(pl.Boolean),
        )
    else:
        websites = (
            df.select("business_id", "website")
            .with_columns(
                pl.when(pl.col("website").is_not_null())
                .then(pl.col("website").struct.field("url"))
                .otherwise(pl.lit(None, dtype=pl.Utf8)).alias("url")
            )
            .filter(pl.col("url").is_not_null() & (pl.col("url") != ""))
            .with_columns(
                pl.when(pl.col("url").str.contains("://")).then(pl.col("url"))
                .otherwise(pl.concat_str([pl.lit("https://"), pl.col("url")])).alias("normalized_url")
            )
            .with_columns(
                pl.col("normalized_url").str.replace(r"^https?://", "").str.split("/").list.first().alias("host"),
                pl.lit(True).alias("is_current"),
                pl.lit(True).alias("is_primary"),
            )
            .select("business_id", "url", "normalized_url", "host", "is_current", "is_primary")
        )

    _addr_exploded = (
        df.select("business_id", "addresses")
        .explode("addresses").drop_nulls("addresses").unnest("addresses")
        .with_columns(
            pl.col("postOffices").list.eval(pl.element().struct.field("city")).list.first().alias("city"),
            pl.col("postOffices").list.eval(pl.element().struct.field("municipalityCode")).list.first().alias("municipality_code"),
        )
    )
    # `country` may be absent in the real PRH-YTJ payload; default to "FI".
    _country_expr = (
        pl.col("country") if "country" in _addr_exploded.columns else pl.lit("FI")
    )
    addresses = _addr_exploded.select(
        "business_id",
        pl.col("type").alias("address_type_code"),
        "street",
        pl.col("postCode").alias("post_code"),
        "city", "municipality_code",
        _country_expr.alias("country"),
    )

    business_lines = (
        df.select("business_id", "mainBusinessLine")
        .with_columns(
            pl.col("mainBusinessLine").struct.field("type").alias("business_line_type"),
            pl.col("mainBusinessLine").struct.field("typeCodeSet").alias("business_line_code_set"),
        )
        .filter(pl.col("business_line_type").is_not_null())
        .select("business_id", "business_line_type", "business_line_code_set")
    )

    return {
        "fi_prhytj_statuses": statuses,
        "fi_prhytj_names": names,
        "fi_prhytj_websites": websites,
        "fi_prhytj_addresses": addresses,
        "fi_prhytj_business_lines": business_lines,
    }


def xbrl_structured_from_statements(
    statements: list[dict], *, run_id: str, parsed_at: dt.datetime
) -> dict[str, pl.DataFrame]:
    by_table: dict[str, list[dict]] = {
        xbrl_tables.STATEMENT_DOCUMENTS_TABLE: [], xbrl_tables.CONTEXTS_TABLE: [],
        xbrl_tables.UNITS_TABLE: [], xbrl_tables.FACTS_TABLE: [],
    }
    for s in statements:
        parsed = parse_statement_xml(
            business_id=s["business_id"], financial_date=s["financial_date"],
            registration_date=s.get("registration_date"), source_url=s["source_url"],
            xml_object_key=s["object_key"], source_run_id=run_id,
            body=s["body"], parsed_at=parsed_at,
        )
        for table, rows in parsed.rows_by_table.items():
            by_table[table].extend(rows)
    # Drop nested fact/context columns Polars can't infer flatly; not consumed downstream.
    drop = {"dimensions", "measures", "schema_refs", "validation_warnings"}
    out = {}
    for table, rows in by_table.items():
        frame = pl.DataFrame(rows, infer_schema_length=None) if rows else pl.DataFrame()
        cleaned = frame.drop([c for c in drop if c in frame.columns]) if rows else frame
        out[table] = cleaned
        # Expose facts under the shorter alias used by downstream consumers / tests.
        if table == xbrl_tables.FACTS_TABLE:
            out["fi_prh_xbrl_facts"] = cleaned
    return out
