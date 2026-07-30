import hashlib
import json
import uuid
from datetime import UTC, datetime

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource
from dlt.sources.helpers import requests

from dagster_v3.defs.brazil_companies.cnae import tables
from dagster_v3.defs.brazil_companies.cnae.vocabulary import (
    CNAE_SUBCLASSES_URL,
    build_cnae_category_rows,
    build_cnae_to_nace_rows,
    nace_division_edges,
    nace_division_label,
    parse_cnae_subclasses,
)
from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist

GROUP_NAME = "brazil_comp_cnae"


def _fetch_subclasses() -> tuple[list[dict], str]:
    """IBGE's subclass listing, with a hash of exactly what was read."""
    response = requests.get(CNAE_SUBCLASSES_URL, timeout=120)
    response.raise_for_status()
    return json.loads(response.content), hashlib.sha256(response.content).hexdigest()


def _replace_table(client, table: str, columns, rows) -> None:
    """Stage, fill, EXCHANGE — the same atomic swap the mapping has always
    used, so a reader never sees a half-written vocabulary."""
    qualified = f"`{tables.BRAZIL_COMP_CNAE_DATABASE}`.`{table}`"
    stage = f"`{tables.BRAZIL_COMP_CNAE_DATABASE}`.`_tmp_{table}_{uuid.uuid4().hex}`"
    try:
        client.execute(f"CREATE TABLE {stage} AS {qualified}")
        client.execute(f"INSERT INTO {stage} ({', '.join(columns)}) VALUES", rows)
        client.execute(f"EXCHANGE TABLES {stage} AND {qualified}")
    finally:
        client.execute(f"DROP TABLE IF EXISTS {stage}")


@dg.asset(
    group_name=GROUP_NAME,
    kinds={"python", "clickhouse", "reference"},
    description=(
        "CNAE 2.0 from IBGE: 1,332 subclasses and their ancestors, in Portuguese."
    ),
)
def brazil_comp_cnae_categories_clickhouse(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.BRAZIL_COMP_CNAE_DATABASE,
        tables=(tables.BR_CNAE_CATEGORIES_TABLE,),
    )
    subclasses, _ = _fetch_subclasses()
    rows = build_cnae_category_rows(
        subclasses=subclasses,
        source_run_id=context.run_id,
        retrieved_at=datetime.now(UTC).replace(tzinfo=None),
    )
    if len(rows) < tables.MIN_CNAE_CATEGORY_ROWS:
        # Refuse to replace on a short read rather than unname the industry of
        # 71.9M establishments.
        raise ValueError(
            f"CNAE vocabulary yielded {len(rows)} rows, "
            f"below the {tables.MIN_CNAE_CATEGORY_ROWS} floor"
        )

    with clickhouse.get_connection() as client:
        _replace_table(
            client,
            tables.BR_CNAE_CATEGORIES_TABLE,
            tables.BR_CNAE_CATEGORIES_COLUMNS,
            rows,
        )

    levels: dict[str, int] = {}
    for row in rows:
        levels[str(row[3])] = levels.get(str(row[3]), 0) + 1
    return dg.MaterializeResult(metadata={"row_count": len(rows), **levels})


@dg.asset(
    deps=[
        dg.AssetKey("nace_categories_clickhouse"),
        dg.AssetKey("brazil_comp_cnae_categories_clickhouse"),
    ],
    group_name=GROUP_NAME,
    kinds={"clickhouse", "reference"},
    description=(
        "Brazil CNAE to NACE edges at DIVISION level, from the shared ISIC Rev.4 "
        "ancestry."
    ),
)
def brazil_comp_cnae_to_nace_clickhouse(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    """One edge per CNAE subclass, to the NACE division it shares with ISIC.

    Division and no deeper, and this replaces a three-row hand-curated fixture
    that covered two codes. CNAE 2.0 and NACE Rev.2 both descend from ISIC
    Rev.4, so all 87 CNAE divisions exist in NACE and mean the same thing.
    Below that a shared code is a false friend — CNAE 4781 is retail of
    clothing while NACE 47.81 is retail of food via market stalls, and that one
    code covers 3,687,768 establishments. Going deeper needs a real
    correspondence table, not digits that happen to line up.
    """
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.BRAZIL_COMP_CNAE_DATABASE,
        tables=(tables.BR_CNAE_TO_NACE_TABLE, tables.BR_CNAE_CATEGORIES_TABLE),
    )
    subclasses, payload_hash = _fetch_subclasses()
    categories = parse_cnae_subclasses(subclasses)

    with clickhouse.get_connection() as client:
        divisions = {
            str(code): nace_division_label(str(code), str(description))
            for code, description in client.execute(
                f"SELECT normalized_code, any(description_en) "
                f"FROM `{tables.BRAZIL_COMP_CNAE_DATABASE}`.`nace_categories` "
                f"WHERE level = 'division' GROUP BY normalized_code"
            )
        }
        if not divisions:
            raise ValueError("No NACE divisions available for the Brazil CNAE bridge")

        edges = nace_division_edges(categories, nace_divisions=divisions)
        rows = build_cnae_to_nace_rows(
            edges,
            source_run_id=context.run_id,
            source_payload_hash=payload_hash,
        )
        if not rows:
            raise ValueError("Brazil CNAE to NACE mapping produced no rows")
        _replace_table(
            client,
            tables.BR_CNAE_TO_NACE_TABLE,
            tables.BR_CNAE_TO_NACE_COLUMNS,
            rows,
        )

    unmapped = sum(1 for c in categories if c.level == "subclass") - len(edges)
    context.log.info(
        "Brazil CNAE to NACE: %d edges, %d subclasses without a NACE division",
        len(edges),
        unmapped,
    )
    return dg.MaterializeResult(
        metadata={
            "rows": len(rows),
            "cnae_subclasses": len(edges),
            "nace_divisions": len({e.nace_normalized_code for e in edges}),
            "subclasses_without_a_division": unmapped,
        }
    )


brazil_comp_cnae_refresh_job = dg.define_asset_job(
    name="brazil_comp_cnae_refresh_job",
    selection=dg.AssetSelection.assets(
        brazil_comp_cnae_categories_clickhouse,
        brazil_comp_cnae_to_nace_clickhouse,
    ),
)
