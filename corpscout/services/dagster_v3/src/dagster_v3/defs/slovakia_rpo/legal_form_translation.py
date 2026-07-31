"""Slovakia's curated legal forms, written to text_translations.

Slovakia's register publishes the legal form as a label on the company
row, so the curated map is applied to that label rather than to a code list.

The map itself stays in resources.py where it is reviewed. This only changes
where its output lands: text_translations rather than a column stamped into
corpscout.sk_companies during the load. See defs/common/legal_form_static.py.
"""

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.common.legal_form_static import load_curated_legal_forms
from dagster_v3.defs.slovakia_rpo.resources import (
    SK_LEGAL_FORM_EN_BY_CODE as CURATED_EN,
)

SOURCE_TABLE = "corpscout.sk_companies"
LABEL_COLUMN = "legal_form_original"
KEY_COLUMN = "legal_form_code"
SOURCE_LANG = "sk"


@dg.asset(
    deps=[dg.AssetKey("slovakia_rpo_clickhouse_companies")],
    group_name="slovakia_rpo",
    kinds={"clickhouse"},
    description=(
        "Insert Slovakia's hand-curated English legal forms into "
        "text_translations. No translator involved."
    ),
)
def slovakia_rpo_curated_legal_forms(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    with clickhouse.get_connection() as client:
        inserted = load_curated_legal_forms(
            client,
            table=SOURCE_TABLE,
            label_column=LABEL_COLUMN,
            key_column=KEY_COLUMN,
            source_lang=SOURCE_LANG,
            mapping=CURATED_EN,
        )
    context.log.info("Slovakia: %d curated legal forms inserted", inserted)
    return dg.MaterializeResult(
        metadata={"inserted": inserted, "curated_terms": len(CURATED_EN)}
    )
