"""Latvia's curated legal forms, written to text_translations.

Its map was re-keyed in June and 118,008 rows kept the old
wording, because the English was stamped in at load time and nothing re-read
it. This is the asset that makes that a one-run fix.

The map itself stays in resources.py where it is reviewed. This only changes
where its output lands: text_translations rather than a column stamped into
corpscout.lv_companies during the load. See defs/common/legal_form_static.py.
"""

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.common.legal_form_static import load_curated_legal_forms
from dagster_v3.defs.latvia_ur.resources import (
    LATVIA_LEGAL_FORM_DESCRIPTION_EN_BY_CODE as CURATED_EN,
)

SOURCE_TABLE = "corpscout.lv_companies"
LABEL_COLUMN = "legal_form_text"
KEY_COLUMN = "legal_form_code"
SOURCE_LANG = "lv"


@dg.asset(
    deps=[dg.AssetKey("latvia_ur_clickhouse_companies")],
    group_name="latvia_ur",
    kinds={"clickhouse"},
    description=(
        "Insert Latvia's hand-curated English legal forms into "
        "text_translations. No translator involved."
    ),
)
def latvia_ur_curated_legal_forms(
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
    context.log.info("Latvia: %d curated legal forms inserted", inserted)
    return dg.MaterializeResult(
        metadata={"inserted": inserted, "curated_terms": len(CURATED_EN)}
    )
