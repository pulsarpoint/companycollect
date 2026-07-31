"""Estonia's curated legal forms, written to text_translations.

Estonia's register publishes no legal-form code, only the Estonian term,
so the label is its own lookup key -- key_column and label_column are the
same column. A register that renames a form therefore drops to untranslated
rather than silently mislabelled, which is the safe direction.

The map itself stays in resources.py where it is reviewed. This only changes
where its output lands: text_translations rather than a column stamped into
corpscout.ee_companies during the load. See defs/common/legal_form_static.py.
"""

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.common.legal_form_static import load_curated_legal_forms
from dagster_v3.defs.estonia_ar.resources import (
    EE_LEGAL_FORM_EN_BY_NAME as CURATED_EN,
)

SOURCE_TABLE = "corpscout.ee_companies"
LABEL_COLUMN = "legal_form_original"
KEY_COLUMN = "legal_form_original"
SOURCE_LANG = "et"


@dg.asset(
    deps=[dg.AssetKey("estonia_ar_clickhouse_companies")],
    group_name="estonia_ar",
    kinds={"clickhouse"},
    description=(
        "Insert Estonia's hand-curated English legal forms into "
        "text_translations. No translator involved."
    ),
)
def estonia_ar_curated_legal_forms(
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
    context.log.info("Estonia: %d curated legal forms inserted", inserted)
    return dg.MaterializeResult(
        metadata={"inserted": inserted, "curated_terms": len(CURATED_EN)}
    )
