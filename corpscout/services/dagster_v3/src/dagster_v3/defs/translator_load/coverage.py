"""Does this source's text actually have translations, and how many are missing.

The check this replaces asked the translator service whether its queue had ever
recorded a failure. That is queue-WIDE and all-time: one poisoned item from any
source, at any past date, failed the check on all eleven translation assets
forever, including assets that had just translated every row they own. It also
could not answer the question anyone actually has, which is "is this column
translated".

So this counts instead. Two numbers per scope -- how many distinct texts the
column has, and how many of them are in text_translations -- reported side by
side whether they agree or not.

**Warning, not failure.** Some gaps are deliberate: company_entity_types holds
209 distinct labels of which 185 are translated, and the 24 remaining are
Finland's, which its register already publishes in English and which are mapped
to nothing on purpose. A check that failed on those would be red forever and
would teach everyone to ignore it. A warning that carries both numbers lets a
reader see 185/209 and know whether that is the expected 24 or a new problem.

The counted population must match what the loader enqueues exactly -- see
build_coverage_sql, which shares its WHERE clauses with build_scan_sql.
"""

from __future__ import annotations

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.translator_load.loader import TranslationField, build_coverage_sql


def translation_coverage_result(
    clickhouse: ClickhouseResource,
    fields: tuple[TranslationField, ...],
    extra_where: str | None = None,
) -> dg.AssetCheckResult:
    """Coverage across one asset's translation scopes, as an AssetCheckResult."""
    per_field: dict[str, str] = {}
    source_texts = 0
    translated = 0

    with clickhouse.get_connection() as client:
        for field in fields:
            rows = client.execute(build_coverage_sql(field.table, field.column, extra_where))
            found, done = (int(rows[0][0]), int(rows[0][1])) if rows else (0, 0)
            source_texts += found
            translated += done
            per_field[f"{field.table}.{field.column}"] = (
                f"{done}/{found} translated, {found - done} missing"
            )

    missing = source_texts - translated
    coverage = round(100 * translated / source_texts, 2) if source_texts else 100.0

    return dg.AssetCheckResult(
        passed=missing == 0,
        # A missing translation degrades a page, it does not break a pipeline,
        # and some are intentional -- so this must not block a materialisation.
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "source_texts": source_texts,
            "translated": translated,
            "untranslated": missing,
            "coverage_pct": coverage,
            **per_field,
        },
    )


# The checks-only job, and its cadence.
#
# Coverage is a property of the DATA, not of the last run: a translator working
# through its queue closes the gap minutes or hours after the load asset
# finished and went green. Re-checking on a timer is what turns "the run
# succeeded" into "the column is actually translated", which is the question
# being asked.
#
# Every ten minutes, and NOT on the (minute, hour) grid the other schedules
# use. That contract exists because the jobs sharing one Postgres and one
# ClickHouse are register refreshes and financial pipelines, and two landing
# together is a real collision. This job runs two counting aggregates over a
# few hundred rows and launches no materialisation -- see
# tests/test_schedule_cron_contracts.py, which exempts check-only schedules by
# name and asserts they stay check-only rather than trusting the name.
TRANSLATION_CHECK_CRON = "*/10 * * * *"
