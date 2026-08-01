"""Translation coverage check: counts, severity, and the loader's own filters."""

from contextlib import contextmanager

import dagster as dg

from dagster_v3.defs.translator_load.coverage import translation_coverage_result
from dagster_v3.defs.translator_load.loader import (
    TranslationField,
    build_coverage_sql,
    build_scan_sql,
)


def value(metadata, key):
    """Dagster wraps metadata in MetadataValue objects."""
    entry = metadata[key]
    return entry.value if hasattr(entry, "value") else entry


class FakeClickhouse:
    """Returns a canned (source_texts, translated) pair per query."""

    def __init__(self, *results: tuple[int, int]) -> None:
        self._results = list(results)
        self.queries: list[str] = []

    def execute(self, sql: str):
        self.queries.append(sql)
        return [self._results.pop(0)]

    @contextmanager
    def get_connection(self):
        yield self


def test_reports_both_numbers_and_the_shortfall() -> None:
    result = translation_coverage_result(
        FakeClickhouse((296, 49)),
        (TranslationField("corpscout.fr_legal_forms", "label_fr"),),
    )
    assert value(result.metadata, "source_texts") == 296
    assert value(result.metadata, "translated") == 49
    assert value(result.metadata, "untranslated") == 247
    assert value(result.metadata, "coverage_pct") == 16.55


def test_a_shortfall_warns_rather_than_fails() -> None:
    """company_entity_types is 185 of 209 translated and always will be -- the
    24 remaining are Finland's, already English in its register and mapped to
    nothing on purpose. A check that failed there would be red forever, and a
    check nobody believes is worse than no check."""
    result = translation_coverage_result(
        FakeClickhouse((209, 185)),
        (TranslationField("corpscout.company_entity_types", "source_label"),),
    )
    assert result.passed is False
    assert result.severity == dg.AssetCheckSeverity.WARN


def test_full_coverage_passes() -> None:
    result = translation_coverage_result(
        FakeClickhouse((151, 151)),
        (TranslationField("corpscout.cz_legal_forms", "label_cs"),),
    )
    assert result.passed is True
    assert value(result.metadata, "untranslated") == 0
    assert value(result.metadata, "coverage_pct") == 100.0


def test_a_source_with_no_text_is_complete_not_divided_by_zero() -> None:
    result = translation_coverage_result(
        FakeClickhouse((0, 0)), (TranslationField("corpscout.empty", "col"),)
    )
    assert result.passed is True
    assert value(result.metadata, "coverage_pct") == 100.0


def test_several_scopes_are_summed_and_also_reported_separately() -> None:
    """Latvia declares more than one translated column, and a single total
    would hide which of them is behind."""
    result = translation_coverage_result(
        FakeClickhouse((100, 90), (10, 1)),
        (
            TranslationField("corpscout.lv_companies", "activity_text_original"),
            TranslationField("corpscout.lv_companies", "other_text"),
        ),
    )
    assert value(result.metadata, "source_texts") == 110
    assert value(result.metadata, "translated") == 91
    assert value(result.metadata, "untranslated") == 19
    assert "90/100 translated, 10 missing" in str(
        value(result.metadata, "corpscout.lv_companies.activity_text_original")
    )
    assert "1/10 translated, 9 missing" in str(
        value(result.metadata, "corpscout.lv_companies.other_text")
    )


def test_counts_exactly_the_population_the_loader_enqueues() -> None:
    """The two must share their WHERE clauses. A check counting texts the
    loader skips reports a shortfall no run can ever close -- and the loader
    skips whitespace-only and >8,000-char texts for reasons that cost this
    project two frozen queues."""
    coverage = build_coverage_sql("corpscout.t", "c")
    scan = build_scan_sql("corpscout.t", "c")
    for clause in ("trim(BOTH ' \\t\\r\\n' FROM c.c) != ''", "length(c.c) <= 8000"):
        assert clause in coverage, clause
        assert clause in scan, clause


def test_extra_where_scopes_both_the_count_and_the_scan() -> None:
    coverage = build_coverage_sql("corpscout.t", "c", "country_code = 'SE'")
    assert "country_code = 'SE'" in coverage


def test_translation_coverage_job_covers_every_loader() -> None:
    """The ten-minute job names its assets as strings, so nothing tells it when
    a new translation loader appears. This does: a loader that grows a
    translations_present check and is not added to the list fails here rather
    than silently going unchecked."""
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    declared = {
        key
        for key in repo.asset_graph.asset_check_keys
        if key.name == "translations_present"
    }
    job = repo.get_job("translation_coverage_job")
    selected = {
        key for key in job.asset_layer.asset_graph.asset_check_keys
        if key.name == "translations_present"
    }
    missing = sorted(k.to_user_string() for k in declared - selected)
    assert missing == [], (
        f"these loaders have a coverage check that the ten-minute job does not "
        f"run: {missing} -- add them to TRANSLATION_LOAD_ASSETS"
    )
