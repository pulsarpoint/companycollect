"""The se_company_info final asset: change detection, artifact reads, the model
step and the wiring (jobs, sensor, schedule, freshness leaves).

The ClickHouse-facing helpers are asserted as SQL text (this repo has no live
ClickHouse in CI); the resolution loop is exercised end-to-end through
``materialize_se_company_info`` with the scripted fake client from
``test_se_company_common``.
"""

import json
import re
import uuid
from datetime import UTC, datetime
from types import SimpleNamespace

import dagster as dg
import pytest

from dagster_v3.defs.se_company.common import input_hash_for
from dagster_v3.defs.se_company.info import (
    DESCRIPTION_PROMPT_VERSION,
    SELECTION_COLUMNS,
    DescriptionSuggestion,
    LlmProfileConfig,
    build_artifact_rows_sql,
    build_changed_companies_sql,
    build_description_request,
    parse_description_suggestion,
)
from dagster_v3.defs.se_company.info_rules import (
    ArtifactRow,
    evidence_set_hash_for,
    merge_company_info,
)

NOW = datetime(2026, 8, 22, 12, tzinfo=UTC)
COMPANY = "5565200028"
OTHER_COMPANY = "5560125220"
THIRD_COMPANY = "5567890123"
EPOCH_SQL = "toDateTime64('1970-01-01 00:00:00', 3, 'UTC')"
# "Still owed a description", as both branches of the change scan must spell it.
PENDING_SQL = ("ifNull(published.description_source_count, 0) > 1"
               " AND published.suggestion_id IS NULL"
               " AND length(published.correction_ids) = 0")

# assert_clickhouse_tables_exist runs its own SELECT against system.tables first,
# so every scripted answer list starts with the tables it asks about.
EXISTING_TABLES = [
    (table,)
    for table in (
        "se_company_info_scb",
        "se_company_info_esef",
        "se_company_info_wikidata",
        "se_company_info",
        "se_company_info_correction",
        "se_company_info_enrichment_observation",
    )
]


SCB_SWEDISH = "IT-konsulter."
SCB_ENGLISH = "IT consultants."


def _profile(model: str, **overrides) -> LlmProfileConfig:
    return LlmProfileConfig(model=model, **overrides)


# The profile every resolution test runs under: its model/provider names are what the
# fake client, the observation row and the published row are then asserted against.
PROFILE = _profile("fake-model", provider="fake-provider")


def _scb_row(company_id: str, description: str = SCB_SWEDISH) -> tuple:
    return (
        "scb", company_id, f"scb:{company_id}", "a" * 64, NOW,
        json.dumps({
            "legal_name": "Alpha AB", "legal_name_raw": "", "legal_form_code": "AB",
            "status": "active", "incorporation_date": "", "dissolution_date": "",
            "activity_description": description, "activity_description_en": SCB_ENGLISH,
            "primary_sni_code": "62010", "primary_nace_code": "62.01"}),
    )


def _wikidata_row(company_id: str) -> tuple:
    return (
        "wikidata", company_id, "wikidata:Q1", "c" * 64, NOW,
        json.dumps({
            "wikidata_id": "Q1", "wikidata_url": "", "name": "Alpha", "official_name": "",
            "company_description": "Swedish fintech company", "inception_date": "",
            "legal_form_label": "", "industry_wikidata_id": "", "industry_label": "",
            "headquarters_label": "", "employee_count": ""}),
    )


ARTIFACT_ROWS = [_scb_row(COMPANY), _wikidata_row(COMPANY)]


def _selected(company_id: str, **flags: int) -> tuple:
    """One change-scan row: the company id followed by its reason flags, in
    SELECTION_COLUMNS order. Written positionally from that tuple rather than by hand,
    so a new reason column shifts every scripted row at once instead of silently
    misaligning the metadata counts the loop reads by position."""
    unknown = set(flags) - set(SELECTION_COLUMNS[1:])
    assert not unknown, f"not scan columns: {sorted(unknown)}"
    return (company_id, *(int(flags.get(name, 0)) for name in SELECTION_COLUMNS[1:]))


def _outcome():
    scb = ArtifactRow("scb", "scb:1", "a" * 64, NOW, {
        "legal_name": "Alpha AB", "legal_name_raw": None, "legal_form_code": "AB",
        "status": "active", "incorporation_date": None, "dissolution_date": None,
        "activity_description": SCB_SWEDISH, "activity_description_en": SCB_ENGLISH,
        "primary_sni_code": "62010", "primary_nace_code": "62.01"})
    wiki = ArtifactRow("wikidata", "wikidata:Q1", "c" * 64, NOW, {
        "wikidata_id": "Q1", "wikidata_url": "", "name": "Alpha", "official_name": None,
        "company_description": "Swedish fintech company", "inception_date": None,
        "legal_form_label": None, "industry_wikidata_id": None, "industry_label": None,
        "headquarters_label": None, "employee_count": None})
    return merge_company_info(COMPANY, [scb, wiki])


class _FakeCompletions:
    def __init__(self, contents: list[str]) -> None:
        self.contents = list(contents)
        self.requests: list[dict] = []

    def create(self, **request):
        self.requests.append(request)
        content = self.contents.pop(0) if len(self.contents) > 1 else self.contents[0]
        return SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content=content))],
            usage=SimpleNamespace(prompt_tokens=11, completion_tokens=7),
        )


class FakeLlm:
    """Just enough of the OpenAI client for _request_description."""

    def __init__(self, *contents: str) -> None:
        self.completions = _FakeCompletions(list(contents))
        self.chat = SimpleNamespace(completions=self.completions)


GOOD_REPLY = json.dumps({
    "description": "Alpha AB is a Swedish fintech company providing IT consulting.",
    "description_sv": "Alpha AB aer ett svenskt fintechbolag som erbjuder IT-konsulttjaenster.",
    "language": "en", "rationale": "both sources"})


def _final_rows(client) -> list[tuple]:
    """Every row staged for the final table, in insert order."""
    rows = []
    for sql, params in client.executed:
        if re.match(r"^INSERT INTO `corpscout`\.`_tmp_se_company_info_[0-9a-f]{32}`", sql):
            rows.extend(params)
    return rows


def _change_scans(client) -> list[dict]:
    return [params for sql, params in client.executed if sql.startswith("WITH artifacts AS (")]


def test_changed_companies_sql_compares_artifact_versions_and_ledger_with_the_final() -> None:
    sql = build_changed_companies_sql()
    for table in ("se_company_info_scb", "se_company_info_esef", "se_company_info_wikidata"):
        assert f"FROM corpscout.{table}" in sql
    assert "FROM corpscout.se_company_info AS final FINAL" in sql
    # A company is changed when it has never been published, when an artifact carries a
    # newer observation than the published resolution, or when the ledger gained a row
    # after it. Deliberately NOT a published-vs-live correction_ids comparison: a stale
    # or malformed correction is never applied, so that predicate would re-select the
    # same company on every run forever.
    assert "ifNull(published.company_id, '') = ''" in sql
    assert f"artifacts.latest_observed_at > ifNull(published.resolved_at, {EPOCH_SQL})" in sql
    assert f"ifNull(ledger.latest_correction_at, {EPOCH_SQL}) > ifNull(published.resolved_at, {EPOCH_SQL})" in sql
    # resolve_all re-resolves every in-scope company even though no evidence moved --
    # for a rules-only change (new merge logic, a new artifact column) that no
    # observed_at or ledger row reflects. It sits in the ordinary branch only, still
    # paged and still capped by max_companies.
    assert "OR %(resolve_all)s = 1" in sql
    assert f"%(pending_model_only)s = 1 AND {PENDING_SQL}" in sql
    # ... and, on a model-on ordinary run, "published with several sources but no
    # suggestion" is itself a change -- otherwise nothing about such a company ever
    # changes again and only a manual pending_model_only pass would retry it.
    assert f"OR (%(include_pending)s = 1 AND {PENDING_SQL})" in sql
    # One predicate, spelled the same in both WHERE branches AND in the projected
    # reason flag -- all three come from the same Python constant.
    assert sql.count(PENDING_SQL) == 3
    # A company that already carries an applied correction is NOT pending: a reviewer
    # has decided its description and the model would only be overridden again. Keyed
    # on the applied-correction list, never on llm_enhanced -- reject_suggestion leaves
    # that flag down, exactly as a never-modelled row has it. Array columns are never Nullable, so a
    # LEFT JOIN miss is [] under either join_use_nulls setting (and ifNull on a
    # non-Nullable Array is a type error).
    assert "length(published.correction_ids) = 0" in sql
    assert "ifNull(published.correction_ids" not in sql
    assert "final.correction_ids AS correction_ids" in sql  # the CTE has to project it
    assert "arraySort(groupArrayIf(toString(correction_id), NOT superseded))" not in sql
    # Every LEFT JOIN miss must be read through ifNull: bare comparisons are NULL under
    # join_use_nulls = 1, which makes the whole scan return zero rows.
    assert "published.company_id = ''" not in sql.replace("ifNull(published.company_id, '') = ''", "")
    assert "> published.resolved_at" not in sql
    assert "published.description_source_count > 1" not in sql
    assert "ledger.latest_correction_at >" not in sql
    # ClickHouse 26.5: after the LEFT JOINs every company_id reference is qualified.
    assert "AND artifacts.company_id > %(after_company_id)s" in sql
    assert "ORDER BY artifacts.company_id" in sql
    assert "\nORDER BY company_id" not in sql and "\n  AND company_id " not in sql
    # One page per call: the LIMIT is the page size, not the whole run's cap.
    assert "LIMIT %(page_size)s" in sql


def test_the_scan_projects_why_each_company_was_selected() -> None:
    """Every selected row says which reason(s) put it there, so a preview -- and a real
    run's metadata -- can break the selection down without a second scan. The reasons are
    the WHERE's own expressions (a SELECT-list alias is not guaranteed visible to WHERE at
    the same level in ClickHouse), and they overlap: a never-published company also has
    evidence newer than its epoch resolved_at."""
    sql = build_changed_companies_sql()
    assert SELECTION_COLUMNS == (
        "company_id", "never_published", "new_evidence_scb", "new_evidence_esef",
        "new_evidence_wikidata", "ledger_pending", "pending_model", "multi_source")
    # Reason order IS the projection order -- the loop counts by position.
    projected = re.search(r"SELECT artifacts\.company_id AS company_id,\n(.*?)\nFROM artifacts",
                          sql, re.DOTALL)
    assert projected is not None
    assert [line.split(" AS ")[-1].strip() for line in projected.group(1).split(",\n")] == list(
        SELECTION_COLUMNS[1:])
    # Per-source freshness needs the union to carry which artifact each maximum came from.
    for source in ("scb", "esef", "wikidata"):
        assert f"SELECT '{source}' AS source, company_id, max(observed_at) AS source_observed_at" in sql
        # NOT max(latest_observed_at): that is the outer aggregate's alias, and reusing
        # the name makes ClickHouse 26.5 reject the query (ILLEGAL_AGGREGATION).
        assert f"maxIf(source_observed_at, source = '{source}') AS {source}_observed_at" in sql
        assert f"artifacts.{source}_observed_at > ifNull(published.resolved_at, {EPOCH_SQL})" in sql
    # multi_source is the model-cost forecast, read off the LAST published resolution.
    assert "ifNull(published.description_source_count, 0) > 1 AS multi_source" in sql


def test_artifact_rows_sql_unions_the_three_artifacts_with_a_source_column() -> None:
    sql = build_artifact_rows_sql()
    assert sql.count("UNION ALL") == 2
    for source in ("'scb' AS source", "'esef' AS source", "'wikidata' AS source"):
        assert source in sql
    assert "WHERE company_id IN %(company_ids)s" in sql
    assert "toString(evidence_hash) AS evidence_hash" in sql
    assert "toJSONString(map('legal_name'" in sql and "'activity_description'" in sql
    # ClickHouse 26.5 has no common type for a date/number and '': the cast must be
    # inside the ifNull, never around it.
    for column in ("fiscal_year", "description_confidence", "incorporation_date",
                   "dissolution_date", "inception_date", "employee_count"):
        assert f"ifNull(toString({column}), '')" in sql
        assert f"toString(ifNull({column}" not in sql
    assert "* EXCEPT" not in sql and " * " not in sql  # explicit read contract, never star


def test_artifact_reads_are_derived_from_each_artifact_modules_column_list() -> None:
    from dagster_v3.defs.se_company.esef import SE_COMPANY_INFO_ESEF_COLUMNS
    from dagster_v3.defs.se_company.info import ARTIFACT_READS, ARTIFACT_TABLES
    from dagster_v3.defs.se_company.scb import SE_COMPANY_INFO_SCB_COLUMNS
    from dagster_v3.defs.se_company.wikidata import SE_COMPANY_INFO_WIKIDATA_COLUMNS

    envelope = {"company_id", "source_record_uid", "observed_at", "source_run_id"}
    assert ARTIFACT_READS["scb"] == tuple(c for c in SE_COMPANY_INFO_SCB_COLUMNS if c not in envelope)
    assert ARTIFACT_READS["wikidata"] == tuple(c for c in SE_COMPANY_INFO_WIKIDATA_COLUMNS if c not in envelope)
    # ESEF's two JSON blobs are the one payload this module deliberately does not read.
    assert ARTIFACT_READS["esef"] == tuple(
        c for c in SE_COMPANY_INFO_ESEF_COLUMNS if c not in envelope and not c.endswith("_json")
    )
    assert ARTIFACT_TABLES == {
        "scb": "se_company_info_scb",
        "esef": "se_company_info_esef",
        "wikidata": "se_company_info_wikidata",
    }


def test_description_request_is_json_only_and_lists_every_source() -> None:
    outcome = _outcome()
    assert outcome.needs_model
    request = build_description_request(outcome, LlmProfileConfig())
    payload = json.loads(request["messages"][1]["content"])
    assert payload["company_id"] == COMPANY and payload["legal_name"] == "Alpha AB"
    assert [c["source"] for c in payload["sources"]] == ["wikidata", "scb"]
    # SCB's entry carries the register's own Swedish wording beside the English text the
    # translator produced, so the Swedish summary can reuse its phrasing instead of being
    # a re-translation of the model's English one. Only SCB has a Swedish original.
    scb_entry, wikidata_entry = payload["sources"][1], payload["sources"][0]
    assert scb_entry == {"source": "scb", "text": SCB_ENGLISH, "text_sv": SCB_SWEDISH}
    assert "text_sv" not in wikidata_entry
    assert request["response_format"] == {"type": "json_object"} and request["temperature"] == 0
    system = request["messages"][0]["content"]
    assert "untrusted" in system.lower()
    # One call, two languages: the prompt has to ask for both AND bind them to the same
    # facts -- two independently written summaries would publish a company that says
    # different things depending on which column a surface reads.
    assert '"description_sv"' in system
    assert "English" in system and "Swedish" in system
    assert "same facts" in system
    assert "text_sv" in system and "reuse its phrasing" in system


def test_an_untranslated_scb_company_sends_its_swedish_text_once() -> None:
    """text_sv is added only when it says something the entry's own text does not: an
    untranslated company already sends the Swedish original AS the candidate text."""
    scb = ArtifactRow("scb", "scb:1", "a" * 64, NOW, {
        "legal_name": "Alpha AB", "legal_name_raw": None, "legal_form_code": "AB",
        "status": "active", "incorporation_date": None, "dissolution_date": None,
        "activity_description": SCB_SWEDISH, "activity_description_en": "",
        "primary_sni_code": "62010", "primary_nace_code": "62.01"})
    wiki = ArtifactRow("wikidata", "wikidata:Q1", "c" * 64, NOW, {
        "wikidata_id": "Q1", "wikidata_url": "", "name": "Alpha", "official_name": None,
        "company_description": "Swedish fintech company", "inception_date": None,
        "legal_form_label": None, "industry_wikidata_id": None, "industry_label": None,
        "headquarters_label": None, "employee_count": None})
    payload = json.loads(build_description_request(
        merge_company_info(COMPANY, [scb, wiki]), _profile("m"))["messages"][1]["content"])
    assert payload["sources"][1] == {"source": "scb", "text": SCB_SWEDISH}


def test_the_request_is_the_same_before_and_after_the_model_has_answered() -> None:
    """The model-on run hashes the request before calling, and a later model-off run
    recomputes that hash from the same outcome to keep an approval alive. Every field the
    payload reads must therefore be one no model result and no correction rewrites --
    reading `description_sv` (which the model replaces) instead of the never-mutated
    `description_sv_candidate` would make every reviewer decision read stale."""
    from dataclasses import replace

    outcome = _outcome()
    answered = replace(
        outcome, description="Merged text", description_sv="Sammanslagen text",
        llm_enhanced=True, description_language="en", suggestion_id=uuid.uuid4(),
        model_provider="deepseek", model_name="deepseek-v4-flash", prompt_version="x")
    assert build_description_request(answered, _profile("m")) == build_description_request(outcome, _profile("m"))
    assert input_hash_for(build_description_request(answered, _profile("m")), DESCRIPTION_PROMPT_VERSION) == (
        input_hash_for(build_description_request(outcome, _profile("m")), DESCRIPTION_PROMPT_VERSION))


def test_parse_description_suggestion_validates_shape() -> None:
    suggestion = parse_description_suggestion(
        '{"description": "Alpha AB is a Swedish fintech company offering IT consulting.",'
        ' "description_sv": "Alpha AB aer ett svenskt fintechbolag.",'
        ' "language": "en", "rationale": "both"}'
    )
    assert isinstance(suggestion, DescriptionSuggestion) and suggestion.language == "en"
    assert suggestion.description_sv == "Alpha AB aer ett svenskt fintechbolag."
    for bad in (
        '{"description": "", "description_sv": "sv", "language": "en", "rationale": ""}',
        '{"description": "ok", "description_sv": "", "language": "en", "rationale": ""}',
        # Both languages are required: a reply with only the English half would publish a
        # company whose Swedish column silently reverts to whatever SCB happened to say.
        '{"description": "ok", "language": "en", "rationale": ""}',
        '{"description": "ok", "description_sv": "sv", "language": "EN"}',  # two letters, not a code
        '{"description": "ok", "description_sv": "sv", "language": "en", "extra": 1}',  # extra="forbid"
        "no json here",
        None,
    ):
        with pytest.raises(ValueError):
            parse_description_suggestion(bad)
    # v3: the prompt asks for both languages, so every v2 suggestion answers a request
    # this pipeline no longer makes (input_hash covers the prompt version).
    assert DESCRIPTION_PROMPT_VERSION == "se-company-info-description-v3"


def test_initial_load_can_publish_multi_source_companies_without_the_model() -> None:
    """resolve_multi_source_with_llm=False publishes the provisional pick and records the
    contributing sources; the model is never constructed."""
    from dagster_v3.defs.se_company.info import INSERT_COLUMNS, materialize_se_company_info
    from tests.test_se_company_common import FakeClickhouse, FakeClient  # reuse the scripted fake

    client = FakeClient(answers=[
        EXISTING_TABLES,        # assert_clickhouse_tables_exist
        [_selected(COMPANY)],   # changed companies
        ARTIFACT_ROWS,          # artifact rows
        [],                     # ledger
        [],                     # observations
        [(1, 0)],               # final stage validation
        [(0,)],                 # target row count before the insert
        [(1,)],                 # target row count after the insert
    ])
    metadata = materialize_se_company_info(
        clickhouse=FakeClickhouse(client), source_run_id="run", resolved_at=NOW,
        company_ids=[COMPANY], max_companies=1, company_batch_size=1, execute=True,
        llm_client=None, llm_profile=PROFILE, log=None,
        resolve_multi_source_with_llm=False)

    assert metadata["multi_source_count"] == 1 and metadata.get("llm_request_count", 0) == 0
    # The observation table is read (a stored suggestion may exist) but never written.
    assert not any(sql.startswith("INSERT") and "enrichment_observation" in sql
                   for sql, _ in client.executed)
    assert "observation_inserted_count" not in metadata

    # Every position of the published tuple, not just the description ones: a swapped
    # pair of same-typed columns here would otherwise pass an entirely green suite.
    staged = _final_rows(client)
    assert len(staged) == 1
    assert dict(zip(INSERT_COLUMNS, staged[0], strict=True)) == {
        "company_id": COMPANY, "legal_name": "Alpha AB", "legal_form_code": "AB",
        "status": "active", "incorporation_date": None,
        "description": "Swedish fintech company",
        # SCB contributed a candidate, so its Swedish original is published beside the
        # English pick even though the model never ran.
        "description_sv": SCB_SWEDISH, "description_language": "en",
        # Copied from Wikidata, the highest-priority candidate -- the model never ran,
        # so nothing about this row is the model's.
        "llm_enhanced": False, "description_sources": ["wikidata", "scb"],
        "description_source_record_uids": ["wikidata:Q1", f"scb:{COMPANY}"],
        "description_source_count": 2, "primary_nace_code": "62.01", "primary_sni_code": "62010",
        "wikidata_id": "Q1", "lei": None,
        "source_record_uids": [f"scb:{COMPANY}", "wikidata:Q1"],
        "evidence_hashes": ["a" * 64, "c" * 64], "correction_ids": [], "suggestion_id": None,
        "model_provider": "deterministic", "model_name": "se-company-info-rules",
        "prompt_version": "se-company-info-rules-v1", "source_run_id": "run", "resolved_at": NOW,
    }


def test_model_pass_records_the_observation_before_publishing_its_description() -> None:
    from dagster_v3.defs.se_company.info import (
        INSERT_COLUMNS,
        OBSERVATION_COLUMNS,
        materialize_se_company_info,
    )
    from tests.test_se_company_common import FakeClickhouse, FakeClient

    llm = FakeLlm(GOOD_REPLY)
    client = FakeClient(answers=[
        EXISTING_TABLES, [_selected(COMPANY)], ARTIFACT_ROWS, [], [],
        [(1, 0)], [(0,)], [(1,)],   # observation publish
        [(1, 0)], [(0,)], [(1,)],   # final publish
    ])
    metadata = materialize_se_company_info(
        clickhouse=FakeClickhouse(client), source_run_id="run", resolved_at=NOW,
        company_ids=[COMPANY], max_companies=1, company_batch_size=1, execute=True,
        llm_client=llm, llm_profile=PROFILE, log=None)

    assert metadata["llm_request_count"] == 1 and metadata["observation_inserted_count"] == 1
    assert llm.completions.requests[0]["model"] == "fake-model"
    statements = [sql for sql, _ in client.executed]
    observation_stage = next(i for i, sql in enumerate(statements)
                             if sql.startswith("INSERT INTO `corpscout`.`_tmp_se_company_info_enrichment_observation_"))
    observation_target = next(i for i, sql in enumerate(statements)
                              if sql.startswith("INSERT INTO `corpscout`.`se_company_info_enrichment_observation`"))
    final_stage = next(i for i, sql in enumerate(statements)
                       if re.match(r"^INSERT INTO `corpscout`\.`_tmp_se_company_info_[0-9a-f]{32}`", sql))
    assert observation_target < final_stage  # durable before the description it justifies

    observation = dict(zip(OBSERVATION_COLUMNS, client.executed[observation_stage][1][0], strict=True))
    suggestion_id = observation.pop("suggestion_id")
    input_hash = observation.pop("input_hash")
    assert isinstance(suggestion_id, uuid.UUID) and re.fullmatch(r"[0-9a-f]{64}", input_hash)
    assert observation == {
        "company_id": COMPANY,
        "suggestion": json.dumps({
            "description": "Alpha AB is a Swedish fintech company providing IT consulting.",
            "description_sv": "Alpha AB aer ett svenskt fintechbolag som erbjuder IT-konsulttjaenster.",
            "language": "en", "rationale": "both sources"}, ensure_ascii=False),
        "raw_response": GOOD_REPLY, "model_provider": "fake-provider", "model_name": "fake-model",
        "prompt_version": DESCRIPTION_PROMPT_VERSION, "prompt_tokens": 11, "completion_tokens": 7,
        "source_run_id": "run", "created_at": NOW,
    }

    assert dict(zip(INSERT_COLUMNS, _final_rows(client)[0], strict=True)) == {
        "company_id": COMPANY, "legal_name": "Alpha AB", "legal_form_code": "AB",
        "status": "active", "incorporation_date": None,
        "description": "Alpha AB is a Swedish fintech company providing IT consulting.",
        "description_sv": "Alpha AB aer ett svenskt fintechbolag som erbjuder IT-konsulttjaenster.",
        "description_language": "en", "llm_enhanced": True,
        "description_sources": ["wikidata", "scb"],
        "description_source_record_uids": ["wikidata:Q1", f"scb:{COMPANY}"],
        "description_source_count": 2, "primary_nace_code": "62.01", "primary_sni_code": "62010",
        "wikidata_id": "Q1", "lei": None,
        "source_record_uids": [f"scb:{COMPANY}", "wikidata:Q1"],
        "evidence_hashes": ["a" * 64, "c" * 64], "correction_ids": [],
        "suggestion_id": suggestion_id, "model_provider": "fake-provider",
        "model_name": "fake-model", "prompt_version": DESCRIPTION_PROMPT_VERSION,
        "source_run_id": "run", "resolved_at": NOW,
    }


def test_one_unusable_model_reply_only_costs_its_own_company() -> None:
    """A malformed reply must not fail the asset and discard the page's paid calls:
    that company publishes its deterministic pick (and no suggestion_id, so the
    pending_model_only pass re-selects it), while its neighbour's observation is
    still inserted."""
    from dagster_v3.defs.se_company.info import INSERT_COLUMNS, materialize_se_company_info
    from tests.test_se_company_common import FakeClickhouse, FakeClient

    llm = FakeLlm(json.dumps({"description": "x", "language": "en", "rationale": "", "extra": 1}),
                  GOOD_REPLY)
    client = FakeClient(answers=[
        EXISTING_TABLES,
        [_selected(COMPANY), _selected(OTHER_COMPANY)],
        [*ARTIFACT_ROWS, _scb_row(OTHER_COMPANY), _wikidata_row(OTHER_COMPANY)],
        [], [],
        [(1, 0)], [(0,)], [(1,)],   # observation publish -- only the second company's
        [(2, 0)], [(0,)], [(2,)],   # final publish -- both companies
    ])
    logged: list[tuple] = []
    metadata = materialize_se_company_info(
        clickhouse=FakeClickhouse(client), source_run_id="run", resolved_at=NOW,
        company_ids=[], max_companies=2, company_batch_size=2, execute=True,
        llm_client=llm, llm_profile=PROFILE, log=lambda *args: logged.append(args))

    assert metadata["model_failed_count"] == 1
    assert metadata["llm_request_count"] == 1 and metadata["observation_inserted_count"] == 1
    assert metadata["multi_source_count"] == 2
    assert any("model failed" in str(entry[0]) and COMPANY in str(entry) for entry in logged)

    failed, succeeded = (dict(zip(INSERT_COLUMNS, row, strict=True)) for row in _final_rows(client))
    assert failed["company_id"] == COMPANY
    assert failed["description"] == "Swedish fintech company"      # deterministic pick
    assert failed["llm_enhanced"] is False and failed["suggestion_id"] is None
    assert failed["model_provider"] == "deterministic"
    assert succeeded["company_id"] == OTHER_COMPANY
    assert succeeded["llm_enhanced"] is True and succeeded["suggestion_id"] is not None


def test_the_change_scan_is_paged_and_stops_on_a_short_page() -> None:
    """Each scan asks for exactly one page and resumes from the last company id, so the
    scan is never re-run for rows that are then thrown away; a page shorter than the
    limit means there is nothing left to ask for."""
    from dagster_v3.defs.se_company.info import materialize_se_company_info
    from tests.test_se_company_common import FakeClickhouse, FakeClient

    client = FakeClient(answers=[
        EXISTING_TABLES,
        [_selected(OTHER_COMPANY), _selected(COMPANY)],     # page 1: full
        [_scb_row(OTHER_COMPANY), _scb_row(COMPANY)], [], [],
        [(2, 0)], [(0,)], [(2,)],
        [_selected(THIRD_COMPANY)],                         # page 2: short -> stop
        [_scb_row(THIRD_COMPANY)], [], [],
        [(1, 0)], [(2,)], [(3,)],
    ])
    metadata = materialize_se_company_info(
        clickhouse=FakeClickhouse(client), source_run_id="run", resolved_at=NOW,
        company_ids=[], max_companies=5, company_batch_size=2, execute=True,
        llm_client=FakeLlm(GOOD_REPLY), llm_profile=PROFILE, log=None)

    scans = _change_scans(client)
    assert [scan["after_company_id"] for scan in scans] == ["", COMPANY]  # keyset advances
    assert [scan["page_size"] for scan in scans] == [2, 2]
    assert metadata["selected_company_count"] == 3 and metadata["stopped_at_cap"] is False
    assert len(_final_rows(client)) == 3


def test_the_model_pass_refuses_to_run_with_the_model_switched_off() -> None:
    from dagster_v3.defs.se_company.info import materialize_se_company_info
    from tests.test_se_company_common import FakeClickhouse, FakeClient

    with pytest.raises(ValueError, match="pending_model_only"):
        materialize_se_company_info(
            clickhouse=FakeClickhouse(FakeClient(answers=[])), source_run_id="run", resolved_at=NOW,
            company_ids=[], max_companies=1, company_batch_size=1, execute=True,
            llm_client=None, llm_profile=PROFILE, log=None,
            resolve_multi_source_with_llm=False, pending_model_only=True)


def test_a_model_on_run_also_re_selects_companies_still_owed_a_description() -> None:
    """A company published with several description sources and no suggestion (the
    initial load ran with the model off, or its model call failed) is never touched by
    artifacts or the ledger again, so the ordinary model-on run has to treat that state
    as a change -- otherwise only a manual pending_model_only pass would ever retry it."""
    from dagster_v3.defs.se_company.info import materialize_se_company_info
    from tests.test_se_company_common import FakeClickhouse, FakeClient

    def _scan_parameters(**kwargs) -> dict:
        client = FakeClient(answers=[EXISTING_TABLES, []])  # an empty scan: its parameters are the point
        materialize_se_company_info(
            clickhouse=FakeClickhouse(client), source_run_id="run", resolved_at=NOW,
            company_ids=[], max_companies=5, company_batch_size=2, execute=True,
            llm_client=FakeLlm(GOOD_REPLY), llm_profile=PROFILE, log=None, **kwargs)
        return _change_scans(client)[0]

    model_on = _scan_parameters()
    assert model_on["include_pending"] == 1 and model_on["pending_model_only"] == 0
    # resolve_all is off unless asked for: an ordinary run still resolves only what moved.
    assert model_on["resolve_all"] == 0
    assert _scan_parameters(resolve_all=True)["resolve_all"] == 1
    # Model off: there is nothing to retry, so the term must not select anything.
    assert _scan_parameters(resolve_multi_source_with_llm=False)["include_pending"] == 0
    # The dedicated pass selects exactly those companies through its own branch.
    assert _scan_parameters(pending_model_only=True)["include_pending"] == 0


def test_model_calls_are_flushed_in_batches_so_a_crash_cannot_lose_a_whole_page(monkeypatch) -> None:
    from dagster_v3.defs.se_company import info
    from tests.test_se_company_common import FakeClickhouse, FakeClient

    monkeypatch.setattr(info, "OBSERVATION_FLUSH_ROWS", 1)
    client = FakeClient(answers=[
        EXISTING_TABLES, [_selected(COMPANY), _selected(OTHER_COMPANY)],
        [*ARTIFACT_ROWS, _scb_row(OTHER_COMPANY), _wikidata_row(OTHER_COMPANY)], [], [],
        [(1, 0)], [(0,)], [(1,)],   # flush after the first company
        [(1, 0)], [(1,)], [(2,)],   # flush after the second
        [(2, 0)], [(0,)], [(2,)],   # final publish
    ])
    metadata = info.materialize_se_company_info(
        clickhouse=FakeClickhouse(client), source_run_id="run", resolved_at=NOW,
        company_ids=[], max_companies=2, company_batch_size=2, execute=True,
        llm_client=FakeLlm(GOOD_REPLY), llm_profile=PROFILE, log=None)

    assert metadata["observation_inserted_count"] == 2 and metadata["llm_request_count"] == 2
    stages = [i for i, (sql, _) in enumerate(client.executed)
              if sql.startswith("INSERT INTO `corpscout`.`_tmp_se_company_info_enrichment_observation_")]
    final_stage = next(i for i, (sql, _) in enumerate(client.executed)
                       if re.match(r"^INSERT INTO `corpscout`\.`_tmp_se_company_info_[0-9a-f]{32}`", sql))
    assert len(stages) == 2 and max(stages) < final_stage  # two flushes, both before the final
    assert [len(client.executed[i][1]) for i in stages] == [1, 1]


def test_the_cap_rather_than_exhaustion_is_reported_when_a_full_page_uses_it_up() -> None:
    """stopped_at_cap is only reachable after a FULL page (a short page ends the loop
    at the bottom), so it always means "more changed companies may remain"."""
    from dagster_v3.defs.se_company.info import materialize_se_company_info
    from tests.test_se_company_common import FakeClickhouse, FakeClient

    client = FakeClient(answers=[
        EXISTING_TABLES, [_selected(COMPANY)], ARTIFACT_ROWS, [], [], [(1, 0)], [(0,)], [(1,)]])
    logged: list[tuple] = []
    metadata = materialize_se_company_info(
        clickhouse=FakeClickhouse(client), source_run_id="run", resolved_at=NOW,
        company_ids=[], max_companies=1, company_batch_size=1, execute=True,
        llm_client=None, llm_profile=PROFILE,
        log=lambda *args: logged.append(args), resolve_multi_source_with_llm=False)

    assert metadata["stopped_at_cap"] is True
    assert len(_change_scans(client)) == 1  # the cap stops the loop before asking again
    assert any("max_companies cap" in str(entry[0]) for entry in logged)


def test_an_explicit_scope_is_chunked_so_the_rendered_query_stays_under_max_query_size() -> None:
    """Both scan queries embed the id list three times, substituted client-side, so a
    5,000-id scope already renders to ~212 KB of the 262,144-byte default max_query_size.
    A scoped run (a correction sensor can name thousands of companies) is split into
    chunks of company_batch_size, each paged on its own."""
    from dagster_v3.defs.se_company.info import materialize_se_company_info
    from tests.test_se_company_common import FakeClickhouse, FakeClient

    scope = [f"55600000{index:02d}" for index in range(7)]
    client = FakeClient(answers=[EXISTING_TABLES, [], [], []])  # one empty scan per chunk
    materialize_se_company_info(
        clickhouse=FakeClickhouse(client), source_run_id="run", resolved_at=NOW,
        company_ids=scope, max_companies=1_000, company_batch_size=3, execute=True,
        llm_client=FakeLlm(GOOD_REPLY), llm_profile=PROFILE, log=None)

    scans = _change_scans(client)
    assert [len(scan["company_ids"]) for scan in scans] == [3, 3, 1]
    assert sorted(sum((list(scan["company_ids"]) for scan in scans), [])) == sorted(scope)
    assert all(scan["all_companies"] == 0 for scan in scans)
    assert all(scan["after_company_id"] == "" for scan in scans)  # each chunk pages from its start


def test_the_config_caps_the_batch_at_the_query_size_limit() -> None:
    from dagster_v3.defs.se_company.info import SECompanyInfoConfig

    assert SECompanyInfoConfig().company_batch_size == 5_000
    with pytest.raises(ValueError):
        SECompanyInfoConfig(company_batch_size=5_001)
    assert SECompanyInfoConfig().resolve_all is False
    assert SECompanyInfoConfig(resolve_all=True).resolve_all is True


def test_an_approval_survives_a_run_that_does_not_call_the_model() -> None:
    """The ledger validates approve/reject against the hash of the request that produced
    the suggestion, and that hash includes the model name. A model-off run therefore has
    to recompute it from the newest stored observation -- otherwise every reviewer
    decision reads stale and the approved text is dropped on the next publish."""
    from dagster_v3.defs.se_company.info import INSERT_COLUMNS, materialize_se_company_info
    from tests.test_se_company_common import FakeClickhouse, FakeClient

    suggestion_id, correction_id = uuid.uuid4(), uuid.uuid4()
    suggestion = {"description": "Alpha AB builds payment software in Sweden.",
                  "description_sv": "Alpha AB bygger betalprogramvara i Sverige.",
                  "language": "en", "rationale": "merged"}
    # The hash the model-off run must arrive at: same request, the STORED model name.
    stored_hash = input_hash_for(
        build_description_request(_outcome(), _profile("stored-model")), DESCRIPTION_PROMPT_VERSION)

    client = FakeClient(answers=[
        EXISTING_TABLES, [_selected(COMPANY)], ARTIFACT_ROWS,
        [(correction_id, COMPANY, "approve_suggestion", json.dumps({"suggestion_id": str(suggestion_id)}),
          evidence_set_hash_for(("a" * 64, "c" * 64)), None, NOW)],
        [(suggestion_id, COMPANY, stored_hash, json.dumps(suggestion),
          "fake-provider", "stored-model", DESCRIPTION_PROMPT_VERSION, NOW)],
        [(1, 0)], [(0,)], [(1,)],
    ])
    metadata = materialize_se_company_info(
        clickhouse=FakeClickhouse(client), source_run_id="run", resolved_at=NOW,
        company_ids=[COMPANY], max_companies=1, company_batch_size=1, execute=True,
        llm_client=None, llm_profile=PROFILE, log=None,
        resolve_multi_source_with_llm=False)

    assert metadata["stale_correction_count"] == 0 and metadata["applied_correction_count"] == 1
    assert metadata.get("llm_request_count", 0) == 0  # no model call was made to get there
    row = dict(zip(INSERT_COLUMNS, _final_rows(client)[0], strict=True))
    assert row["description"] == "Alpha AB builds payment software in Sweden."
    assert row["description_sv"] == "Alpha AB bygger betalprogramvara i Sverige."
    # Approved, but the text is still the model's -- and the reviewer's involvement is
    # what correction_ids below records.
    assert row["llm_enhanced"] is True and row["description_language"] == "en"
    assert row["suggestion_id"] == suggestion_id and row["correction_ids"] == [correction_id]
    assert row["model_provider"] == "fake-provider" and row["model_name"] == "stored-model"


def test_insert_columns_match_the_migration_in_order() -> None:
    from dagster_v3.defs.se_company.info import INSERT_COLUMNS
    from tests.se_company_ddl import declared_columns

    assert list(INSERT_COLUMNS) == [
        c for c in declared_columns("se_company_info") if c != "evidence_set_hash"
    ]


def test_definitions_wire_final_jobs_sensor_schedule_and_leaves() -> None:
    from dagster_v3.definitions import defs as load_defs
    from dagster_v3.defs.common.clickhouse_checks import CLICKHOUSE_LEAVES

    repository = load_defs().get_repository_def()
    final = repository.asset_graph.get(dg.AssetKey("se_company_info_clickhouse"))
    assert final.parent_keys == {
        dg.AssetKey("se_company_info_scb_clickhouse"),
        dg.AssetKey("se_company_info_esef_clickhouse"),
        dg.AssetKey("se_company_info_wikidata_clickhouse"),
    }
    assert final.group_name == "se_company"
    keys = {k.path[-1] for k in repository.get_job("se_company_info_job").asset_layer.executable_asset_keys}
    assert keys == {"se_company_info_scb_clickhouse", "se_company_info_esef_clickhouse",
                    "se_company_info_wikidata_clickhouse", "se_company_info_clickhouse"}
    assert {k.path[-1] for k in repository.get_job("se_company_info_review_job").asset_layer.executable_asset_keys} == {
        "se_company_info_clickhouse"}
    sensor = repository.get_sensor_def("se_company_info_correction_sensor")
    assert sensor.job_name == "se_company_info_review_job"
    assert sensor.default_status == dg.DefaultSensorStatus.STOPPED
    schedule = repository.get_schedule_def("se_company_info_weekly")
    # 06:45 Monday would collide with the existing "45 6 * * 6" slot the cron contract guards.
    assert schedule.cron_schedule == "50 6 * * 1"
    assert schedule.default_status == dg.DefaultScheduleStatus.STOPPED
    leaves = {leaf.asset_key: leaf for leaf in CLICKHOUSE_LEAVES}
    assert leaves["se_company_info_clickhouse"].tables == ("se_company_info",)
    assert leaves["se_company_info_scb_clickhouse"].tables == ("se_company_info_scb",)
    assert leaves["se_company_info_esef_clickhouse"].tables == ("se_company_info_esef",)
    assert leaves["se_company_info_wikidata_clickhouse"].tables == ("se_company_info_wikidata",)
    # se_company_info_weekly is RUNNING (phase 7): a missed week must show as stale.
    from dagster_v3.defs.common.clickhouse_checks import WEEKLY

    assert all(leaves[key].max_age == WEEKLY for key in (
        "se_company_info_clickhouse", "se_company_info_scb_clickhouse",
        "se_company_info_esef_clickhouse", "se_company_info_wikidata_clickhouse"))


def test_a_truncated_model_response_is_reported_as_truncation_not_as_bad_json() -> None:
    """deepseek-v4-flash spends completion tokens on reasoning_content; when max_tokens runs out the
    JSON has no closing brace. The failure must name the cause so the operator raises the budget."""
    from dagster_v3.defs.se_company.info import _request_description

    class _Truncating:
        def __init__(self) -> None:
            self.chat = SimpleNamespace(completions=self)

        def create(self, **request):  # noqa: ANN003
            return SimpleNamespace(
                choices=[SimpleNamespace(finish_reason="length",
                                         message=SimpleNamespace(content='{"description": "cut off'))],
                usage=SimpleNamespace(prompt_tokens=300, completion_tokens=800))

    with pytest.raises(ValueError, match="truncated .*finish_reason=length.*completion_tokens=800"):
        _request_description(_Truncating(), {"model": "m", "messages": []}, provider="p",
                             prompt_version=DESCRIPTION_PROMPT_VERSION)
    # Two summaries plus reasoning_content: 4000 was sized for one.
    assert build_description_request(_outcome(), _profile("m"))["max_tokens"] >= 6000


def test_a_run_without_execute_writes_nothing_calls_nothing_and_says_what_it_would_do() -> None:
    """The gate the 2026-08-23 incident bought: a "Materialize" click in the Dagster UI
    sends no config at all, so `execute` defaults to False and the run must be a report.
    It still pages the change scan exactly as a real run does -- but every statement it
    issues is a read, the model client it was handed is never touched, and no artifact
    row, ledger row or stored observation is even fetched."""
    from dagster_v3.defs.se_company.info import materialize_se_company_info
    from tests.test_se_company_common import FakeClickhouse, FakeClient

    llm = FakeLlm(GOOD_REPLY)
    client = FakeClient(answers=[
        EXISTING_TABLES,
        [_selected(COMPANY, never_published=1, new_evidence_scb=1, new_evidence_wikidata=1),
         _selected(OTHER_COMPANY, new_evidence_esef=1, ledger_pending=1, pending_model=1,
                   multi_source=1)],
        [],                     # page 2: exhausted
        [(12, 640.0, 240.0)],   # observed token cost of this model
    ])
    metadata = materialize_se_company_info(
        clickhouse=FakeClickhouse(client), source_run_id="run", resolved_at=NOW,
        company_ids=[], max_companies=5, company_batch_size=2, execute=False,
        llm_client=llm, llm_profile=PROFILE, log=None)

    assert all(sql.lstrip().upper().startswith(("SELECT", "WITH")) for sql, _ in client.executed)
    assert llm.completions.requests == []
    assert not any(sql.startswith(("INSERT", "CREATE", "DROP", "ALTER")) for sql, _ in client.executed)
    # The scan and only the scan: no artifact/ledger/observation reads for the page.
    assert len(_change_scans(client)) == 2
    assert not any("UNION ALL" in sql and "payload_json" in sql for sql, _ in client.executed)

    assert metadata["preview"] is True
    assert metadata["selected_company_count"] == 2
    assert {reason: metadata[reason] for reason in (
        "never_published", "new_evidence_scb", "new_evidence_esef", "new_evidence_wikidata",
        "ledger_pending", "pending_model")} == {
        "never_published": 1, "new_evidence_scb": 1, "new_evidence_esef": 1,
        "new_evidence_wikidata": 1, "ledger_pending": 1, "pending_model": 1}
    # One of the two was published with several description sources, so only that one
    # would enter the model step; the estimate is that count times the observed averages.
    assert metadata["would_call_model"] == 1
    assert metadata["prompt_tokens_per_call"] == 640 and metadata["completion_tokens_per_call"] == 240
    assert metadata["estimated_prompt_tokens"] == 640
    assert metadata["estimated_completion_tokens"] == 240
    assert metadata["llm_model"] == "fake-model" and metadata["llm_provider"] == "fake-provider"


def test_a_preview_with_the_model_off_forecasts_no_calls_at_all() -> None:
    from dagster_v3.defs.se_company.info import (
        FALLBACK_COMPLETION_TOKENS,
        FALLBACK_PROMPT_TOKENS,
        materialize_se_company_info,
    )
    from tests.test_se_company_common import FakeClickhouse, FakeClient

    def _preview(**kwargs) -> dict:
        client = FakeClient(answers=[
            EXISTING_TABLES,
            [_selected(COMPANY, multi_source=1)],
            [(0, None, None)],  # this model has never been called
        ])
        return materialize_se_company_info(
            clickhouse=FakeClickhouse(client), source_run_id="run", resolved_at=NOW,
            company_ids=[], max_companies=1, company_batch_size=1, execute=False,
            llm_client=None, llm_profile=PROFILE, log=None, **kwargs)

    # An unseen model has no observed average, so the forecast falls back to a flat
    # per-call figure rather than reporting zero cost.
    model_on = _preview()
    assert model_on["would_call_model"] == 1
    assert model_on["estimated_prompt_tokens"] == FALLBACK_PROMPT_TOKENS
    assert model_on["estimated_completion_tokens"] == FALLBACK_COMPLETION_TOKENS
    model_off = _preview(resolve_multi_source_with_llm=False)
    assert model_off["would_call_model"] == 0
    assert model_off["estimated_prompt_tokens"] == 0 and model_off["estimated_completion_tokens"] == 0


def test_an_execute_run_that_may_call_the_model_refuses_to_start_without_a_client() -> None:
    """The client is built by the asset, from the run's profile and the host's key, before
    anything is read or written -- so materialize refuses a model-on execute run that has
    none rather than discovering it half-way through a page."""
    from dagster_v3.defs.se_company.info import materialize_se_company_info
    from tests.test_se_company_common import FakeClickhouse, FakeClient

    client = FakeClient(answers=[])
    with pytest.raises(ValueError, match="LLM client"):
        materialize_se_company_info(
            clickhouse=FakeClickhouse(client), source_run_id="run", resolved_at=NOW,
            company_ids=[], max_companies=1, company_batch_size=1, execute=True,
            llm_client=None, llm_profile=PROFILE, log=None)
    assert client.executed == []  # not even the table-existence check ran


def test_the_api_key_is_read_from_the_host_by_provider_name(monkeypatch) -> None:
    """The one thing the run config never carries. A provider whose key this host does
    not have fails with the variable's name, before any write and before any call."""
    from dagster_v3.defs.se_company.info import build_llm_client, llm_api_key_variable

    assert llm_api_key_variable("deepseek") == "DEEPSEEK_API_KEY"
    monkeypatch.setenv("DEEPSEEK_API_KEY", "   ")
    with pytest.raises(ValueError, match="DEEPSEEK_API_KEY"):
        build_llm_client(_profile("deepseek-v4-flash"), timeout_seconds=10)
    monkeypatch.setenv("DEEPSEEK_API_KEY", "sk-test")
    built = build_llm_client(_profile("deepseek-v4-flash", base_url="https://api.deepseek.com/"),
                             timeout_seconds=30)
    assert str(built.base_url).rstrip("/") == "https://api.deepseek.com"
    monkeypatch.delenv("OTHER_API_KEY", raising=False)
    with pytest.raises(ValueError, match="OTHER_API_KEY"):
        build_llm_client(_profile("m", provider="other"), timeout_seconds=10)


def test_the_run_config_profile_reaches_the_request_and_the_stored_observation() -> None:
    """provider/model/temperature/max_tokens/prompt_version all come from the run's own
    profile: the request carries them and the observation row records what answered, so a
    later run can tell which model wrote a description."""
    from dagster_v3.defs.se_company.info import OBSERVATION_COLUMNS, materialize_se_company_info
    from tests.test_se_company_common import FakeClickhouse, FakeClient

    profile = _profile("other-model", provider="other-provider", temperature=0.4,
                       max_tokens=1_234, prompt_version="se-company-info-description-v9")
    llm = FakeLlm(GOOD_REPLY)
    client = FakeClient(answers=[
        EXISTING_TABLES, [_selected(COMPANY)], ARTIFACT_ROWS, [], [],
        [(1, 0)], [(0,)], [(1,)],
        [(1, 0)], [(0,)], [(1,)],
    ])
    materialize_se_company_info(
        clickhouse=FakeClickhouse(client), source_run_id="run", resolved_at=NOW,
        company_ids=[COMPANY], max_companies=1, company_batch_size=1, execute=True,
        llm_client=llm, llm_profile=profile, log=None)

    request = llm.completions.requests[0]
    assert request["model"] == "other-model"
    assert request["temperature"] == 0.4 and request["max_tokens"] == 1_234
    staged = next(params for sql, params in client.executed
                  if sql.startswith("INSERT INTO `corpscout`.`_tmp_se_company_info_enrichment_observation_"))
    observation = dict(zip(OBSERVATION_COLUMNS, staged[0], strict=True))
    assert observation["model_provider"] == "other-provider"
    assert observation["model_name"] == "other-model"
    assert observation["prompt_version"] == "se-company-info-description-v9"


def test_the_model_step_keeps_at_most_concurrency_calls_in_flight() -> None:
    """concurrency > 1 issues that many description calls at once and no more. The fake
    refuses to answer until exactly two calls are in flight, so a sequential model step
    would deadlock the barrier and fail this test rather than quietly pass it."""
    import threading

    from dagster_v3.defs.se_company.info import INSERT_COLUMNS, materialize_se_company_info
    from tests.test_se_company_common import FakeClickhouse, FakeClient

    companies = [COMPANY, OTHER_COMPANY, THIRD_COMPANY, "5569999999"]

    class _ConcurrentLlm:
        def __init__(self, parallel: int) -> None:
            self.barrier = threading.Barrier(parallel, timeout=10)
            self.lock = threading.Lock()
            self.in_flight = 0
            self.max_in_flight = 0
            self.requests: list[dict] = []
            self.chat = SimpleNamespace(completions=self)

        def create(self, **request):
            with self.lock:
                self.in_flight += 1
                self.max_in_flight = max(self.max_in_flight, self.in_flight)
                self.requests.append(request)
            self.barrier.wait()
            with self.lock:
                self.in_flight -= 1
            return SimpleNamespace(
                choices=[SimpleNamespace(message=SimpleNamespace(content=GOOD_REPLY))],
                usage=SimpleNamespace(prompt_tokens=11, completion_tokens=7))

    llm = _ConcurrentLlm(parallel=2)
    client = FakeClient(answers=[
        EXISTING_TABLES,
        [_selected(company) for company in companies],
        [row for company in companies for row in (_scb_row(company), _wikidata_row(company))],
        [], [],
        [(4, 0)], [(0,)], [(4,)],   # one observation flush, all four calls
        [(4, 0)], [(0,)], [(4,)],   # final publish
    ])
    metadata = materialize_se_company_info(
        clickhouse=FakeClickhouse(client), source_run_id="run", resolved_at=NOW,
        company_ids=[], max_companies=4, company_batch_size=4, execute=True,
        llm_client=llm, llm_profile=_profile("fake-model", provider="fake-provider", concurrency=2),
        log=None)

    assert llm.max_in_flight == 2
    assert metadata["llm_request_count"] == 4 and metadata["observation_inserted_count"] == 4
    # Results are consumed in company order however they finished, so the published rows
    # and the observation rows stay in scan order at any concurrency.
    assert [dict(zip(INSERT_COLUMNS, row, strict=True))["company_id"] for row in _final_rows(client)] == companies
    staged = next(params for sql, params in client.executed
                  if sql.startswith("INSERT INTO `corpscout`.`_tmp_se_company_info_enrichment_observation_"))
    assert [row[1] for row in staged] == companies


def test_the_config_gates_the_run_and_pins_the_profile_the_automation_sends() -> None:
    from dagster_v3.defs.se_company.info import (
        DEFAULT_LLM_PROFILE,
        SECompanyInfoConfig,
        se_company_info_weekly,
    )

    # Preview by default: an empty config (a UI "Materialize" click) resolves nothing.
    assert SECompanyInfoConfig().execute is False and SECompanyInfoConfig().llm is None
    assert SECompanyInfoConfig(execute=True).execute is True
    # The pinned profile IS the field defaults -- one set of values, asserted equal
    # rather than kept in step by hand.
    assert DEFAULT_LLM_PROFILE == LlmProfileConfig().model_dump()
    assert DEFAULT_LLM_PROFILE["prompt_version"] == DESCRIPTION_PROMPT_VERSION
    assert LlmProfileConfig().concurrency == 1
    for bad in (0, 9):
        with pytest.raises(ValueError):
            LlmProfileConfig(concurrency=bad)
    # The automated triggers must both spell out execute AND the profile: a
    # sensor/schedule run carries only the config its definition writes. Read off an
    # evaluated tick, which is the config the daemon would actually submit.
    context = dg.build_schedule_context(
        scheduled_execution_time=datetime(2026, 8, 24, 6, 50, tzinfo=UTC))
    run_requests = se_company_info_weekly.evaluate_tick(context).run_requests
    assert run_requests is not None and run_requests[0].run_config == {
        "ops": {"se_company_info_clickhouse": {"config": {"execute": True, "llm": DEFAULT_LLM_PROFILE}}}}


def test_the_correction_sensor_launches_a_real_run_not_a_preview(monkeypatch) -> None:
    """A ledger row must actually re-resolve its company. Without execute in the sensor's
    run config the review job would run the scan and write nothing -- the reviewer's
    correction would sit unapplied forever, and nothing would look broken."""
    from contextlib import contextmanager

    import dagster as dg
    from dagster_clickhouse import ClickhouseResource

    from dagster_v3.defs.se_company.info import (
        DEFAULT_LLM_PROFILE,
        se_company_info_correction_sensor,
    )
    from tests.test_se_company_common import _FakeLedgerClient

    ledger = _FakeLedgerClient()
    ledger.append(COMPANY, str(uuid.UUID(int=7)), "2026-08-22 09:00:00.000")
    resource = ClickhouseResource(host="localhost")

    @contextmanager
    def fake_get_connection(self):
        yield ledger

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)
    context = dg.build_sensor_context(cursor=None, resources={"clickhouse": resource})
    execution_data = se_company_info_correction_sensor.evaluate_tick(context)

    assert execution_data.run_requests is not None
    assert execution_data.run_requests[0].run_config == {
        "ops": {"se_company_info_clickhouse": {"config": {
            "execute": True, "llm": DEFAULT_LLM_PROFILE, "company_ids": [COMPANY]}}}}
