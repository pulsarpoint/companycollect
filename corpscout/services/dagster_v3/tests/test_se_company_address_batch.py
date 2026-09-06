"""The batch around the pure fold: selection, paging, in-page geocoding, history before
main. A fake client records every statement; geocode_addresses is stubbed."""

from datetime import UTC, datetime
from typing import Any

import pytest

from dagster_v3.defs.se_company.address import batch, tables
from dagster_v3.defs.se_company.address.fold import FOLD_VERSION
from dagster_v3.defs.se_company.address.geocode import GeocodeOutcome
from dagster_v3.defs.se_company.address.normalize_se import NORMALIZER_VERSION, NormalizedAddress, address_key
from dagster_v3.defs.sweden_company.address_resolution_policy import SWEDEN_ADDRESS_RESOLUTION_POLICY

POLICY = SWEDEN_ADDRESS_RESOLUTION_POLICY.version
REFERENCE = "ref-1"
T0 = datetime(2026, 8, 1, 8, 0, tzinfo=UTC)
T1 = datetime(2026, 9, 1, 8, 0, tzinfo=UTC)
T2 = datetime(2026, 9, 2, 8, 0, tzinfo=UTC)
FOLDED_AT = datetime(2026, 9, 7, 9, 0, tzinfo=UTC)
A, B, F = "5560000001", "5560000002", "5560000003"


def normalized_row(company_id: str, source: str, slot: str = "postal", *, street_name="storgatan", house_number="5",
                   postal_code="11122", city="stockholm", care_of=None, box=None, unit=None, country_code="SE",
                   parse_status="ok", text="Storgatan 5, 111 22 Stockholm", suggested_at=T1, kind="postal") -> tuple:
    key = address_key(NormalizedAddress(care_of, box, street_name, house_number, unit, postal_code, city, country_code, text, parse_status, ""))
    return (company_id, source, slot, f"{source}-{slot}".ljust(64, "0"), kind, care_of, box, street_name, house_number, unit,
            postal_code, city, country_code, text, key, parse_status, NORMALIZER_VERSION, suggested_at)


def main_row(company_id: str, key: str, **overrides) -> tuple:
    values = {
        "company_id": company_id, "address_key": key, "care_of": None, "box": None, "street_name": "storgatan",
        "house_number": "5", "unit": None, "postal_code": "11122", "city": "stockholm", "country_code": "SE",
        "normalized_address": "Storgatan 5, 111 22 Stockholm", "kinds": ["postal"], "sources": ["scb"], "slots": ["postal"],
        "normalized_ids": ["scb-postal".ljust(64, "0")], "text_source": "scb", "active": 1, "inactive_reason": "",
        "latitude": 59.3, "longitude": 18.1, "geocode_status": "matched_exact", "geocode_method": "raw_full_exact",
        "geocode_confidence": 0.98, "geocode_precision": "address", "geocode_policy": POLICY, "geocode_reference": REFERENCE,
        "geocoded_at": T0, "normalizer_version": NORMALIZER_VERSION,
    }
    values.update(overrides)
    return tuple(values[c] for c in batch.MAIN_COMPARE_COLUMNS)


class FakeClient:
    """Answers each SELECT from the dict keyed by the SQL-text function name; records
    every call with its settings."""

    def __init__(self, answers: dict[str, list]) -> None:
        self.answers = answers
        self.calls: list[tuple[str, Any, Any]] = []
        self.inserts: list[tuple[str, list]] = []

    def execute(self, sql, params=None, settings=None):
        self.calls.append((sql, params, settings))
        if sql.startswith("INSERT"):
            self.inserts.append((sql, list(params)))
            return []
        for name, rows in self.answers.items():
            if sql == getattr(batch, name)():
                return rows
        raise AssertionError(f"unexpected SQL: {sql[:80]}")


def stub_geocode(monkeypatch, outcomes_by_key: dict[str, GeocodeOutcome] | None = None, *, calls: list | None = None):
    def fake(addresses, *, clickhouse, duckdb, run_id, matched_at, log=None):
        if calls is not None:
            calls.append(dict(addresses))
        result = {}
        for key, address in addresses.items():
            assert address.parse_status not in ("foreign", "no_address")
            result[key] = (outcomes_by_key or {}).get(key) or GeocodeOutcome(
                location_key=key, match_status="matched_exact", match_method="raw_full_exact", match_confidence=0.9,
                latitude=59.0, longitude=18.0, geocode_provider="osm", geocode_precision="address", coordinate_method="resolver",
                coordinate_locality="", coordinate_supporting_point_count=1, coordinate_spread_meters=None,
                policy_version=POLICY, reference_md5=REFERENCE, from_cache=False, matched_at=matched_at,
            )
        return result
    monkeypatch.setattr(batch, "geocode_addresses", fake)
    monkeypatch.setattr(batch, "ensure_reference_documents", lambda duckdb, log=None: REFERENCE)


def run(client, ids, *, changed_only=True, page_size=batch.PAGE_SIZE):
    return batch.fold_companies(client, object(), ids, changed_only=changed_only, source_run_id="run-1",
                                folded_at=FOLDED_AT, page_size=page_size)


def test_sql_texts_bind_ids_read_final_and_filter_drafts_and_no_address() -> None:
    assert "%(company_ids)s" in batch.current_normalized_sql() and "FINAL" in batch.current_normalized_sql()
    assert "source != 'reviewer_draft'" in batch.current_normalized_sql()
    assert "parse_status IN ('ok', 'partial', 'foreign')" in batch.current_normalized_sql()
    assert batch.normalized_watermarks_sql().count("FINAL") == 1 and "countIf" in batch.normalized_watermarks_sql()
    assert "FINAL" in batch.hidden_keys_sql() and "action = 'hide'" in batch.hidden_keys_sql() and "removed = 0" in batch.hidden_keys_sql()
    assert "field = 'text'" in batch.company_precedence_sql()
    stale = batch.stale_companies_sql()
    for fragment in ("inactive_reason != 'withdrawn'", "geocode_status != 'foreign'", "legacy_adopted_v1", "%(policy)s", "%(reference)s", "%(normalizer)s"):
        assert fragment in stale
    assert batch.main_insert_sql().startswith(f"INSERT INTO {tables.QUALIFIED_MAIN_TABLE} (")
    assert batch.history_insert_sql().startswith(f"INSERT INTO {tables.QUALIFIED_HISTORY_TABLE} (")
    assert f"modulo(cityHash64(company_id), {batch.BUCKET_COUNT})" in batch.bucket_company_ids_sql()


def test_first_fold_writes_history_then_main_with_a_geocode_block(monkeypatch) -> None:
    calls: list = []
    stub_geocode(monkeypatch, calls=calls)
    client = FakeClient({
        "normalized_watermarks_sql": [(A, T1, 1)], "main_watermarks_sql": [], "rule_watermarks_sql": [], "stale_companies_sql": [],
        "current_normalized_sql": [normalized_row(A, "scb"), normalized_row(A, "bolagsverket", kind="registered")],
        "current_main_rows_sql": [], "hidden_keys_sql": [], "company_precedence_sql": [],
    })
    counts = run(client, [A])
    assert (counts.considered, counts.folded, counts.published, counts.changed, counts.unchanged) == (1, 1, 1, 1, 0)
    assert (counts.geocoded, counts.cache_hits, counts.matched) == (1, 0, 1)
    assert [sql.split(" (")[0] for sql, _ in client.inserts] == [f"INSERT INTO {tables.QUALIFIED_HISTORY_TABLE}", f"INSERT INTO {tables.QUALIFIED_MAIN_TABLE}"]
    row = dict(zip(tables.MAIN_COLUMNS, client.inserts[1][1][0]))
    assert (row["geocode_status"], row["geocode_policy"], row["geocode_reference"], row["geocoded_at"]) == ("matched_exact", POLICY, REFERENCE, FOLDED_AT)
    assert row["sources"] == ["bolagsverket", "scb"] and row["folded_at"] == FOLDED_AT and row["fold_version"] == FOLD_VERSION
    assert len(calls[0]) == 1  # one merged address, one location key


def test_an_unchanged_company_rewrites_main_without_history(monkeypatch) -> None:
    stub_geocode(monkeypatch)
    key = normalized_row(A, "scb")[14]
    client = FakeClient({
        "normalized_watermarks_sql": [(A, T1, 1)], "main_watermarks_sql": [(A, T0)], "rule_watermarks_sql": [], "stale_companies_sql": [],
        "current_normalized_sql": [normalized_row(A, "scb")], "current_main_rows_sql": [main_row(A, key)],
        "hidden_keys_sql": [], "company_precedence_sql": [],
    })
    counts = run(client, [A])
    assert (counts.changed, counts.unchanged) == (0, 1)
    assert [sql.split(" (")[0] for sql, _ in client.inserts] == [f"INSERT INTO {tables.QUALIFIED_MAIN_TABLE}"]


def test_changed_only_selection_rules(monkeypatch) -> None:
    stub_geocode(monkeypatch)
    client = FakeClient({
        # A: main newer than everything -> skipped. B: rule newer -> folded. F: no main, publishable -> folded.
        "normalized_watermarks_sql": [(A, T0, 1), (B, T0, 1), (F, T1, 1)],
        "main_watermarks_sql": [(A, T1), (B, T1)],
        "rule_watermarks_sql": [(B, T2)],
        "stale_companies_sql": [],
        "current_normalized_sql": [normalized_row(B, "scb"), normalized_row(F, "scb")],
        "current_main_rows_sql": [main_row(B, normalized_row(B, "scb")[14])], "hidden_keys_sql": [], "company_precedence_sql": [],
    })
    counts = run(client, [A, B, F])
    assert (counts.companies, counts.considered) == (3, 2)
    scoped = [params["company_ids"] for sql, params, _ in client.calls if sql == batch.current_normalized_sql()]
    assert scoped == [[B, F]]


def test_a_company_with_only_no_address_rows_and_no_main_row_is_never_selected(monkeypatch) -> None:
    stub_geocode(monkeypatch)
    client = FakeClient({"normalized_watermarks_sql": [(A, T1, 0)], "main_watermarks_sql": [], "rule_watermarks_sql": [], "stale_companies_sql": []})
    counts = run(client, [A])
    assert counts.considered == 0 and client.inserts == []


def test_a_stale_geocode_policy_selects_the_company(monkeypatch) -> None:
    stub_geocode(monkeypatch)
    key = normalized_row(A, "scb")[14]
    client = FakeClient({
        "normalized_watermarks_sql": [(A, T0, 1)], "main_watermarks_sql": [(A, T1)], "rule_watermarks_sql": [], "stale_companies_sql": [(A,)],
        "current_normalized_sql": [normalized_row(A, "scb")], "current_main_rows_sql": [main_row(A, key, geocode_policy="se-address-resolution-policy-v6")],
        "hidden_keys_sql": [], "company_precedence_sql": [],
    })
    counts = run(client, [A])
    assert counts.considered == 1
    stale_params = [params for sql, params, _ in client.calls if sql == batch.stale_companies_sql()][0]
    assert stale_params == {"company_ids": [A], "policy": POLICY, "reference": REFERENCE, "normalizer": NORMALIZER_VERSION}


def test_foreign_rows_publish_without_reaching_geocode(monkeypatch) -> None:
    calls: list = []
    stub_geocode(monkeypatch, calls=calls)
    foreign = normalized_row(F, "scb", street_name=None, house_number=None, postal_code=None, city=None, country_code="", parse_status="foreign", text="")
    client = FakeClient({
        "normalized_watermarks_sql": [(F, T1, 1)], "main_watermarks_sql": [], "rule_watermarks_sql": [], "stale_companies_sql": [],
        "current_normalized_sql": [foreign], "current_main_rows_sql": [], "hidden_keys_sql": [], "company_precedence_sql": [],
    })
    counts = run(client, [F])
    assert counts.published == 1 and counts.geocoded == 0 and calls == []
    row = dict(zip(tables.MAIN_COLUMNS, client.inserts[1][1][0]))
    assert (row["geocode_status"], row["latitude"], row["geocode_policy"]) == ("foreign", None, "")


def test_withdrawn_rows_keep_their_block_and_are_not_geocoded(monkeypatch) -> None:
    calls: list = []
    stub_geocode(monkeypatch, calls=calls)
    old_key = "a" * 64
    client = FakeClient({
        "normalized_watermarks_sql": [(A, T2, 1)], "main_watermarks_sql": [(A, T1)], "rule_watermarks_sql": [], "stale_companies_sql": [],
        "current_normalized_sql": [normalized_row(A, "scb", house_number="7", text="Storgatan 7, 111 22 Stockholm")],
        "current_main_rows_sql": [main_row(A, old_key)], "hidden_keys_sql": [], "company_precedence_sql": [],
    })
    counts = run(client, [A])
    assert (counts.published, counts.withdrawn, counts.changed) == (1, 1, 2)
    assert len(calls[0]) == 1
    rows = [dict(zip(tables.MAIN_COLUMNS, values)) for values in client.inserts[1][1]]
    withdrawn = [r for r in rows if r["address_key"] == old_key][0]
    assert (withdrawn["active"], withdrawn["inactive_reason"], withdrawn["latitude"]) == (0, "withdrawn", 59.3)


def test_a_hide_rule_and_a_company_precedence_row_reach_the_fold(monkeypatch) -> None:
    stub_geocode(monkeypatch)
    key = normalized_row(A, "scb")[14]
    client = FakeClient({
        "normalized_watermarks_sql": [(A, T1, 1)], "main_watermarks_sql": [], "rule_watermarks_sql": [(A, T1)], "stale_companies_sql": [],
        "current_normalized_sql": [normalized_row(A, "scb"), normalized_row(A, "ratsit")], "current_main_rows_sql": [],
        "hidden_keys_sql": [(A, key)], "company_precedence_sql": [(A, "ratsit", 5000)],
    })
    counts = run(client, [A])
    assert (counts.published, counts.hidden) == (0, 1)
    row = dict(zip(tables.MAIN_COLUMNS, client.inserts[1][1][0]))
    assert (row["active"], row["inactive_reason"], row["text_source"]) == (0, "hidden", "ratsit")


def test_a_missing_outcome_fails_the_page(monkeypatch) -> None:
    monkeypatch.setattr(batch, "geocode_addresses", lambda addresses, **kwargs: {})
    monkeypatch.setattr(batch, "ensure_reference_documents", lambda duckdb, log=None: REFERENCE)
    client = FakeClient({
        "normalized_watermarks_sql": [(A, T1, 1)], "main_watermarks_sql": [], "rule_watermarks_sql": [], "stale_companies_sql": [],
        "current_normalized_sql": [normalized_row(A, "scb")], "current_main_rows_sql": [], "hidden_keys_sql": [], "company_precedence_sql": [],
    })
    with pytest.raises(RuntimeError):
        run(client, [A])
    assert client.inserts == []


def test_every_id_bound_read_passes_the_query_settings_and_pages(monkeypatch) -> None:
    stub_geocode(monkeypatch)
    client = FakeClient({"normalized_watermarks_sql": [], "main_watermarks_sql": [], "rule_watermarks_sql": [], "stale_companies_sql": []})
    run(client, [A, B, F], page_size=2)
    bound = [(params, settings) for sql, params, settings in client.calls if "%(company_ids)s" in sql]
    assert bound and all(settings == batch.FOLD_ID_BOUND_QUERY_SETTINGS for _, settings in bound)
    assert [params["company_ids"] for params, _ in bound][:4] == [[A, B]] * 4


def test_a_full_page_renders_under_the_query_size_setting() -> None:
    """Rendered exactly as tests/test_se_company_basic_info_batch.py renders it: the
    driver's escape_params against a SimpleNamespace context, with 12-digit ids (the wider
    of the two widths normalized_se_company_ids admits). No server needed."""
    from types import SimpleNamespace

    from clickhouse_driver.util.escape import escape_params

    DEFAULT_MAX_QUERY_SIZE = 262_144
    context = SimpleNamespace(
        server_info=SimpleNamespace(get_timezone=lambda: "UTC"),
        client_settings={"server_side_params": False},
    )
    ids = [str(556000000000 + i) for i in range(batch.PAGE_SIZE)]
    sizes = []
    for text in (batch.current_normalized_sql(), batch.current_main_rows_sql(), batch.normalized_watermarks_sql(), batch.stale_companies_sql()):
        params = {"company_ids": ids, "policy": POLICY, "reference": "0" * 32, "normalizer": NORMALIZER_VERSION}
        rendered = text % escape_params(params, context)
        rendered_size = len(rendered.encode())
        sizes.append(rendered_size)
        assert rendered_size < batch.FOLD_ID_BOUND_QUERY_SETTINGS["max_query_size"]
    # Half one: the default really would reject the largest of these renders (Code: 62,
    # "Max query size exceeded"), so the raised setting is not decoration.
    assert max(sizes) > DEFAULT_MAX_QUERY_SIZE


def test_fold_bucket_reads_the_bucket_ids_then_folds_them(monkeypatch) -> None:
    stub_geocode(monkeypatch)
    client = FakeClient({"bucket_company_ids_sql": [(A,)], "normalized_watermarks_sql": [], "main_watermarks_sql": [], "rule_watermarks_sql": [], "stale_companies_sql": []})
    counts = batch.fold_bucket(client, object(), 7, changed_only=True, source_run_id="run-1", folded_at=FOLDED_AT)
    assert counts.companies == 1
    assert client.calls[0][1] == {"bucket": 7}
    assert client.calls[0][2] == batch.FOLD_ID_BOUND_QUERY_SETTINGS
    with pytest.raises(ValueError):
        batch.fold_bucket(client, object(), 64, changed_only=True, source_run_id="run-1", folded_at=FOLDED_AT)


def test_fold_counts_as_metadata_names_every_counter() -> None:
    counts = batch.FoldCounts(companies=1, considered=2, folded=3, published=4, hidden=5, withdrawn=6, changed=7, unchanged=8,
                              unpublished=9, geocoded=10, cache_hits=11, matched=12)
    assert set(counts.as_metadata()) == {"companies", "considered", "folded", "published", "hidden", "withdrawn", "changed",
                                         "unchanged", "unpublished", "geocoded", "cache_hits", "matched"}


def test_invalid_company_ids_are_refused_before_any_query(monkeypatch) -> None:
    stub_geocode(monkeypatch)
    client = FakeClient({})
    with pytest.raises(ValueError):
        run(client, ["12"])
    assert client.calls == []
