"""The bulk warm step: reads every current location key, hands them to `geocode_addresses`
in chunks. A fake client records every statement; `geocode_addresses` is stubbed."""

from datetime import UTC, datetime
from typing import Any

from dagster_v3.defs.se_company.address import warm
from dagster_v3.defs.se_company.address.geocode import GEOCODE_QUERY_SETTINGS, GeocodeOutcome
from dagster_v3.defs.sweden_company.address_resolution_policy import SWEDEN_ADDRESS_RESOLUTION_POLICY
from dagster_v3.defs.sweden_company.geocode_serving_overlay import GEOCODE_FALLBACK_PROVIDER

POLICY = SWEDEN_ADDRESS_RESOLUTION_POLICY.version
REFERENCE = "ref-1"
MATCHED_AT = datetime(2026, 9, 7, 9, 0, tzinfo=UTC)


def key_row(*, street_name="storgatan", house_number="5", postal_code="11122", city="stockholm",
            box=None, unit=None, country_code="SE", normalized_address="Storgatan 5, 111 22 Stockholm") -> tuple:
    return (country_code, postal_code, city, street_name, box, house_number, unit, normalized_address)


def stub_outcome(key: str, *, from_cache: bool, match_status: str = "matched_exact",
                  geocode_provider: str = "osm") -> GeocodeOutcome:
    return GeocodeOutcome(
        location_key=key, match_status=match_status, match_method="raw_full_exact", match_confidence=0.9,
        latitude=59.0, longitude=18.0, geocode_provider=geocode_provider, geocode_precision="address",
        coordinate_method="resolver", coordinate_locality="", coordinate_supporting_point_count=1,
        coordinate_spread_meters=None, policy_version=POLICY, reference_md5=REFERENCE,
        from_cache=from_cache, matched_at=MATCHED_AT,
    )


class FakeClient:
    def __init__(self, rows: list[tuple]) -> None:
        self.rows = rows
        self.calls: list[tuple[str, Any, Any]] = []

    def execute(self, sql, params=None, settings=None):
        self.calls.append((sql, params, settings))
        assert sql == warm.keys_sql(limit=(params or {}).get("limit", 0))
        return self.rows


def stub_geocode(monkeypatch, outcomes_by_chunk: list[dict[str, GeocodeOutcome]] | None = None, *, calls: list | None = None):
    chunks = list(outcomes_by_chunk or [])

    def fake(addresses, *, clickhouse, duckdb, run_id, matched_at, log=None):
        if calls is not None:
            calls.append((dict(addresses), run_id, matched_at))
        if chunks:
            outcomes = chunks.pop(0)
            assert set(outcomes) == set(addresses)
            return outcomes
        return {key: stub_outcome(key, from_cache=False) for key in addresses}

    monkeypatch.setattr(warm, "geocode_addresses", fake)


def test_keys_sql_reads_final_filters_drafts_and_statuses_and_groups_by_the_seven_fields() -> None:
    sql = warm.keys_sql()
    assert "FINAL" in sql
    assert "source != 'reviewer_draft'" in sql
    assert "parse_status IN ('ok', 'partial')" in sql
    assert f"GROUP BY {', '.join(warm.KEY_COLUMNS)}" in sql
    assert "LIMIT" not in sql


def test_keys_sql_with_limit_ends_with_the_limit_placeholder_and_binds_it(monkeypatch) -> None:
    sql = warm.keys_sql(limit=5)
    assert sql.endswith("LIMIT %(limit)s")
    stub_geocode(monkeypatch)
    client = FakeClient([])
    warm.warm_geocodes(client, object(), run_id="run-1", matched_at=MATCHED_AT, limit=5)
    call_sql, params, settings = client.calls[0]
    assert call_sql == warm.keys_sql(limit=5)
    assert params == {"limit": 5}
    assert settings == GEOCODE_QUERY_SETTINGS


def test_addresses_from_rows_status_and_care_of_and_key_collapse() -> None:
    ok_row = key_row(postal_code="11122", city="stockholm")
    partial_row = key_row(postal_code=None, city=None)
    addresses = warm.addresses_from_rows([ok_row, partial_row])
    statuses = {a.parse_status for a in addresses.values()}
    assert statuses == {"ok", "partial"}
    for address in addresses.values():
        assert address.care_of is None

    same_key_a = key_row(normalized_address="Storgatan 5, 111 22 Stockholm")
    same_key_b = key_row(normalized_address="STORGATAN 5, 111 22 STOCKHOLM")
    collapsed = warm.addresses_from_rows([same_key_a, same_key_b])
    assert len(collapsed) == 1


def test_chunking_makes_one_geocode_call_per_chunk_with_run_id_and_matched_at_and_counts(monkeypatch) -> None:
    rows = [
        key_row(house_number="1"),
        key_row(house_number="2"),
        key_row(house_number="3"),
    ]
    addresses = warm.addresses_from_rows(rows)
    keys = sorted(addresses)
    assert len(keys) == 3

    outcomes = [
        {keys[0]: stub_outcome(keys[0], from_cache=True, match_status="matched_exact"),
         keys[1]: stub_outcome(keys[1], from_cache=False, match_status="unmatched", geocode_provider=GEOCODE_FALLBACK_PROVIDER)},
        {keys[2]: stub_outcome(keys[2], from_cache=False, match_status="matched_street")},
    ]
    calls: list = []
    stub_geocode(monkeypatch, outcomes, calls=calls)

    client = FakeClient(rows)
    counts = warm.warm_geocodes(client, object(), run_id="run-1", matched_at=MATCHED_AT, chunk_size=2)

    assert len(calls) == 2
    assert len(calls[0][0]) == 2 and len(calls[1][0]) == 1
    assert calls[0][1] == "run-1" and calls[0][2] == MATCHED_AT
    assert calls[1][1] == "run-1" and calls[1][2] == MATCHED_AT

    assert counts.keys == 3
    assert counts.chunks == 2
    assert counts.cache_hits == 1
    assert counts.matched == 2
    assert counts.geocoded == 2  # matched_exact and matched_street are GEOCODED_STATUSES; unmatched is not
    assert counts.fallback == 1  # the centroid_fallback provider outcome


def test_no_rows_makes_zero_chunks_and_no_geocode_call(monkeypatch) -> None:
    calls: list = []
    stub_geocode(monkeypatch, calls=calls)
    client = FakeClient([])
    counts = warm.warm_geocodes(client, object(), run_id="run-1", matched_at=MATCHED_AT)
    assert calls == []
    assert (counts.keys, counts.chunks, counts.cache_hits, counts.matched, counts.geocoded, counts.fallback) == (0, 0, 0, 0, 0, 0)


def test_warm_counts_as_metadata_names_the_six_counters() -> None:
    counts = warm.WarmCounts(keys=1, chunks=2, cache_hits=3, matched=4, geocoded=5, fallback=6)
    assert set(counts.as_metadata()) == {"keys", "chunks", "cache_hits", "matched", "geocoded", "fallback"}
