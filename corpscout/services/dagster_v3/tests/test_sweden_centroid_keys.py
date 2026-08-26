import duckdb

from dagster_v3.defs.sweden_company.centroid_keys import city_key_sql, postcode_key_sql


def test_city_key_preserves_swedish_letters():
    con = duckdb.connect()
    for raw, expected in [("GÖTEBORG", "GÖTEBORG"), (" Göteborg ", "GÖTEBORG"),
                          ("Upplands Väsby", "UPPLANDS VÄSBY"), ("trelleborg", "TRELLEBORG")]:
        got = con.execute(f"select {city_key_sql('?')}", [raw]).fetchone()[0]
        assert got == expected, (raw, got)


def test_postcode_key_is_digits_only():
    con = duckdb.connect()
    for raw, expected in [("231 00", "23100"), ("23100", "23100"), ("  114 56 ", "11456")]:
        assert con.execute(f"select {postcode_key_sql('?')}", [raw]).fetchone()[0] == expected
