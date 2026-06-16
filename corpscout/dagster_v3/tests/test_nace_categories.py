def test_dlt_clickhouse_destination_dependencies_are_available() -> None:
    import clickhouse_connect
    import dlt

    assert clickhouse_connect
    assert hasattr(dlt.destinations, "clickhouse")
