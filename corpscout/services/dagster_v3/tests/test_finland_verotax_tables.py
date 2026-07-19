from dagster_v3.defs.finland_verotax import tables
from dagster_v3.defs.finland_verotax.resources import parse_year_sources


def test_export_columns_drop_provenance_only() -> None:
    dropped = set(tables.FI_TAX_RECORDS_COLUMNS) - set(
        tables.FI_TAX_RECORDS_EXPORT_COLUMNS
    )
    assert dropped == set(tables.CLICKHOUSE_EXCLUDED_COLUMNS)
    # Export order preserves the full column order.
    filtered = tuple(
        column
        for column in tables.FI_TAX_RECORDS_COLUMNS
        if column not in tables.CLICKHOUSE_EXCLUDED_COLUMNS
    )
    assert tables.FI_TAX_RECORDS_EXPORT_COLUMNS == filtered


def test_metric_columns_paired_original_usd() -> None:
    for metric in tables.TAX_METRIC_NAMES:
        assert f"{metric}_amount_original" in tables.FI_TAX_RECORDS_COLUMNS
        assert f"{metric}_amount_usd" in tables.FI_TAX_RECORDS_COLUMNS


def test_raw_column_shapes() -> None:
    assert len(tables.RAW_COLUMNS_8) == 8
    assert len(tables.RAW_COLUMNS_9) == 9
    assert "prepayments_total" not in tables.RAW_COLUMNS_8
    assert "prepayments_total" in tables.RAW_COLUMNS_9


def test_duckdb_file_stem_differs_from_dataset_name() -> None:
    assert tables.DUCKDB_FILE_NAME.removesuffix(".duckdb") != tables.DLT_DATASET_NAME


def test_fallback_year_sources_cover_expected_years() -> None:
    assert set(tables.FALLBACK_YEAR_SOURCES) >= set(tables.EXPECTED_YEARS)


def test_parse_year_sources_from_index_html() -> None:
    html = """
    <a href="/contentassets/a7/2025-tuloverotuksen-muutosverotuksen-julkiset-tiedot-alkaen-vv2022.csv">x</a>
    <a href="/contentassets/a7/verohallinto_yhteisojen-tuloverotuksen-julkiset-tiedot-2020.csv">x</a>
    <a href="/contentassets/a7/yhteiso-tuloverotus-julk-2023.csv">x</a>
    <a href="/contentassets/a7/yhteis%C3%B6_tuloverotus_julk_2024.csv">x</a>
    <a href="/contentassets/a7/kiinteistoverotus-2024.csv">x</a>
    """
    sources = parse_year_sources(html)
    assert set(sources) == {2020, 2023, 2024}
    assert sources[2024].endswith("yhteis%C3%B6_tuloverotus_julk_2024.csv")
    assert sources[2024].startswith(tables.VERO_BASE_URL)
    # The amendments file (muutos) and unrelated datasets are excluded.
    assert all("muutos" not in url for url in sources.values())
