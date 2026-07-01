from norway_financial_bootstrap.storage import (
    completed_key_from_raw_fetch_key,
    raw_fetch_key,
)


def test_raw_fetch_key_matches_existing_norway_financial_storage_contract() -> None:
    assert raw_fetch_key("811685852", "2024") == (
        "norway_brreg/financial/raw_fetches/org=811685852/"
        "year=2024/financial_fetch.parquet"
    )


def test_completed_key_from_raw_fetch_key_parses_existing_storage_path() -> None:
    assert completed_key_from_raw_fetch_key(
        "norway_brreg/financial/raw_fetches/org=811685852/"
        "year=2024/financial_fetch.parquet"
    ) == ("811685852", "2024")
