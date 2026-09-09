from dagster import AssetKey

from dagster_v3.defs.esef_filings import segment_assets


def test_manifest_and_parse_rows_never_stamp_a_company() -> None:
    assert not hasattr(segment_assets, "load_esef_company_links")
    assert not hasattr(segment_assets, "_company_id_for_index_row")
    index_row = {
        "source_document_id": "LEI-2024-12-31-ESEF-SE-0", "package_sha256": "AB" * 32, "lei": "LEI",
        "entity_name": "X AB", "country_iso2": "SE", "period_end": "2024-12-31", "package_url": "u",
        "report_url": "", "viewer_url": "",
    }
    common = segment_assets._contact_candidate_common(
        index_row, fiscal_year=2024, source_run_id="r", extracted_at="2026-01-01T00:00:00Z"
    )
    assert "country_iso2" not in common and "company_id" not in common
    assert common["lei"] == "LEI"


def test_manifest_asset_no_longer_depends_on_the_entity_registry_map() -> None:
    manifest_asset = segment_assets.esef_document_extraction_manifest_s3
    assert AssetKey("esef_entity_registry_map_clickhouse") not in (
        manifest_asset.dependency_keys
    )
