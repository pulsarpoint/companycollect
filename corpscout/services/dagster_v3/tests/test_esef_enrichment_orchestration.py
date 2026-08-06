from dagster import AssetKey

from dagster_v3.defs.esef_filings import assets
from dagster_v3.defs.esef_filings.enrichment_orchestration import (
    ESEF_DOCUMENT_LLM_SELECTION,
)


def test_routine_weekly_refresh_includes_deterministic_document_evidence() -> None:
    repo = _repository()
    refresh_job = repo.get_job("esef_filings_refresh_job")
    refresh_keys = refresh_job.asset_layer.executable_asset_keys

    assert AssetKey("esef_document_extraction_manifest_s3") in refresh_keys
    assert AssetKey("esef_source_documents_duckdb") in refresh_keys
    assert AssetKey("esef_document_contact_candidates_duckdb") in refresh_keys
    assert AssetKey("esef_document_concept_labels_duckdb") in refresh_keys
    assert (
        AssetKey("esef_document_concept_official_translations_clickhouse")
        in refresh_keys
    )
    assert AssetKey("esef_document_concept_translation_load") in refresh_keys
    assert AssetKey("esef_fact_disclosures_duckdb") in refresh_keys
    assert AssetKey("esef_company_source_records_clickhouse") in refresh_keys
    assert AssetKey("esef_document_company_information_duckdb") not in refresh_keys
    assert refresh_job.partitions_def is assets.ESEF_PROCESSED_WEEK_PARTITIONS


def test_paid_llm_job_is_explicit_and_unpartitioned() -> None:
    repo = _repository()
    llm_keys = ESEF_DOCUMENT_LLM_SELECTION.resolve(repo.asset_graph)
    llm_job = repo.get_job("esef_document_company_information_job")

    assert llm_keys == {
        AssetKey("esef_document_company_information_duckdb"),
        AssetKey("esef_document_company_information_clickhouse"),
        AssetKey("esef_document_observations_clickhouse"),
    }
    assert AssetKey("esef_source_documents_clickhouse") not in llm_keys
    assert llm_job.partitions_def is None


def test_fiscal_year_evidence_sensor_is_removed() -> None:
    sensor_names = {sensor.name for sensor in _repository().sensor_defs}

    assert "esef_index_document_evidence_sensor" not in sensor_names


def _repository():
    from dagster_v3.definitions import defs as load_defs

    return load_defs().get_repository_def()
