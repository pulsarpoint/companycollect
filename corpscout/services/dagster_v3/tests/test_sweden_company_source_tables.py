"""The two register source tables of the 2026-09-03 SE basic-info design (spec 3.1).

The column pins read the migration DDL through tests/se_company_ddl.py -- the same helper
test_se_company_layout.py uses -- so the tuples in sweden_company/tables.py and the
deployed DDL cannot drift apart. The exporter binds those tuples positionally.
"""

from dagster_v3.defs.sweden_company import tables
from tests.se_company_ddl import declared_columns, table_block


def test_se_scb_companies_declares_the_whole_scb_record_in_export_order() -> None:
    assert declared_columns("se_scb_companies") == list(
        tables.SE_SCB_COMPANIES_EXPORT_COLUMNS
    )
    block = table_block("se_scb_companies")

    assert block.count("CREATE TABLE") == 1 and block.endswith(";")
    assert "ENGINE = ReplacingMergeTree(observed_at)" in block
    assert "ORDER BY company_id" in block
    # SCB's own codes, kept as delivered: turning FtgStat into an entity status is the scb
    # suggestion extractor's job (slice 1), never a source table's.
    assert "source_status_code LowCardinality(Nullable(String))" in block
    assert "source_secondary_status_code LowCardinality(Nullable(String))" in block
    assert "derived_status" not in block
    # Date32, not 000257's Date: Date starts at 1970-01-01 and registration dates do not.
    assert "registration_date Nullable(Date32)" in block
    # The address and SNI columns company_registry_states never carried.
    for column in ("ng1_code", "ng5_code", "care_of", "street_address", "post_town"):
        assert f"    {column} " in block, column


def test_se_bolagsverket_companies_declares_the_whole_bolagsverket_record() -> None:
    assert declared_columns("se_bolagsverket_companies") == list(
        tables.SE_BOLAGSVERKET_COMPANIES_EXPORT_COLUMNS
    )
    block = table_block("se_bolagsverket_companies")

    assert block.count("CREATE TABLE") == 1 and block.endswith(";")
    assert "ENGINE = ReplacingMergeTree(observed_at)" in block
    assert "ORDER BY company_id" in block
    assert "registration_date Nullable(Date32)" in block
    assert "deregistration_date Nullable(Date32)" in block
    # The packed postal address exactly as Bolagsverket delivers it: parsing belongs to the
    # address entity, which keeps its own tables until its own slice.
    assert "postal_address Nullable(String)" in block
    assert "derived_status" not in block


def test_neither_source_table_carries_provenance_it_does_not_need() -> None:
    """Provenance is the four columns section 3.1 names. No source_record_uid DEFAULT
    (000257's registry tables had one; the slice-1 extractors compute the uid themselves),
    no raw_record, no *_current bookkeeping."""
    for table in ("se_scb_companies", "se_bolagsverket_companies"):
        block = table_block(table)
        assert "source_run_id String" in block, table
        assert "source_record_id String" in block, table
        assert "source_payload_hash String" in block, table
        assert "observed_at DateTime64(3, 'UTC')" in block, table
        assert "source_record_uid" not in block, table
        assert "raw_record" not in block, table
        assert "has_observation" not in block, table
        assert "observation_fingerprint" not in block, table
        assert "state_fingerprint" not in block, table
