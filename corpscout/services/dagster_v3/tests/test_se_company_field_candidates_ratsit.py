"""The Ratsit candidate extractor: newest complete report, first industry, newest periods."""

from datetime import UTC, datetime

import dagster as dg

from dagster_v3.defs.se_company.fields.candidates import ratsit
from dagster_v3.defs.se_company.fields.candidates.common import CandidateRow
from dagster_v3.defs.sweden_ratsit.normalization import RATSIT_NORMALIZER_VERSION

HB = "5020077862"
OBSERVED = datetime(2024, 12, 31, tzinfo=UTC)


def test_scope_scans_the_completion_marker_only() -> None:
    sql = ratsit.build_scope_sql()
    assert (f"SELECT company_id, toDateTime64(normalized_at, 3, 'UTC') AS changed_at FROM corpscout.se_ratsit_company\n"
            f"    WHERE normalizer_version = '{RATSIT_NORMALIZER_VERSION}'") in sql
    assert "se_ratsit_financial_periods" not in sql  # children are complete once the company row exists
    # The shared scan (Task 1's changed_companies_scope_sql): per-company watermark, since floor.
    assert "FROM corpscout.se_company_field_candidate\n    WHERE source = '" in sql
    assert "changes.changed_at > greatest(ifNull(watermark.extracted_at, toDateTime64('1970-01-01 00:00:00.000', 3, 'UTC')), parseDateTime64BestEffort(%(since)s, 3, 'UTC'))" in sql
    assert sql.endswith("ORDER BY company_id\nLIMIT %(page_size)s")


def test_candidates_pin_the_newest_report_and_convert_revenue() -> None:
    sql = ratsit.build_candidates_sql()
    assert "argMax(result_sha256, normalized_at) AS result_sha256" in sql
    assert f"FROM corpscout.se_ratsit_company FINAL\n    WHERE normalizer_version = '{RATSIT_NORMALIZER_VERSION}' AND company_id IN %(company_ids)s" in sql
    assert "INNER JOIN report ON report.company_id = codes.company_id AND report.result_sha256 = codes.result_sha256" in sql
    assert "ORDER BY codes.industry_index ASC\n    LIMIT 1 BY codes.company_id" in sql
    assert "if(codes.nace_mapping_status = 'mapped', ifNull(codes.nace_normalized_code, ''), '') AS nace_digits" in sql
    assert "LEFT JOIN labels ON labels.classification_version = industry.nace_revision AND labels.normalized_code = industry.nace_digits" in sql
    assert "industry.nace_digits AS nace_code" in sql  # nace_normalized_code is already dot-less
    assert "toDecimal128(p.revenue_amount * multiIf(p.monetary_unit = 'TSEK', 1000, p.monetary_unit = 'MSEK', 1000000, 1), 2) AS amount" in sql
    assert "ifNull(p.period_end, makeDate32(p.fiscal_year, 12, 31)) AS period_end" in sql
    assert "ASOF LEFT JOIN (SELECT rate_date, rate, k FROM fx WHERE quote_currency = 'SEK') AS sek ON periods.k = sek.k AND sek.rate_date <= periods.period_end" in sql
    assert "ASOF LEFT JOIN (SELECT rate_date, rate, k FROM fx WHERE quote_currency = 'USD') AS usd ON periods.k = usd.k AND usd.rate_date <= periods.period_end" in sql
    assert "toDecimal128(toFloat64(periods.amount) / toFloat64(sek.rate) * toFloat64(usd.rate), 2)" in sql
    for field in ("primary_sni_code", "primary_nace_code", "industry_label_en", "employee_count", "latest_revenue"):
        assert f"'{field}'" in sql, field
    assert sql.count("UNION ALL") == 4
    assert "%" not in sql.replace("%(company_ids)s", "")


def test_rows_from_result_binds_the_ratsit_source_and_version() -> None:
    rows = ratsit.rows_from_result([(HB, "employee_count", "ratsit:x:financial:0:1", OBSERVED, "11900", '{"compare_key":"11900"}')])
    assert rows == [CandidateRow(HB, "employee_count", "ratsit", "ratsit:x:financial:0:1", "11900", '{"compare_key":"11900"}',
                                 OBSERVED, "ratsit-candidates-v1")]


def test_asset_is_registered_with_the_normalized_tables_and_the_rates() -> None:
    from dagster_v3.definitions import defs as load_defs

    asset = load_defs().get_repository_def().asset_graph.get(dg.AssetKey("se_company_field_candidates_ratsit"))
    assert asset.parent_keys == {
        dg.AssetKey("se_ratsit_company"),
        dg.AssetKey("se_ratsit_company_industry_codes"),
        dg.AssetKey("se_ratsit_financial_periods"),
        dg.AssetKey("nace_categories_clickhouse"),
        dg.AssetKey("exchange_rates_v2_clickhouse"),
    }
    assert asset.group_name == "se_company_fields"
    assert asset.metadata["source"] == "ratsit"
