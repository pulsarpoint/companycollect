import io
import zipfile
from decimal import Decimal
from datetime import date
from pathlib import Path

import duckdb
import pytest

from dagster_v3.defs.estonia_ar import financials, tables


def _zip_session(csv_text: str):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("data.csv", csv_text.encode("utf-8"))
    body = buf.getvalue()

    class _Resp:
        content = body
        headers = {"Content-Length": str(len(body))}

        def raise_for_status(self):
            return None

        def iter_content(self, chunk_size=0):
            yield body

    class _Session:
        def get(self, url, *, timeout, stream=False):
            return _Resp()

    return _Session()


RG_HEADER = (
    '"report_id";"taidetud_aruanne_report_id";"registrikood";"õiguslik vorm";'
    '"staatus";"aruandeaasta";"kas konsolideeritud?";"period_start";"period_end";'
    '"esitatud_kpv";"kas auditeeritud?";"valitud aruanne kategooria"'
)
RG_CSV = "\n".join(
    [
        RG_HEADER,
        '1703729;;10000018;"OÜ";"Registrisse kantud";2019;"Ei";"01.01.2019";'
        '"31.12.2019";"14.10.2020";"Jah";"Suurettevõtja"',
    ]
)

KI_HEADER = '"report_id";"tabel";"elemendi_label";"elemendi_nimetus";"vaartus"'
KI_2019_CSV = "\n".join(
    [
        KI_HEADER,
        '1703729;"Bilanss";"Varad";"Assets";"50000.00"',
        '1703729;"Bilanss";"Käibevara";"CurrentAssets";"30000.00"',
        '1703729;"Bilanss";"Põhivara";"NonCurrentAssets";"20000.00"',
        '1703729;"Bilanss";"Omakapital";"Equity";"15000.00"',
        '1703729;"Bilanss";"Lühiajalised kohustised";"CurrentLiabilities";"25000.00"',
        '1703729;"Bilanss";"Pikaajalised kohustised";"NonCurrentLiabilities";"10000.00"',
        '1703729;"Kasumiaruanne";"Müügitulu";"Revenue";"100000.00"',
        '1703729;"Kasumiaruanne";"Kasum enne tulumaksu";"TotalProfitLossBeforeTax";"6000.00"',
        '1703729;"Kasumiaruanne";"Aruandeaasta kasum";"TotalAnnualPeriodProfitLoss";"5000.00"',
    ]
)


def _seed_raw(db_path: Path, *, report_general: str = RG_CSV) -> None:
    financials.load_estonia_ar_financial_csv(
        database_path=db_path,
        download_url="https://example/report_general.zip",
        raw_table=tables.REPORT_GENERAL_RAW_TABLE,
        session=_zip_session(report_general),
    )
    for year in tables.EE_FINANCIAL_YEARS:
        csv_text = KI_2019_CSV if year == 2019 else KI_HEADER
        financials.load_estonia_ar_financial_csv(
            database_path=db_path,
            download_url=f"https://example/{year}.zip",
            raw_table=tables.key_indicators_raw_table(year),
            session=_zip_session(csv_text),
        )


def test_pivot_builds_wide_statement_with_english_and_metrics(tmp_path: Path):
    db_path = tmp_path / "estonia_ar_source.duckdb"
    _seed_raw(db_path)

    counts = financials.build_estonia_ar_financial_statements(
        database_path=db_path, source_run_id="run-1"
    )
    assert counts["financial_statements"] == 1
    assert counts["with_metrics"] == 1

    wide = f"{tables.DLT_DATASET_NAME}.{tables.FINANCIAL_STATEMENTS_WIDE_TABLE}"
    with duckdb.connect(str(db_path), read_only=True) as conn:
        row = conn.execute(
            "select reg_code, fiscal_year, period_start_date, period_end_date, "
            "submitted_date, is_consolidated, is_audited, report_category_original, "
            "report_category_en, currency, revenue, gross_profit, pretax_result, "
            "net_result, total_assets, current_assets, non_current_assets, equity, "
            "current_liabilities, non_current_liabilities "
            f"from {wide} where report_id = '1703729'"
        ).fetchone()
    assert row[0] == "10000018"  # reg_code
    assert row[1] == 2019  # fiscal_year
    assert row[2] == date(2019, 1, 1)
    assert row[3] == date(2019, 12, 31)
    assert row[4] == date(2020, 10, 14)  # submitted
    assert row[5] == 0  # is_consolidated (Ei)
    assert row[6] == 1  # is_audited (Jah)
    assert row[7] == "Suurettevõtja"
    assert row[8] == "Large enterprise"  # report_category_en
    assert row[9] == "EUR"
    assert row[10] == Decimal("100000.00")  # revenue
    assert row[11] is None  # gross_profit (no Estonian element)
    assert row[12] == Decimal("6000.00")  # pretax_result
    assert row[13] == Decimal("5000.00")  # net_result
    assert row[14] == Decimal("50000.00")  # total_assets (Assets)
    assert row[15] == Decimal("30000.00")  # current_assets
    assert row[16] == Decimal("20000.00")  # non_current_assets
    assert row[17] == Decimal("15000.00")  # equity
    assert row[18] == Decimal("25000.00")  # current_liabilities
    assert row[19] == Decimal("10000.00")  # non_current_liabilities


def test_wide_columns_match_schema(tmp_path: Path):
    db_path = tmp_path / "estonia_ar_source.duckdb"
    _seed_raw(db_path)
    financials.build_estonia_ar_financial_statements(
        database_path=db_path, source_run_id="run-1"
    )
    wide = f"{tables.DLT_DATASET_NAME}.{tables.FINANCIAL_STATEMENTS_WIDE_TABLE}"
    with duckdb.connect(str(db_path), read_only=True) as conn:
        cols = [r[0] for r in conn.execute(f"describe {wide}").fetchall()]
    assert set(cols) == set(tables.EE_FINANCIAL_STATEMENTS_COLUMNS)


class _IndexSession:
    def __init__(self, *, text: str | None = None, raise_exc: Exception | None = None):
        self._text = text
        self._raise = raise_exc

    def get(self, url, *, timeout, stream=False):
        if self._raise is not None:
            raise self._raise
        text = self._text

        class _Resp:
            def raise_for_status(self):
                return None

        resp = _Resp()
        resp.text = text
        return resp


# a *different* datestamp + suffix than the pinned constant, to prove resolution.
_INDEX_HTML = (
    '<a href="/sites/default/files/1.aruannete_yldandmed_kuni_30062026_1.zip">x</a>'
    '<a href="/sites/default/files/4.2024_aruannete_elemendid_kuni_30062026_1.zip">y</a>'
)


def test_resolve_financial_url_reads_current_datestamp_from_index():
    url = financials.resolve_financial_url(
        tables.REPORT_GENERAL_RAW_TABLE, session=_IndexSession(text=_INDEX_HTML)
    )
    assert url.endswith("/1.aruannete_yldandmed_kuni_30062026_1.zip")
    ki = financials.resolve_financial_url(
        tables.key_indicators_raw_table(2024), session=_IndexSession(text=_INDEX_HTML)
    )
    assert ki.endswith("/4.2024_aruannete_elemendid_kuni_30062026_1.zip")


def test_resolve_financial_url_falls_back_to_pinned_on_failure():
    pinned = tables.EE_FINANCIAL_RAW_SOURCES[tables.REPORT_GENERAL_RAW_TABLE]
    # index fetch raises
    assert (
        financials.resolve_financial_url(
            tables.REPORT_GENERAL_RAW_TABLE,
            session=_IndexSession(raise_exc=RuntimeError("down")),
        )
        == pinned
    )
    # index reachable but missing the file -> pinned
    assert (
        financials.resolve_financial_url(
            tables.REPORT_GENERAL_RAW_TABLE, session=_IndexSession(text="<html></html>")
        )
        == pinned
    )


def test_build_refuses_empty_report_general(tmp_path: Path):
    db_path = tmp_path / "estonia_ar_source.duckdb"
    _seed_raw(db_path, report_general=RG_HEADER)  # header only -> 0 spine rows
    with pytest.raises(ValueError, match="refusing to replace"):
        financials.build_estonia_ar_financial_statements(
            database_path=db_path, source_run_id="run-1"
        )
