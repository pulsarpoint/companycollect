"""The four address extractors' SQL on a real ClickHouse (spec 2026-09-06 section 7, esef
extractor per spec 2026-09-09 section 3): each source's scope converges, its SELECT produces
the expected wide raw row, the suggestion_id stamp matches the hash formula, a tombstoned
register row writes a NULL raw row and re-selects, and the normalize hand-off
(`changed_rows_sql()`/`normalized_row()`) turns the raw rows -- one of them freshly
tombstoned -- into the expected parse statuses. Runs under join_use_nulls 0 and 1."""

import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest

from dagster_v3.defs.esef_filings import tables as esef_tables
from dagster_v3.defs.esef_filings.country_views import build_se_esef_view_sql
from dagster_v3.defs.se_company.address import bolagsverket, esef, ratsit, scb, tables
from dagster_v3.defs.se_company.address.normalize import RAW_ROW_COLUMNS, changed_rows_sql, normalized_row
from dagster_v3.defs.se_company.address.normalize_se import NORMALIZER_VERSION
from dagster_v3.defs.se_company.address.suggestions import ADDRESS_TARGET
from dagster_v3.defs.se_company.basic_info.extract import changed_scope_sql, insert_page_sql
from dagster_v3.defs.sweden_ratsit.normalization import RATSIT_NORMALIZER_VERSION
from tests.clickhouse_local import clickhouse_local_command
from tests.test_se_company_basic_info_clickhouse_local import _bind

pytestmark = pytest.mark.integration

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
MIGRATIONS = (
    "000373_corpscout_se_scb_companies.up.sql",
    "000374_corpscout_se_bolagsverket_companies.up.sql",
    "000382_corpscout_se_company_address_suggestion.up.sql",
    "000383_corpscout_se_company_address_normalized.up.sql",
    "000390_corpscout_se_source_translated_views.up.sql",
)
# se_ratsit_company is not created by any of the five migrations above (its own migration,
# 000343, is out of scope here), so the fixture supplies it with no risk of colliding with a
# CREATE TABLE the migrations already issued for one of these five. 000390's two views
# (se_bolagsverket_companies_translated, se_ratsit_company_translated -- basic-info's ratsit
# extractor, reused here unchanged, reads the latter) need only se_bolagsverket_companies
# (000374), se_ratsit_company and text_translations (both already in the fixture); its two
# INSERT INTO ... SELECT statements are data moves the schema replay skips.
FIXTURE = Path(__file__).resolve().parent / "fixtures" / "se_basic_info_source_tables.sql"

COMPANY_SCB_BV = "5561552760"
COMPANY_RATSIT = "5560125220"
RATSIT_RESULT_SHA256 = "d" * 64
STAMP = datetime(2026, 9, 6, 12, 0, 0, tzinfo=UTC)

# company_id, lei, raw_value, expected raw_address, street_address, post_town, country_code
# (spec 2026-09-09 section 3's regex cases, extended by the review's town-bound/NBSP fixes).
# The first two reuse COMPANY_RATSIT and COMPANY_SCB_BV on purpose -- one company can carry
# both a ratsit/scb-family row and an esef row (different source, same company_id), and the
# suggestion table's key is (company_id, source, slot).
ESEF_CASES: tuple[tuple[str, str, str, str | None, str | None, str | None, str | None], ...] = (
    (
        COMPANY_RATSIT, "ESEFLEI0000000000001", "Kungsträdgårdsgatan 2, 106 70 Stockholm",
        "Kungsträdgårdsgatan 2$$Stockholm$10670$", None, None, None,
    ),
    (
        COMPANY_SCB_BV, "ESEFLEI0000000000002", "Regeringsgatan 25, 111 53 Stockholm, Sverige",
        "Regeringsgatan 25$$Stockholm$11153$SE", None, None, "SE",
    ),
    (
        "5562434182", "ESEFLEI0000000000003", "<div>Kungsgatan 17</div><div>111 43 Stockholm</div>",
        "Kungsgatan 17$$Stockholm$11143$", None, None, None,
    ),
    (
        "5563333333", "ESEFLEI0000000000004", "Ideongatan 1, Lund.",
        None, "Ideongatan 1", "Lund", None,
    ),
    (
        "5564444444", "ESEFLEI0000000000005", "Lands vägen 57, Box 1264, 172 25 Sundbyberg",
        "Lands vägen 57, Box 1264$$Sundbyberg$17225$", None, None, None,
    ),
    (
        # A digit-led "town" would otherwise let the postcode land inside this glued digit
        # run (postcode 45103, city "62 stockholm"); the bounded town group instead finds
        # the postcode at "103 62" and stops the town at the letter-led "Stockholm".
        "5565555555", "ESEFLEI0000000000006", "Box 3145103 62 Stockholm",
        "Box 3145$$Stockholm$10362$", None, None, None,
    ),
    (
        # Trailing text after the town (a visiting-address clause) must not be swallowed into
        # the city.
        "5566666666", "ESEFLEI0000000000007", "Box 2269, 403 14 Göteborg Besöksadress: Östra Hamngatan 16",
        "Box 2269$$Göteborg$40314$", None, None, None,
    ),
    (
        # NBSP (U+00A0) between the postcode groups -- \s is ASCII-only in RE2, so this must
        # collapse to a plain space before the postcode regex runs.
        "5567777777", "ESEFLEI0000000000008", "Ingmar Bergmans Gata 4, 7 tr, 114\xa034 Stockholm",
        "Ingmar Bergmans Gata 4, 7 tr$$Stockholm$11434$", None, None, None,
    ),
)
ESEF_COMPANY_IDS: tuple[str, ...] = tuple(case[0] for case in ESEF_CASES)

# A company with two filings where the OLDER period_end carries the NEWER processed_at
# (finding 2 of the slice-3 review): proves esef_current_sql()'s argMax(processed_at,
# (period_end, processed_at)) stamps the same processed_at that esef_select_sql() picks for
# its chosen (newest period_end, then newest processed_at) filing. Under the old max(processed_at)
# formula the scan would see 2026-01-15 (the older filing's) while the select stamped
# 2025-05-10 (the newer filing's, the one actually chosen) -- an eternal mismatch that
# re-triggers this company on every run.
COMPANY_ESEF_REORDER = "5568888888"
ESEF_REORDER_LEI = "ESEFLEI0000000000009"
ESEF_REORDER_OLDER_ADDRESS = "Gamla vägen 1, 111 11 Stockholm"
ESEF_REORDER_NEWER_ADDRESS = "Nya vägen 2, 222 22 Göteborg"
ESEF_REORDER_EXPECTED_RAW_ADDRESS = "Nya vägen 2$$Göteborg$22222$"
ESEF_REORDER_EXPECTED_OBSERVED_AT = "2025-05-10 00:00:00.000"

# A company whose newest filing has a period_end after today() (2029-05-01 -- the same shape
# as the real bug on LEI 549300GU5OHTR1T5IY68, ruling B 2026-09-12). That filing must never
# win "the newest filing": both esef_current_sql()'s scope signature and esef_select_sql()'s
# picked row must fall back to the older filing (2024-12-31), the newest one that has
# actually happened.
COMPANY_ESEF_FUTURE_PERIOD = "5569999999"
ESEF_FUTURE_PERIOD_LEI = "ESEFLEI0000000000010"
ESEF_FUTURE_PERIOD_FUTURE_ADDRESS = "Framtidsvägen 1, 999 99 Framtidsstad"
ESEF_FUTURE_PERIOD_PAST_ADDRESS = "Gammalgatan 5, 111 22 Stockholm"
ESEF_FUTURE_PERIOD_EXPECTED_RAW_ADDRESS = "Gammalgatan 5$$Stockholm$11122$"
ESEF_FUTURE_PERIOD_EXPECTED_OBSERVED_AT = "2025-06-01 00:00:00.000"

RAW_ROW_CHECK_COLUMNS = (
    "company_id", "source", "slot", "kind", "raw_address", "care_of", "street_address",
    "postal_code", "post_town", "county", "extractor_version", "decided_by", "note",
    "replaces_key", "suggested_at",
)
RAW_ROWS_SQL = (
    "SELECT company_id, source, slot, kind, raw_address, care_of, street_address, postal_code, "
    "post_town, county, extractor_version, decided_by, note, replaces_key, toString(suggested_at) "
    f"FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL ORDER BY company_id, source"
)
# The same hash formula as ADDRESS_TRAILING_SELECT_SQL (suggestions.py): '\\n' in the Python
# source is a literal backslash-n in the rendered SQL text, which ClickHouse's string literal
# turns into an actual newline when concat() builds the hashed string -- so this reproduces
# exactly what stamped suggestion_id.
IDENTITY_CHECK_SQL = (
    f"SELECT count() FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL "
    "WHERE suggestion_id != lower(hex(SHA256(concat(company_id, '\\n', toString(source), '\\n', "
    "slot, '\\n', toString(suggested_at)))))"
)
SCB_AFTER_TOMBSTONE_SQL = (
    "SELECT street_address, postal_code, post_town, care_of, toString(suggested_at) "
    f"FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL "
    f"WHERE company_id = '{COMPANY_SCB_BV}' AND source = 'scb' AND slot = ''"
)
# ifNull(toString(...), 'NULL') instead of relying on the TSV \N marker: raw_address's packed
# rows and the components row's street_address/post_town are Nullable(String), and this way
# a missing value always prints the literal 'NULL' regardless of join_use_nulls.
ESEF_RAW_ROWS_SQL = (
    "SELECT company_id, ifNull(toString(raw_address), 'NULL'), ifNull(toString(street_address), 'NULL'), "
    "ifNull(toString(post_town), 'NULL'), ifNull(toString(country_code), 'NULL') "
    f"FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL WHERE source = 'esef' ORDER BY company_id"
)
ESEF_REORDER_ROW_SQL = (
    "SELECT raw_address, toString(observed_at) "
    f"FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL "
    f"WHERE company_id = '{COMPANY_ESEF_REORDER}' AND source = 'esef' AND slot = ''"
)
ESEF_FUTURE_PERIOD_ROW_SQL = (
    "SELECT raw_address, toString(observed_at) "
    f"FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL "
    f"WHERE company_id = '{COMPANY_ESEF_FUTURE_PERIOD}' AND source = 'esef' AND slot = ''"
)


def _schema() -> list[str]:
    tables_sql: list[str] = []
    views_sql: list[str] = []
    for name in MIGRATIONS:
        text = (MIGRATIONS_DIR / name).read_text(encoding="utf-8")
        for raw in text.split(";"):
            statement = "\n".join(
                line for line in raw.splitlines() if not line.strip().startswith("--")
            ).strip()
            if statement.upper().startswith(("CREATE DATABASE", "CREATE TABLE")):
                tables_sql.append(statement)
            elif statement.upper().startswith(("CREATE OR REPLACE VIEW", "CREATE VIEW")):
                # 000390's two INSERT INTO ... SELECT statements (data moves, not schema) are
                # excluded by construction -- they start with neither prefix above.
                views_sql.append(statement)
    fixture = [s.strip() for s in FIXTURE.read_text(encoding="utf-8").split(";") if s.strip()]
    esef_views = [
        build_se_esef_view_sql(view)
        for view in esef_tables.SE_ESEF_VIEWS
        if view.table in ("esef_facts", "esef_filings")
    ]
    # Views last: 000390's views read se_bolagsverket_companies/se_ratsit_company (created
    # above) and text_translations (fixture); the esef views read the fixture's esef_facts,
    # esef_filings and esef_entity_registry_map tables.
    return tables_sql + fixture + esef_views + views_sql


def _run(statements: list[str], *, join_use_nulls: int) -> list[str]:
    script = f"SET join_use_nulls = {join_use_nulls};\n" + ";\n".join(statements) + ";\n"
    completed = subprocess.run(clickhouse_local_command(), input=script, capture_output=True, text=True, timeout=900)
    assert completed.returncode == 0, completed.stderr or completed.stdout
    return [line for line in completed.stdout.splitlines() if line.strip()]


def _scope_sql(current_sql: str, source: str, **extra) -> str:
    return _bind(changed_scope_sql(current_sql=current_sql, target=ADDRESS_TARGET), source=source, **extra)


def _scope(current_sql: str, source: str, **extra) -> str:
    # The scope SQL is unpaged now (`scope_pages` runs it into a scratch table and pages
    # that), so the harness renders it alone and sorts for a stable printed order.
    return _scope_sql(current_sql, source, **extra) + "\nORDER BY company_id"


def _insert(select_sql: str, ids: list[str], *, extractor_version: str, **extra) -> str:
    return _bind(
        insert_page_sql(select_sql=select_sql, target=ADDRESS_TARGET),
        company_ids=ids, source_run_id="run-1", extractor_version=extractor_version, **extra,
    )


def _labelled(sql: str, label: str) -> str:
    """Tag a scope query's rows so several scopes combined in one UNION ALL stay tellable
    apart."""
    return f"SELECT '{label}', company_id FROM ({sql}) ORDER BY company_id"


def _ordered(sql: str, order_by: str) -> str:
    """Wrap a (possibly UNION ALL) statement so an ORDER BY unambiguously applies to it."""
    return f"SELECT * FROM ({sql}) AS ordered ORDER BY {order_by}"


def _as_row(fields: list[str]) -> tuple[str | None, ...]:
    """One changed_rows_sql() TSV line -> the tuple shape normalized_row() expects: \\N ->
    None, every other field kept as the string ClickHouse printed (including suggested_at)."""
    assert len(fields) == len(RAW_ROW_COLUMNS), fields
    return tuple(None if field == "\\N" else field for field in fields)


def _esef_seed_statements() -> list[str]:
    """One register-verified esef_entity_registry_map row (so the se_esef_* views expose the
    fact/filing under that company_id), one esef_filings row and one esef_facts row (concept
    AddressOfRegisteredOfficeOfEntity) per ESEF_CASES entry."""
    statements: list[str] = []
    for i, (company_id, lei, raw_value, *_expected) in enumerate(ESEF_CASES, start=1):
        fxo_id = f"fxo-{i}"
        package_sha256 = str(i) * 64
        statements.append(
            "INSERT INTO corpscout.esef_entity_registry_map "
            "(lei, country_iso2, registry_id_raw, registry_id, match_source, link_status, source_run_id) VALUES "
            f"('{lei}', 'SE', '{company_id}', '{company_id}', 'gleif_registered_as', 'register_verified', 'r')"
        )
        statements.append(
            "INSERT INTO corpscout.esef_filings (lei, entity_name, fxo_id, country, period_end, processed_at, "
            "package_sha256) VALUES "
            f"('{lei}', 'Esef Case {i} AB', '{fxo_id}', 'SE', toDate32('2025-12-31'), "
            f"toDateTime64('2026-09-0{i} 00:00:00', 6, 'UTC'), '{package_sha256}')"
        )
        statements.append(
            "INSERT INTO corpscout.esef_facts (lei, fxo_id, period_end, fact_id, concept_local_name, raw_value, "
            "language, processed_week) VALUES "
            f"('{lei}', '{fxo_id}', toDate32('2025-12-31'), 'fact-{i}', 'AddressOfRegisteredOfficeOfEntity', "
            f"'{raw_value}', 'sv', toDate('2026-09-01'))"
        )
    return statements


def _esef_reorder_seed_statements() -> list[str]:
    """COMPANY_ESEF_REORDER's two filings: the older period_end (2023-12-31) stamped with the
    NEWER processed_at (2026-01-15), the newer period_end (2024-12-31, the one esef_select_sql
    picks) stamped with the OLDER processed_at (2025-05-10) -- the inverted-clock case finding
    2 of the slice-3 review requires."""
    return [
        "INSERT INTO corpscout.esef_entity_registry_map "
        "(lei, country_iso2, registry_id_raw, registry_id, match_source, link_status, source_run_id) VALUES "
        f"('{ESEF_REORDER_LEI}', 'SE', '{COMPANY_ESEF_REORDER}', '{COMPANY_ESEF_REORDER}', "
        "'gleif_registered_as', 'register_verified', 'r')",
        "INSERT INTO corpscout.esef_filings (lei, entity_name, fxo_id, country, period_end, processed_at, "
        "package_sha256) VALUES "
        f"('{ESEF_REORDER_LEI}', 'Esef Reorder AB', 'fxo-reorder-older', 'SE', toDate32('2023-12-31'), "
        f"toDateTime64('2026-01-15 00:00:00', 6, 'UTC'), '{'b' * 64}')",
        "INSERT INTO corpscout.esef_filings (lei, entity_name, fxo_id, country, period_end, processed_at, "
        "package_sha256) VALUES "
        f"('{ESEF_REORDER_LEI}', 'Esef Reorder AB', 'fxo-reorder-newer', 'SE', toDate32('2024-12-31'), "
        f"toDateTime64('2025-05-10 00:00:00', 6, 'UTC'), '{'c' * 64}')",
        "INSERT INTO corpscout.esef_facts (lei, fxo_id, period_end, fact_id, concept_local_name, raw_value, "
        "language, processed_week) VALUES "
        f"('{ESEF_REORDER_LEI}', 'fxo-reorder-older', toDate32('2023-12-31'), 'fact-reorder-older', "
        f"'AddressOfRegisteredOfficeOfEntity', '{ESEF_REORDER_OLDER_ADDRESS}', 'sv', toDate('2026-09-01'))",
        "INSERT INTO corpscout.esef_facts (lei, fxo_id, period_end, fact_id, concept_local_name, raw_value, "
        "language, processed_week) VALUES "
        f"('{ESEF_REORDER_LEI}', 'fxo-reorder-newer', toDate32('2024-12-31'), 'fact-reorder-newer', "
        f"'AddressOfRegisteredOfficeOfEntity', '{ESEF_REORDER_NEWER_ADDRESS}', 'sv', toDate('2026-09-01'))",
    ]


def _esef_future_period_seed_statements() -> list[str]:
    """COMPANY_ESEF_FUTURE_PERIOD's two filings: a period_end after today() (2029-05-01) that
    must never win, and an older period_end (2024-12-31) that is the newest ELIGIBLE filing --
    ruling B's period_end <= today() guard on esef_current_sql()/esef_select_sql()."""
    return [
        "INSERT INTO corpscout.esef_entity_registry_map "
        "(lei, country_iso2, registry_id_raw, registry_id, match_source, link_status, source_run_id) VALUES "
        f"('{ESEF_FUTURE_PERIOD_LEI}', 'SE', '{COMPANY_ESEF_FUTURE_PERIOD}', '{COMPANY_ESEF_FUTURE_PERIOD}', "
        "'gleif_registered_as', 'register_verified', 'r')",
        "INSERT INTO corpscout.esef_filings (lei, entity_name, fxo_id, country, period_end, processed_at, "
        "package_sha256) VALUES "
        f"('{ESEF_FUTURE_PERIOD_LEI}', 'Esef Future Period AB', 'fxo-future-newer', 'SE', toDate32('2029-05-01'), "
        f"toDateTime64('2026-07-01 00:00:00', 6, 'UTC'), '{'e' * 64}')",
        "INSERT INTO corpscout.esef_filings (lei, entity_name, fxo_id, country, period_end, processed_at, "
        "package_sha256) VALUES "
        f"('{ESEF_FUTURE_PERIOD_LEI}', 'Esef Future Period AB', 'fxo-future-older', 'SE', toDate32('2024-12-31'), "
        f"toDateTime64('2025-06-01 00:00:00', 6, 'UTC'), '{'f' * 64}')",
        "INSERT INTO corpscout.esef_facts (lei, fxo_id, period_end, fact_id, concept_local_name, raw_value, "
        "language, processed_week) VALUES "
        f"('{ESEF_FUTURE_PERIOD_LEI}', 'fxo-future-newer', toDate32('2029-05-01'), 'fact-future-newer', "
        f"'AddressOfRegisteredOfficeOfEntity', '{ESEF_FUTURE_PERIOD_FUTURE_ADDRESS}', 'sv', toDate('2026-09-01'))",
        "INSERT INTO corpscout.esef_facts (lei, fxo_id, period_end, fact_id, concept_local_name, raw_value, "
        "language, processed_week) VALUES "
        f"('{ESEF_FUTURE_PERIOD_LEI}', 'fxo-future-older', toDate32('2024-12-31'), 'fact-future-older', "
        f"'AddressOfRegisteredOfficeOfEntity', '{ESEF_FUTURE_PERIOD_PAST_ADDRESS}', 'sv', toDate('2026-09-01'))",
    ]


def _sections(lines: list[str]) -> dict[str, list[list[str]]]:
    """Split _run()'s flat line list on the `SELECT '@@name'` markers in the script."""
    result: dict[str, list[list[str]]] = {}
    current = ""
    for line in lines:
        if line.startswith("@@"):
            current = line[2:]
            result[current] = []
        else:
            result[current].append(line.split("\t"))
    return result


def _statements() -> list[str]:
    scb_insert_1 = (
        "INSERT INTO corpscout.se_scb_companies (company_id, company_id_raw, street_address, postal_code, "
        "post_town, has_company, observed_at, source_run_id, source_record_id, source_payload_hash) VALUES "
        f"('{COMPANY_SCB_BV}', '{COMPANY_SCB_BV}', 'SICKLA INDUSTRIVÄG 19', '13134', 'NACKA', 1, "
        "toDateTime64('2026-09-01 00:00:00', 3, 'UTC'), '', 's1', 'h1')"
    )
    bv_insert = (
        "INSERT INTO corpscout.se_bolagsverket_companies (company_id, company_id_raw, postal_address, "
        "has_company, observed_at, source_run_id, source_record_id, source_payload_hash) VALUES "
        f"('{COMPANY_SCB_BV}', '{COMPANY_SCB_BV}', 'Box 5305$$STOCKHOLM$10247$SE-LAND', 1, "
        "toDateTime64('2026-09-02 00:00:00', 3, 'UTC'), '', 's2', 'h2')"
    )
    ratsit_insert = (
        "INSERT INTO corpscout.se_ratsit_company (company_id, result_sha256, normalizer_version, schema_version, "
        "parser_version, requested_url, source_url, result_bucket, result_object_key, name, organization_number, "
        "address_street, address_postal_code, address_locality, address_county, normalized_at) VALUES "
        f"('{COMPANY_RATSIT}', '{RATSIT_RESULT_SHA256}', '{RATSIT_NORMALIZER_VERSION}', 1, 'p', 'u', 'u', 'b', "
        "'k', 'Ratsit Company AB', '556012-5220', 'c/o Anna Svensson Storgatan 5', '11122', 'Stockholm', "
        "'Stockholms län', toDateTime64('2026-09-03 00:00:00', 6, 'UTC'))"
    )
    scb_tombstone = (
        "INSERT INTO corpscout.se_scb_companies (company_id, company_id_raw, has_company, observed_at, care_of, "
        "street_address, postal_code, post_town, source_run_id, source_record_id, source_payload_hash) VALUES "
        f"('{COMPANY_SCB_BV}', '{COMPANY_SCB_BV}', 0, toDateTime64('2026-09-05 00:00:00', 3, 'UTC'), "
        "NULL, NULL, NULL, NULL, '', '', '')"
    )
    reconverged = " UNION ALL ".join(
        f"({query})"
        for query in (
            _labelled(_scope_sql(scb.scb_current_sql(), "scb"), "scb"),
            _labelled(_scope_sql(bolagsverket.bolagsverket_current_sql(), "bolagsverket"), "bolagsverket"),
            _labelled(
                _scope_sql(ratsit.ratsit_current_sql(), "ratsit", **ratsit.RATSIT_ADDRESS_SELECT_PARAMS), "ratsit"
            ),
        )
    )
    changed_rows = _ordered(
        _bind(changed_rows_sql(), company_ids=[COMPANY_SCB_BV, COMPANY_RATSIT], normalizer_version=NORMALIZER_VERSION),
        "company_id, source, slot",
    )
    esef_changed_rows = _ordered(
        _bind(changed_rows_sql(), company_ids=list(ESEF_COMPANY_IDS), normalizer_version=NORMALIZER_VERSION),
        "company_id, source, slot",
    )
    return [
        *_schema(),
        scb_insert_1,
        bv_insert,
        ratsit_insert,
        "SELECT '@@scb_scope_1'",
        _scope(scb.scb_current_sql(), "scb"),
        "SELECT '@@bv_scope_1'",
        _scope(bolagsverket.bolagsverket_current_sql(), "bolagsverket"),
        "SELECT '@@ratsit_scope_1'",
        _scope(ratsit.ratsit_current_sql(), "ratsit", **ratsit.RATSIT_ADDRESS_SELECT_PARAMS),
        _insert(scb.scb_select_sql(), [COMPANY_SCB_BV], extractor_version=scb.SCB_ADDRESS_EXTRACTOR_VERSION),
        _insert(
            bolagsverket.bolagsverket_select_sql(), [COMPANY_SCB_BV],
            extractor_version=bolagsverket.BOLAGSVERKET_ADDRESS_EXTRACTOR_VERSION,
        ),
        _insert(
            ratsit.ratsit_select_sql(), [COMPANY_RATSIT],
            extractor_version=ratsit.RATSIT_ADDRESS_EXTRACTOR_VERSION, **ratsit.RATSIT_ADDRESS_SELECT_PARAMS,
        ),
        "SELECT '@@raw_rows'",
        RAW_ROWS_SQL,
        "SELECT '@@identity_check'",
        IDENTITY_CHECK_SQL,
        "SELECT '@@reconverged'",
        reconverged,
        # Force the gap past now64(3)'s millisecond resolution so this suggested_at cannot tie the earlier SCB insert's (same hazard test_se_company_basic_info_extractors_clickhouse_local.py notes).
        "SELECT sleep(0.01) FORMAT Null",
        scb_tombstone,
        "SELECT '@@scb_scope_3'",
        _scope(scb.scb_current_sql(), "scb"),
        _insert(scb.scb_select_sql(), [COMPANY_SCB_BV], extractor_version=scb.SCB_ADDRESS_EXTRACTOR_VERSION),
        "SELECT '@@scb_scope_after_tombstone'",
        _scope(scb.scb_current_sql(), "scb"),
        "SELECT '@@scb_after_tombstone'",
        SCB_AFTER_TOMBSTONE_SQL,
        "SELECT '@@changed_rows'",
        changed_rows,
        # The esef seed + insert run AFTER the changed_rows section above, on purpose: two of
        # its five company_ids alias COMPANY_SCB_BV/COMPANY_RATSIT, and changed_rows_sql()
        # filters on company_id alone (not source) -- seeding esef data any earlier would pull
        # esef rows into that section's company_ids=[COMPANY_SCB_BV, COMPANY_RATSIT] result and
        # break its len(rows) == 3 assertion.
        *_esef_seed_statements(),
        # COMPANY_ESEF_REORDER's two filings seed alongside the ESEF_CASES ones so it shows up
        # in the same before/after scope pair -- finding 2's argMax fix.
        *_esef_reorder_seed_statements(),
        # COMPANY_ESEF_FUTURE_PERIOD's two filings seed alongside them too -- ruling B's
        # period_end <= today() guard.
        *_esef_future_period_seed_statements(),
        "SELECT '@@esef_scope_before_insert'",
        _scope(esef.esef_current_sql(), "esef"),
        _insert(
            esef.esef_select_sql(),
            [*ESEF_COMPANY_IDS, COMPANY_ESEF_REORDER, COMPANY_ESEF_FUTURE_PERIOD],
            extractor_version=esef.ESEF_ADDRESS_EXTRACTOR_VERSION,
        ),
        "SELECT '@@esef_scope_after_insert'",
        _scope(esef.esef_current_sql(), "esef"),
        "SELECT '@@esef_raw_rows'",
        ESEF_RAW_ROWS_SQL,
        "SELECT '@@esef_reorder_row'",
        ESEF_REORDER_ROW_SQL,
        "SELECT '@@esef_future_period_row'",
        ESEF_FUTURE_PERIOD_ROW_SQL,
        "SELECT '@@esef_changed_rows'",
        esef_changed_rows,
    ]


@pytest.fixture(
    scope="module",
    params=(0, 1),
    ids=("join_use_nulls_off", "join_use_nulls_on"),
)
def sections(request: pytest.FixtureRequest) -> dict[str, list[list[str]]]:
    """Runs the whole script once per join_use_nulls setting (one clickhouse-local invocation
    each), shared across the test functions below rather than re-run per test."""
    return _sections(_run(_statements(), join_use_nulls=request.param))


def test_each_sources_scope_selects_its_company_before_anything_is_suggested(
    sections: dict[str, list[list[str]]],
) -> None:
    assert sections["scb_scope_1"] == [[COMPANY_SCB_BV]]
    assert sections["bv_scope_1"] == [[COMPANY_SCB_BV]]
    assert sections["ratsit_scope_1"] == [[COMPANY_RATSIT]]


def test_raw_rows_hold_each_sources_delivered_columns(sections: dict[str, list[list[str]]]) -> None:
    rows = [dict(zip(RAW_ROW_CHECK_COLUMNS, fields, strict=True)) for fields in sections["raw_rows"]]
    assert len(rows) == 3
    by_key = {(row["company_id"], row["source"]): row for row in rows}

    scb_row = by_key[(COMPANY_SCB_BV, "scb")]
    assert scb_row["slot"] == ""
    assert scb_row["kind"] == "visiting_or_postal"
    assert scb_row["raw_address"] == "\\N"
    assert scb_row["street_address"] == "SICKLA INDUSTRIVÄG 19"
    assert scb_row["postal_code"] == "13134"
    assert scb_row["post_town"] == "NACKA"
    assert scb_row["extractor_version"] == scb.SCB_ADDRESS_EXTRACTOR_VERSION
    assert scb_row["decided_by"] == scb_row["note"] == scb_row["replaces_key"] == "\\N"

    bv_row = by_key[(COMPANY_SCB_BV, "bolagsverket")]
    assert bv_row["slot"] == ""
    assert bv_row["kind"] == "postal"
    assert bv_row["raw_address"] == "Box 5305$$STOCKHOLM$10247$SE-LAND"
    assert bv_row["street_address"] == "\\N"
    assert bv_row["extractor_version"] == bolagsverket.BOLAGSVERKET_ADDRESS_EXTRACTOR_VERSION
    assert bv_row["decided_by"] == bv_row["note"] == bv_row["replaces_key"] == "\\N"

    ratsit_row = by_key[(COMPANY_RATSIT, "ratsit")]
    assert ratsit_row["slot"] == "company"
    assert ratsit_row["street_address"] == "c/o Anna Svensson Storgatan 5"
    assert ratsit_row["county"] == "Stockholms län"
    assert ratsit_row["extractor_version"] == ratsit.RATSIT_ADDRESS_EXTRACTOR_VERSION
    assert ratsit_row["decided_by"] == ratsit_row["note"] == ratsit_row["replaces_key"] == "\\N"


def test_suggestion_id_matches_the_stamp_hash_for_every_row(sections: dict[str, list[list[str]]]) -> None:
    assert sections["identity_check"] == [["0"]]


def test_all_four_scopes_converge_after_insert(sections: dict[str, list[list[str]]]) -> None:
    # scb/bolagsverket/ratsit converge through the shared `reconverged` UNION ALL (computed
    # right after their own inserts); esef converges on its own scope_before/scope_after pair,
    # computed later in the script (after the pre-existing changed_rows section -- see the
    # comment in _statements() for why). COMPANY_ESEF_REORDER and COMPANY_ESEF_FUTURE_PERIOD
    # are seeded alongside the ESEF_CASES companies (see _esef_reorder_seed_statements() and
    # _esef_future_period_seed_statements()), so both are expected in both scopes.
    assert sections["reconverged"] == []
    assert sections["esef_scope_before_insert"] == [
        [company_id]
        for company_id in sorted(
            (*ESEF_COMPANY_IDS, COMPANY_ESEF_REORDER, COMPANY_ESEF_FUTURE_PERIOD)
        )
    ]
    assert sections["esef_scope_after_insert"] == []


def test_esef_reorder_company_stamps_the_selected_filings_observed_at(
    sections: dict[str, list[list[str]]],
) -> None:
    """Finding 2: esef_current_sql()'s argMax(processed_at, (period_end, processed_at)) must
    equal esef_select_sql()'s stamped observed_at for the CHOSEN (newest period_end) filing,
    even when that filing's own processed_at is not the largest of the company's filings.
    test_all_four_scopes_converge_after_insert already proves the company drops out of scope
    after the insert (the two stamps agree); this proves which filing's content won."""
    assert len(sections["esef_reorder_row"]) == 1
    raw_address, observed_at = sections["esef_reorder_row"][0]
    assert raw_address == ESEF_REORDER_EXPECTED_RAW_ADDRESS
    assert observed_at == ESEF_REORDER_EXPECTED_OBSERVED_AT


def test_scb_tombstone_reselects_and_writes_a_null_raw_row(sections: dict[str, list[list[str]]]) -> None:
    first = dict(
        zip(
            RAW_ROW_CHECK_COLUMNS,
            next(fields for fields in sections["raw_rows"] if fields[0] == COMPANY_SCB_BV and fields[1] == "scb"),
            strict=True,
        )
    )
    assert sections["scb_scope_3"] == [[COMPANY_SCB_BV]]
    after = dict(
        zip(("street_address", "postal_code", "post_town", "care_of", "suggested_at"),
            sections["scb_after_tombstone"][0], strict=True)
    )
    assert after["street_address"] == after["postal_code"] == after["post_town"] == after["care_of"] == "\\N"
    assert after["suggested_at"] > first["suggested_at"]


def test_scb_scope_reconverges_after_the_tombstone_reselect(sections: dict[str, list[list[str]]]) -> None:
    assert sections["scb_scope_after_tombstone"] == []


def test_normalize_hand_off_gives_the_expected_parse_statuses(sections: dict[str, list[list[str]]]) -> None:
    rows = [_as_row(fields) for fields in sections["changed_rows"]]
    assert len(rows) == 3
    by_source = {row[1]: row for row in rows}

    scb_result = dict(zip(tables.NORMALIZED_COLUMNS, normalized_row(by_source["scb"], STAMP), strict=True))
    assert scb_result["parse_status"] == "no_address"

    bv_result = dict(zip(tables.NORMALIZED_COLUMNS, normalized_row(by_source["bolagsverket"], STAMP), strict=True))
    assert bv_result["parse_status"] == "ok"
    assert bv_result["box"] == "5305"

    ratsit_result = dict(zip(tables.NORMALIZED_COLUMNS, normalized_row(by_source["ratsit"], STAMP), strict=True))
    assert ratsit_result["parse_status"] == "ok"
    assert ratsit_result["care_of"] == "anna svensson"
    assert ratsit_result["street_name"] == "storgatan"
    assert ratsit_result["house_number"] == "5"


def test_esef_raw_rows_hold_the_cleaned_and_repacked_address(sections: dict[str, list[list[str]]]) -> None:
    # source = 'esef' also carries COMPANY_ESEF_REORDER's row (checked separately by
    # test_esef_reorder_company_stamps_the_selected_filings_observed_at) and
    # COMPANY_ESEF_FUTURE_PERIOD's row (checked by
    # test_esef_future_period_company_ignores_the_filing_after_today), so this counts and
    # indexes only the ESEF_CASES ids.
    rows = {fields[0]: tuple(fields[1:]) for fields in sections["esef_raw_rows"]}
    assert len(rows) == len(ESEF_CASES) + 2
    for company_id, _lei, _raw_value, raw_address, street_address, post_town, country_code in ESEF_CASES:
        expected = tuple(value if value is not None else "NULL" for value in (raw_address, street_address, post_town, country_code))
        assert rows[company_id] == expected, company_id


def test_esef_future_period_company_ignores_the_filing_after_today(
    sections: dict[str, list[list[str]]],
) -> None:
    """Ruling B (2026-09-12): a filing with a period_end after today() (2029-05-01) must
    never win a "latest filing" choice. esef_current_sql()'s scope signature and
    esef_select_sql()'s picked row must both fall back to the newest ELIGIBLE filing
    (2024-12-31) -- test_all_four_scopes_converge_after_insert already proves the company
    drops out of scope after the insert (the two guarded stamps agree); this proves which
    filing's content won."""
    assert len(sections["esef_future_period_row"]) == 1
    raw_address, observed_at = sections["esef_future_period_row"][0]
    assert raw_address == ESEF_FUTURE_PERIOD_EXPECTED_RAW_ADDRESS
    assert observed_at == ESEF_FUTURE_PERIOD_EXPECTED_OBSERVED_AT


def test_esef_normalize_hand_off_gives_the_expected_parse_statuses(sections: dict[str, list[list[str]]]) -> None:
    rows = [_as_row(fields) for fields in sections["esef_changed_rows"]]
    # esef_changed_rows's company_ids alias COMPANY_RATSIT/COMPANY_SCB_BV (see ESEF_CASES), so
    # this also carries their pre-existing ratsit/scb/bolagsverket rows -- filtered out here.
    by_company = {row[0]: row for row in rows if row[1] == "esef"}
    assert len(by_company) == len(ESEF_CASES)

    for company_id, *_rest in ESEF_CASES:
        result = dict(zip(tables.NORMALIZED_COLUMNS, normalized_row(by_company[company_id], STAMP), strict=True))
        if company_id == "5563333333":
            # "Ideongatan 1, Lund." carries no postcode, so ESEF_PACKED_ADDRESS_SQL delivers
            # bare components (street_address, post_town) instead of a packed raw_address;
            # normalize_se_address finds a street but no postcode and returns `partial` (its
            # "missing postcode" branch), not `no_address` -- a street/town pair is a real,
            # if coarse, address.
            assert result["parse_status"] == "partial"
        else:
            assert result["parse_status"] == "ok"
