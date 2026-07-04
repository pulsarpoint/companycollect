import json
import zipfile
from io import BytesIO
from pathlib import Path

import duckdb
import pytest

from dagster_v3.defs.sweden_company import tables
from dagster_v3.defs.sweden_company import raw_duckdb
from dagster_v3.defs.sweden_company.raw_duckdb import load_sweden_company_raw_manifest
from dagster_v3.defs.sweden_company.resources import SWEDEN_COMPANY_RAW_BUCKET


class FakeObjectStore:
    def __init__(self, objects: dict[tuple[str, str], bytes]) -> None:
        self.objects = objects
        self.downloaded_keys: list[tuple[str, str]] = []

    def download_file(
        self, key: str, target_path: str | Path, bucket: str | None = None
    ) -> None:
        assert bucket is not None
        self.downloaded_keys.append((bucket, key))
        Path(target_path).write_bytes(self.objects[(bucket, key)])


def test_load_sweden_company_raw_manifest_creates_raw_duckdb_tables(tmp_path: Path) -> None:
    bolagsverket_key = (
        "sweden_company/raw/source_last_modified=2026-06-29T01-27-14Z/"
        "source=bolagsverket_bulkfil/source.zip"
    )
    scb_key = (
        "sweden_company/raw/source_last_modified=2026-06-29T13-04-12Z/"
        "source=scb_bulkfil/source.zip"
    )
    object_store = FakeObjectStore(
        {
            (SWEDEN_COMPANY_RAW_BUCKET, bolagsverket_key): _zip_bytes(
                "bolagsverket_bulkfil.txt",
                (
                    "organisationsidentitet;namnskyddslopnummer;registreringsland;"
                    "organisationsnamn;organisationsform;avregistreringsdatum;"
                    "avregistreringsorsak;pagandeAvvecklingsEllerOmstruktureringsforfarande;"
                    "registreringsdatum;verksamhetsbeskrivning;postadress\n"
                    '"5560000000$ORGNR-IDORG";"1";"SE-LAND";'
                    '"Acme AB$FORETAGSNAMN-ORGNAM$2020-01-01";"AB-ORGFO";"";"";"";'
                    '"2020-01-01";"Runs acme.se";"Box 1$$STOCKHOLM$11122$SE-LAND"\n'
                ).encode("utf-8"),
            ),
            (SWEDEN_COMPANY_RAW_BUCKET, scb_key): _zip_bytes(
                "scb_bulkfil_JE_20260629T055245_80.txt",
                (
                    "ForAndrTyp\tCOAdress\tForetagsnamn\tFtgStat\tGatuadress\tJEStat\t"
                    "JurForm\tNamn\tNg1\tNg2\tNg3\tNg4\tNg5\tPeOrgNr\tPostNr\tPostOrt\t"
                    "RegDatKtid\tReklamsparrtyp\tmCOAdress\tmForetagsnamn\tmFtgStat\t"
                    "mGatuadress\tmJEStat\tmJurForm\tmNamn\tmNg1\tmNg2\tmNg3\tmNg4\t"
                    "mNg5\tmPostNr\tmPostOrt\tmRegDatKtid\tmReklamsparrtyp\t\r\n"
                    "1\tc/o ACME\t\t0\tMain Street 1\t1\t49\tACME SCB\t62010\t\t\t\t\t"
                    "5560000000\t11122\tSTOCKHOLM\t20200101\t1\t1\t1\t1\t1\t1\t1\t"
                    "1\t1\t1\t1\t1\t1\t1\t1\t1\t\r\n"
                ).encode("latin-1"),
            ),
        }
    )
    manifest = {
        "source": "sweden_company",
        "run_id": "run-1",
        "retrieved_date": "2026-07-03",
        "files": [
            {
                "source_slug": "bolagsverket_bulkfil",
                "source_url": "https://example.test/bolagsverket.zip",
                "s3_key": bolagsverket_key,
                "source_last_modified": "2026-06-29T01-27-14Z",
                "size_bytes": 100,
                "sha256": "bolag-sha",
            },
            {
                "source_slug": "scb_bulkfil",
                "source_url": "https://example.test/scb.zip",
                "s3_key": scb_key,
                "source_last_modified": "2026-06-29T13-04-12Z",
                "size_bytes": 200,
                "sha256": "scb-sha",
            },
        ],
    }

    with duckdb.connect(str(tmp_path / "sweden_company_source.duckdb")) as connection:
        counts = load_sweden_company_raw_manifest(
            connection=connection,
            object_store=object_store,
            manifest=manifest,
            source_run_id="run-1",
        )
        raw_files = connection.execute(
            f"select source_slug, s3_key, sha256 from {tables.DLT_DATASET_NAME}.raw_files "
            "order by source_slug"
        ).fetchall()
        bolag = connection.execute(
            f"select source_run_id, source_line_number, source_record_id, organisationsnamn, raw_record "
            f"from {tables.DLT_DATASET_NAME}.bolagsverket_raw"
        ).fetchone()
        scb = connection.execute(
            f"select source_run_id, source_line_number, source_record_id, Namn, Ng1 "
            f"from {tables.DLT_DATASET_NAME}.scb_raw"
        ).fetchone()
        scb_columns = [
            row[1]
            for row in connection.execute(
                f"pragma table_info('{tables.DLT_DATASET_NAME}.scb_raw')"
            ).fetchall()
        ]

    assert counts == {
        "raw_files": 2,
        "bolagsverket_raw": 1,
        "scb_raw": 1,
    }
    assert raw_files == [
        ("bolagsverket_bulkfil", bolagsverket_key, "bolag-sha"),
        ("scb_bulkfil", scb_key, "scb-sha"),
    ]
    assert bolag[:4] == (
        "run-1",
        1,
        "5560000000$ORGNR-IDORG",
        "Acme AB$FORETAGSNAMN-ORGNAM$2020-01-01",
    )
    assert json.loads(bolag[4])["verksamhetsbeskrivning"] == "Runs acme.se"
    assert scb == ("run-1", 1, "5560000000", "ACME SCB", "62010")
    assert "column34" not in scb_columns


def test_load_sweden_company_raw_manifest_streams_scb_latin1_transcode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bolagsverket_key = (
        "sweden_company/raw/source_last_modified=2026-06-29T01-27-14Z/"
        "source=bolagsverket_bulkfil/source.zip"
    )
    scb_key = (
        "sweden_company/raw/source_last_modified=2026-06-29T13-04-12Z/"
        "source=scb_bulkfil/source.zip"
    )
    object_store = FakeObjectStore(
        {
            (SWEDEN_COMPANY_RAW_BUCKET, bolagsverket_key): _zip_bytes(
                "bolagsverket_bulkfil.txt",
                (
                    "organisationsidentitet;namnskyddslopnummer;registreringsland;"
                    "organisationsnamn;organisationsform;avregistreringsdatum;"
                    "avregistreringsorsak;pagandeAvvecklingsEllerOmstruktureringsforfarande;"
                    "registreringsdatum;verksamhetsbeskrivning;postadress\n"
                    '"5560000000$ORGNR-IDORG";"1";"SE-LAND";'
                    '"Acme AB$FORETAGSNAMN-ORGNAM$2020-01-01";"AB-ORGFO";"";"";"";'
                    '"2020-01-01";"Runs acme.se";"Box 1$$STOCKHOLM$11122$SE-LAND"\n'
                ).encode("utf-8"),
            ),
            (SWEDEN_COMPANY_RAW_BUCKET, scb_key): _zip_bytes(
                "scb_bulkfil_JE_20260629T055245_80.txt",
                (
                    "ForAndrTyp\tCOAdress\tForetagsnamn\tFtgStat\tGatuadress\tJEStat\t"
                    "JurForm\tNamn\tNg1\tNg2\tNg3\tNg4\tNg5\tPeOrgNr\tPostNr\tPostOrt\t"
                    "RegDatKtid\tReklamsparrtyp\tmCOAdress\tmForetagsnamn\tmFtgStat\t"
                    "mGatuadress\tmJEStat\tmJurForm\tmNamn\tmNg1\tmNg2\tmNg3\tmNg4\t"
                    "mNg5\tmPostNr\tmPostOrt\tmRegDatKtid\tmReklamsparrtyp\t\r\n"
                    "1\tc/o ACME\t\t0\tMain Street 1\t1\t49\tÅÄÖ SCB\t62010\t\t\t\t\t"
                    "5560000000\t11122\tSTOCKHOLM\t20200101\t1\t1\t1\t1\t1\t1\t1\t"
                    "1\t1\t1\t1\t1\t1\t1\t1\t1\t\r\n"
                ).encode("latin-1"),
            ),
        }
    )
    manifest = {
        "source": "sweden_company",
        "run_id": "run-1",
        "retrieved_date": "2026-07-03",
        "files": [
            {
                "source_slug": "bolagsverket_bulkfil",
                "source_url": "https://example.test/bolagsverket.zip",
                "s3_key": bolagsverket_key,
                "source_last_modified": "2026-06-29T01-27-14Z",
                "size_bytes": 100,
                "sha256": "bolag-sha",
            },
            {
                "source_slug": "scb_bulkfil",
                "source_url": "https://example.test/scb.zip",
                "s3_key": scb_key,
                "source_last_modified": "2026-06-29T13-04-12Z",
                "size_bytes": 200,
                "sha256": "scb-sha",
            },
        ],
    }

    def fail_read_text(*args: object, **kwargs: object) -> str:
        raise AssertionError("SCB transcode must not read the whole file into memory")

    monkeypatch.setattr(Path, "read_text", fail_read_text)

    with duckdb.connect(str(tmp_path / "sweden_company_source.duckdb")) as connection:
        counts = load_sweden_company_raw_manifest(
            connection=connection,
            object_store=object_store,
            manifest=manifest,
            source_run_id="run-1",
        )
        scb = connection.execute(
            f"select source_record_id, Namn "
            f"from {tables.DLT_DATASET_NAME}.scb_raw"
        ).fetchone()

    assert counts == {
        "raw_files": 2,
        "bolagsverket_raw": 1,
        "scb_raw": 1,
    }
    assert scb == ("5560000000", "ÅÄÖ SCB")


def test_scb_raw_loader_disables_parallel_csv_scan_with_null_padding(
    tmp_path: Path,
) -> None:
    scb_path = tmp_path / "scb_bulkfil.txt"
    scb_path.write_text("PeOrgNr\tNamn\r\n5560000000\tACME\r\n", encoding="latin-1")
    executed_sql: list[str] = []

    class CapturingConnection:
        def execute(self, sql: str, params: list[str]) -> None:
            executed_sql.append(sql)

    raw_duckdb._replace_scb_raw_table(
        connection=CapturingConnection(),
        csv_path=scb_path,
        source_run_id="run-1",
        source_s3_key="raw/scb.zip",
    )

    assert "null_padding=true" in executed_sql[0]
    assert "parallel=false" in executed_sql[0]


def test_scb_raw_loader_treats_quote_as_literal_data(tmp_path: Path) -> None:
    scb_path = tmp_path / "scb_bulkfil.txt"
    header = "\t".join(tables.SCB_SOURCE_COLUMNS)
    values_by_column = {
        column: "1"
        for column in tables.SCB_SOURCE_COLUMNS
    }
    values_by_column["PeOrgNr"] = "168024131248"
    values_by_column["Namn"] = '"(S-) FÖRENINGEN I AUGUSTENDAL'
    row = "\t".join(values_by_column[column] for column in tables.SCB_SOURCE_COLUMNS)
    scb_path.write_text(f"{header}\r\n{row}\r\n", encoding="latin-1", newline="")

    with duckdb.connect(str(tmp_path / "sweden_company_source.duckdb")) as connection:
        connection.execute(f"create schema {tables.DLT_DATASET_NAME}")

        raw_duckdb._replace_scb_raw_table(
            connection=connection,
            csv_path=scb_path,
            source_run_id="run-1",
            source_s3_key="raw/scb.zip",
        )

        scb = connection.execute(
            f"""
            select source_record_id, Namn
            from {tables.DLT_DATASET_NAME}.scb_raw
            """
        ).fetchone()

    assert scb == ("168024131248", '"(S-) FÖRENINGEN I AUGUSTENDAL')


def test_load_sweden_company_raw_manifest_rejects_partial_manifest_before_replacing_raw_files(
    tmp_path: Path,
) -> None:
    bolagsverket_key = (
        "sweden_company/raw/source_last_modified=2026-06-29T01-27-14Z/"
        "source=bolagsverket_bulkfil/source.zip"
    )
    object_store = FakeObjectStore({})
    manifest = {
        "source": "sweden_company",
        "run_id": "run-1",
        "retrieved_date": "2026-07-03",
        "files": [
            {
                "source_slug": "bolagsverket_bulkfil",
                "source_url": "https://example.test/bolagsverket.zip",
                "s3_key": bolagsverket_key,
                "source_last_modified": "2026-06-29T01-27-14Z",
                "size_bytes": 100,
                "sha256": "bolag-sha",
            },
        ],
    }

    with duckdb.connect(str(tmp_path / "sweden_company_source.duckdb")) as connection:
        connection.execute(f"create schema {tables.DLT_DATASET_NAME}")
        connection.execute(
            f"""
            create table {tables.DLT_DATASET_NAME}.raw_files (
                source_slug varchar,
                s3_key varchar
            )
            """
        )
        connection.execute(
            f"""
            insert into {tables.DLT_DATASET_NAME}.raw_files
            values ('preexisting', 'old-key')
            """
        )

        with pytest.raises(
            ValueError,
            match="missing source slug\\(s\\): scb_bulkfil",
        ):
            load_sweden_company_raw_manifest(
                connection=connection,
                object_store=object_store,
                manifest=manifest,
                source_run_id="run-1",
            )

        raw_files = connection.execute(
            f"""
            select source_slug, s3_key
            from {tables.DLT_DATASET_NAME}.raw_files
            """
        ).fetchall()

    assert raw_files == [("preexisting", "old-key")]


def test_load_sweden_company_raw_manifest_rejects_duplicate_manifest_source_slugs(
    tmp_path: Path,
) -> None:
    bolagsverket_key = (
        "sweden_company/raw/source_last_modified=2026-06-29T01-27-14Z/"
        "source=bolagsverket_bulkfil/source.zip"
    )
    scb_key = (
        "sweden_company/raw/source_last_modified=2026-06-29T13-04-12Z/"
        "source=scb_bulkfil/source.zip"
    )
    object_store = FakeObjectStore({})
    manifest = {
        "source": "sweden_company",
        "run_id": "run-1",
        "retrieved_date": "2026-07-03",
        "files": [
            {
                "source_slug": "bolagsverket_bulkfil",
                "source_url": "https://example.test/bolagsverket.zip",
                "s3_key": bolagsverket_key,
                "source_last_modified": "2026-06-29T01-27-14Z",
                "size_bytes": 100,
                "sha256": "bolag-sha",
            },
            {
                "source_slug": "bolagsverket_bulkfil",
                "source_url": "https://example.test/bolagsverket-copy.zip",
                "s3_key": bolagsverket_key,
                "source_last_modified": "2026-06-29T01-27-14Z",
                "size_bytes": 100,
                "sha256": "bolag-sha",
            },
            {
                "source_slug": "scb_bulkfil",
                "source_url": "https://example.test/scb.zip",
                "s3_key": scb_key,
                "source_last_modified": "2026-06-29T13-04-12Z",
                "size_bytes": 200,
                "sha256": "scb-sha",
            },
        ],
    }

    with duckdb.connect(str(tmp_path / "sweden_company_source.duckdb")) as connection:
        connection.execute(f"create schema {tables.DLT_DATASET_NAME}")
        connection.execute(
            f"""
            create table {tables.DLT_DATASET_NAME}.raw_files (
                source_slug varchar,
                s3_key varchar
            )
            """
        )
        connection.execute(
            f"""
            insert into {tables.DLT_DATASET_NAME}.raw_files
            values ('preexisting', 'old-key')
            """
        )

        with pytest.raises(
            ValueError,
            match="duplicate source slug\\(s\\): bolagsverket_bulkfil",
        ):
            load_sweden_company_raw_manifest(
                connection=connection,
                object_store=object_store,
                manifest=manifest,
                source_run_id="run-1",
            )

        raw_files = connection.execute(
            f"""
            select source_slug, s3_key
            from {tables.DLT_DATASET_NAME}.raw_files
            """
        ).fetchall()

    assert raw_files == [("preexisting", "old-key")]


def test_load_sweden_company_raw_manifest_rolls_back_partial_rebuild(
    tmp_path: Path,
) -> None:
    bolagsverket_key = (
        "sweden_company/raw/source_last_modified=2026-06-29T01-27-14Z/"
        "source=bolagsverket_bulkfil/source.zip"
    )
    scb_key = (
        "sweden_company/raw/source_last_modified=2026-06-29T13-04-12Z/"
        "source=scb_bulkfil/source.zip"
    )
    object_store = FakeObjectStore(
        {
            (SWEDEN_COMPANY_RAW_BUCKET, bolagsverket_key): _zip_bytes(
                "bolagsverket_bulkfil.txt",
                (
                    "organisationsidentitet;namnskyddslopnummer;registreringsland;"
                    "organisationsnamn;organisationsform;avregistreringsdatum;"
                    "avregistreringsorsak;pagandeAvvecklingsEllerOmstruktureringsforfarande;"
                    "registreringsdatum;verksamhetsbeskrivning;postadress\n"
                    '"5560000000$ORGNR-IDORG";"1";"SE-LAND";'
                    '"Acme AB$FORETAGSNAMN-ORGNAM$2020-01-01";"AB-ORGFO";"";"";"";'
                    '"2020-01-01";"Runs acme.se";"Box 1$$STOCKHOLM$11122$SE-LAND"\n'
                ).encode("utf-8"),
            ),
            (SWEDEN_COMPANY_RAW_BUCKET, scb_key): _zip_bytes_with_members(
                {
                    "scb-1.txt": b"scb 1\n",
                    "scb-2.txt": b"scb 2\n",
                }
            ),
        }
    )
    manifest = {
        "source": "sweden_company",
        "run_id": "run-1",
        "retrieved_date": "2026-07-03",
        "files": [
            {
                "source_slug": "bolagsverket_bulkfil",
                "source_url": "https://example.test/bolagsverket.zip",
                "s3_key": bolagsverket_key,
                "source_last_modified": "2026-06-29T01-27-14Z",
                "size_bytes": 100,
                "sha256": "bolag-sha",
            },
            {
                "source_slug": "scb_bulkfil",
                "source_url": "https://example.test/scb.zip",
                "s3_key": scb_key,
                "source_last_modified": "2026-06-29T13-04-12Z",
                "size_bytes": 200,
                "sha256": "scb-sha",
            },
        ],
    }

    with duckdb.connect(str(tmp_path / "sweden_company_source.duckdb")) as connection:
        connection.execute(f"create schema {tables.DLT_DATASET_NAME}")
        connection.execute(
            f"""
            create table {tables.DLT_DATASET_NAME}.raw_files (
                source_slug varchar,
                s3_key varchar
            )
            """
        )
        connection.execute(
            f"""
            insert into {tables.DLT_DATASET_NAME}.raw_files
            values ('preexisting', 'old-key')
            """
        )
        connection.execute(
            f"""
            create table {tables.DLT_DATASET_NAME}.bolagsverket_raw (
                source_record_id varchar,
                organisationsnamn varchar
            )
            """
        )
        connection.execute(
            f"""
            insert into {tables.DLT_DATASET_NAME}.bolagsverket_raw
            values ('old-id', 'Old Company')
            """
        )

        with pytest.raises(
            ValueError,
            match="Expected one file in scb_bulkfil\\.zip",
        ):
            load_sweden_company_raw_manifest(
                connection=connection,
                object_store=object_store,
                manifest=manifest,
                source_run_id="run-1",
            )

        raw_files = connection.execute(
            f"""
            select source_slug, s3_key
            from {tables.DLT_DATASET_NAME}.raw_files
            """
        ).fetchall()
        bolagsverket = connection.execute(
            f"""
            select source_record_id, organisationsnamn
            from {tables.DLT_DATASET_NAME}.bolagsverket_raw
            """
        ).fetchall()

    assert raw_files == [("preexisting", "old-key")]
    assert bolagsverket == [("old-id", "Old Company")]


def test_extract_single_member_streams_zip_member(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    zip_path = tmp_path / "source.zip"
    zip_path.write_bytes(_zip_bytes("source.txt", b"source body\n"))
    calls = 0

    def copy_in_chunks(source: object, target: object) -> None:
        nonlocal calls
        calls += 1
        while chunk := source.read(3):
            target.write(chunk)

    monkeypatch.setattr(raw_duckdb, "copyfileobj", copy_in_chunks)

    output_path = raw_duckdb._extract_single_member(
        zip_path=zip_path,
        output_dir=tmp_path,
    )

    assert calls == 1
    assert output_path.read_bytes() == b"source body\n"


def _zip_bytes(name: str, body: bytes) -> bytes:
    return _zip_bytes_with_members({name: body})


def _zip_bytes_with_members(members: dict[str, bytes]) -> bytes:
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, body in members.items():
            archive.writestr(name, body)
    return buffer.getvalue()
