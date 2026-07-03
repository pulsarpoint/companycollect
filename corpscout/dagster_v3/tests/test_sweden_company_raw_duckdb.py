import json
import zipfile
from io import BytesIO
from pathlib import Path

import duckdb

from dagster_v3.defs.sweden_company import tables
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
            f"select source_slug, s3_key, sha256 from {tables.DLT_DATASET_NAME}.{tables.RAW_FILES_TABLE} "
            "order by source_slug"
        ).fetchall()
        bolag = connection.execute(
            f"select source_run_id, source_line_number, source_record_id, organisationsnamn, raw_record "
            f"from {tables.DLT_DATASET_NAME}.{tables.BOLAGSVERKET_RAW_TABLE}"
        ).fetchone()
        scb = connection.execute(
            f"select source_run_id, source_line_number, source_record_id, Namn, Ng1 "
            f"from {tables.DLT_DATASET_NAME}.{tables.SCB_RAW_TABLE}"
        ).fetchone()
        scb_columns = [
            row[1]
            for row in connection.execute(
                f"pragma table_info('{tables.DLT_DATASET_NAME}.{tables.SCB_RAW_TABLE}')"
            ).fetchall()
        ]

    assert counts == {
        tables.RAW_FILES_TABLE: 2,
        tables.BOLAGSVERKET_RAW_TABLE: 1,
        tables.SCB_RAW_TABLE: 1,
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


def _zip_bytes(name: str, body: bytes) -> bytes:
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(name, body)
    return buffer.getvalue()
