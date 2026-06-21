import io
import json
import zipfile

import duckdb

from dagster_v3.defs.estonia_ar import general_data, tables

SAMPLE_RECORDS = [
    {
        "ariregistri_kood": 16752073,
        "yldandmed": {
            "sidevahendid": [
                {"liik": "WWW", "sisu": "https://acme.ee", "lopp_kpv": None},
                {"liik": "EMAIL", "sisu": "info@acme.ee", "lopp_kpv": None},
            ],
            "teatatud_tegevusalad": [
                {"emtak_kood": "73111", "emtak_tekstina": "Reklaam", "emtak_versioon_tekstina": "EMTAK 2025", "nace_kood": "73.11", "on_pohitegevusala": True, "lopp_kpv": None},
            ],
        },
    },
]


def _zip_bytes(records: list) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(
            "ettevotja_rekvisiidid__yldandmed.json", json.dumps(records).encode("utf-8")
        )
    return buf.getvalue()


class _FakeResponse:
    def __init__(self, body: bytes) -> None:
        self.content = body
        self.headers = {"Content-Length": str(len(body))}

    def raise_for_status(self) -> None:
        return None

    def iter_content(self, chunk_size: int = 0):
        yield self.content


class _FakeSession:
    def __init__(self, body: bytes) -> None:
        self._body = body
        self.calls = 0

    def get(self, url: str, *, timeout: int, stream: bool = False) -> _FakeResponse:
        self.calls += 1
        return _FakeResponse(self._body)


def test_one_download_builds_contacts_and_industries(tmp_path):
    db = tmp_path / "estonia_ar_source.duckdb"
    session = _FakeSession(_zip_bytes(SAMPLE_RECORDS))

    counts = general_data.build_estonia_ar_general_data(
        database_path=db, source_run_id="run-1", session=session
    )

    # ONE download fed both tables.
    assert session.calls == 1
    assert counts["contacts"] == 2  # WWW + EMAIL
    assert counts["industries"] == 1

    with duckdb.connect(str(db), read_only=True) as con:
        contacts = con.execute(
            f"select count(*) from {tables.DLT_DATASET_NAME}.{tables.GENERAL_DATA_RAW_TABLE}"
        ).fetchone()[0]
        industries = con.execute(
            f"select count(*) from {tables.DLT_DATASET_NAME}.{tables.INDUSTRIES_RAW_TABLE}"
        ).fetchone()[0]
    assert contacts == 2
    assert industries == 1
