"""Declarative source config for Finland PRH YTJ. Mirrors the Go source catalog."""

SOURCE_NAME = "finland_prhytj"
COUNTRY = "finland"
DISPLAY_NAME = "Finland PRH YTJ"
BUCKET = "source-finland-prhytj"
BASE_URL = "https://avoindata.prh.fi/opendata-ytj-api/v3/companies"
DESCRIPTION_PATH = "/opendata-ytj-api/v3/description"
PAGE_SIZE = 100

# Order matches the Go source catalog sort_order.
CODE_LISTS = [
    ("REK", "en"),
    ("REK_KDI", "en"),
    ("VIRANOM", "en"),
    ("TLAJI", "en"),
    ("YRMU", "en"),
    ("STATUS3", "en"),
    ("KIELI", "en"),
]


def snapshot_object_key(run_id: str) -> str:
    return f"runs/{run_id}/source.ndjson"


def code_list_object_key(run_id: str, code: str, lang: str) -> str:
    return f"runs/{run_id}/codelists/{code}.{lang}.tsv"


def manifest_object_key(run_id: str) -> str:
    return f"runs/{run_id}/manifest.json"
