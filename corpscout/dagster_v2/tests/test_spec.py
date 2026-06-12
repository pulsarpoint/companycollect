from dagster_corpscout.sources.finland.prh_ytj import spec


def test_constants():
    assert spec.SOURCE_NAME == "finland_prhytj"
    assert spec.BUCKET == "source-finland-prhytj"
    assert spec.BASE_URL.startswith("https://avoindata.prh.fi/")
    assert spec.PAGE_SIZE == 100


def test_code_lists_match_go_catalog():
    codes = [code for code, _ in spec.CODE_LISTS]
    assert codes == ["REK", "REK_KDI", "VIRANOM", "TLAJI", "YRMU", "STATUS3", "KIELI"]
    assert all(lang == "en" for _, lang in spec.CODE_LISTS)


def test_object_keys():
    assert spec.snapshot_object_key("20260611T120000Z") == "runs/20260611T120000Z/source.ndjson"
    assert (
        spec.code_list_object_key("20260611T120000Z", "REK", "en")
        == "runs/20260611T120000Z/codelists/REK.en.tsv"
    )
