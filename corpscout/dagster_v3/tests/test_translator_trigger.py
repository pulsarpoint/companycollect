from dagster_v3.defs.norway_brreg.assets import (
    NORWAY_BRREG_TRANSLATE_WORKFLOW_ID,
    build_norway_brreg_translate_input,
)


def test_translate_workflow_id_is_stable():
    assert NORWAY_BRREG_TRANSLATE_WORKFLOW_ID == "translate-norway_brreg"


def test_build_translate_input_targets_norway_brreg():
    params = build_norway_brreg_translate_input()
    assert params.source_slug == "norway_brreg"
    assert params.batch_size == 50
    assert params.max_batch_failures == 0
    assert params.queue_dir == "data/translator"
