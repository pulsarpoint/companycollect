"""People job selections and their execution order."""

import dagster as dg

from dagster_v3.defs.se_company.person import assets


def _repo():
    from dagster_v3.definitions import defs as load_defs

    return load_defs().get_repository_def()


def test_only_the_two_global_people_jobs_are_registered() -> None:
    repo = _repo()
    assert {job.name for job in repo.get_all_jobs() if job.name.startswith("se_company_person_")} == {
        "se_company_person_sync_job", "se_company_person_refresh_job",
    }
    assert not any(schedule.name.startswith("se_company_person_") for schedule in repo.schedule_defs)
    assert dg.AssetKey("se_company_person_fold") not in repo.asset_graph.get_all_asset_keys()


def test_the_normalize_asset_runs_after_the_extractors() -> None:
    node = _repo().asset_graph.get(dg.AssetKey("se_company_person_normalize"))
    assert {k.path[-1] for k in node.parent_keys} == set(assets.EXTRACTOR_ASSET_NAMES)


def test_no_person_sensor_yet() -> None:
    assert not any("se_company_person" in sensor.name for sensor in _repo().sensor_defs)


def test_backoffice_jobs_have_all_company_scope_and_publish_after_their_upstreams() -> None:
    repo = _repo()
    refresh = repo.get_job("se_company_person_refresh_job")
    sync = repo.get_job("se_company_person_sync_job")
    assert {key.path[-1] for key in refresh.asset_layer.executable_asset_keys} == {
        *assets.EXTRACTOR_ASSET_NAMES, "se_company_person_normalize", "se_company_person_match_input", "se_company_person_match",
        "se_company_person_publish",
    }
    assert {key.path[-1] for key in sync.asset_layer.executable_asset_keys} == {
        *assets.EXTRACTOR_ASSET_NAMES, "se_company_person_normalize", "se_company_person_match_input",
    }
    assert not repo.has_job("se_company_person_publish_job")
    input_node = repo.asset_graph.get(dg.AssetKey("se_company_person_match_input"))
    assert input_node.parent_keys == {dg.AssetKey("se_company_person_normalize")}
    graph = repo.asset_graph.get(dg.AssetKey("se_company_person_publish"))
    assert {key.path[-1] for key in graph.parent_keys} == {
        "se_company_person_normalize", "se_company_person_match",
    }
    assert assets.se_company_person_publish.op.pool == assets.FOLD_POOL
    assert assets.se_company_person_publish.partitions_def is None
    config = {"ops": {
        **{name: {"config": {"execute": True, "page_size": 10_000}} for name in assets.EXTRACTOR_ASSET_NAMES},
        "se_company_person_normalize": {"config": {"changed_only": True}},
        "se_company_person_match_input": {"config": {"changed_only": True}},
        "se_company_person_match": {"config": {
            "provider": "openrouter", "model": "chosen/model", "base_url": "https://example.com/v1",
            "api_key_environment_variable": "WORKER_LLM_KEY", "system_prompt": "Return pairs.",
            "prompt_version": "people:prompt-1:r2", "temperature": 0, "concurrency": 1, "changed_only": True,
        }},
        "se_company_person_publish": {"config": {"changed_only": True, "page_size": 10_000}},
    }}
    dg.validate_run_config(refresh, config)
    dg.validate_run_config(sync, {"ops": {
        name: value for name, value in config["ops"].items()
        if name not in {"se_company_person_match", "se_company_person_publish"}
    }})
