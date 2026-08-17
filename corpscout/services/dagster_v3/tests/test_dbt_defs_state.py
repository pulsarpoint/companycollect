from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).parents[1]
DEFS_ROOT = PROJECT_ROOT / "src" / "dagster_v3" / "defs"
DBT_DEFS_STATE_CASES = (
    (
        DEFS_ROOT / "company_domain_suggestions" / "dbt_component" / "defs.yaml",
        "DbtProjectComponent__dbt__",
    ),
    (
        DEFS_ROOT / "company_domain_suggestions" / "web_dbt_component" / "defs.yaml",
        "DbtProjectComponent__dbt____web-features",
    ),
    (
        DEFS_ROOT / "company_serving" / "dbt_component" / "defs.yaml",
        "DbtProjectComponent__dbt____company-serving",
    ),
)
PYTHONIC_DBT_PROJECTS = (
    (
        DEFS_ROOT / "finland_ytj" / "resolved.py",
        DEFS_ROOT / "finland_ytj" / "dbt",
    ),
    (
        DEFS_ROOT / "exchange_rates_v2" / "assets.py",
        DEFS_ROOT / "exchange_rates_v2" / "dbt",
    ),
)
HOT_SYNC_TASKS = (
    PROJECT_ROOT / "ansible" / "roles" / "dagster_dev" / "tasks" / "sync.yml"
)
FULL_SYNC_TASKS = (
    PROJECT_ROOT / "ansible" / "roles" / "dagster_dev" / "tasks" / "main.yml"
)
FULL_SYNC_EXCLUDES = (
    PROJECT_ROOT
    / "ansible"
    / "roles"
    / "dagster_dev"
    / "files"
    / "rsync-excludes.txt"
)


def test_dbt_components_never_refresh_shared_state_during_runtime_loads() -> None:
    for defs_path, _state_directory in DBT_DEFS_STATE_CASES:
        component = yaml.safe_load(defs_path.read_text(encoding="utf-8"))

        assert component["attributes"]["prepare_if_dev"] is False


def test_pythonic_dbt_projects_never_prepare_during_runtime_imports() -> None:
    for definitions_path, _project_path in PYTHONIC_DBT_PROJECTS:
        definitions_source = definitions_path.read_text(encoding="utf-8")

        assert ".prepare_if_dev()" not in definitions_source


def test_hot_sync_preflight_requires_every_dbt_component_state() -> None:
    hot_sync_tasks = HOT_SYNC_TASKS.read_text(encoding="utf-8")

    for _defs_path, state_directory in DBT_DEFS_STATE_CASES:
        state_prefix = f".local_defs_state/{state_directory}/project"

        assert f"{state_prefix}/dbt_project.yml" in hot_sync_tasks
        assert f"{state_prefix}/target/manifest.json" in hot_sync_tasks


def test_hot_sync_preflight_requires_every_pythonic_dbt_manifest() -> None:
    hot_sync_tasks = HOT_SYNC_TASKS.read_text(encoding="utf-8")

    for _definitions_path, project_path in PYTHONIC_DBT_PROJECTS:
        project_prefix = project_path.relative_to(PROJECT_ROOT).as_posix()

        assert f"{project_prefix}/dbt_project.yml" in hot_sync_tasks
        assert f"{project_prefix}/target/manifest.json" in hot_sync_tasks


def test_deployments_ship_dbt_manifests_without_runtime_artifacts() -> None:
    hot_sync_tasks = yaml.safe_load(HOT_SYNC_TASKS.read_text(encoding="utf-8"))
    full_sync_tasks = yaml.safe_load(FULL_SYNC_TASKS.read_text(encoding="utf-8"))
    full_sync_excludes = FULL_SYNC_EXCLUDES.read_text(encoding="utf-8")

    hot_sync_tasks_by_name = {task["name"]: task for task in hot_sync_tasks}
    for task_name in (
        "Preview the Dagster content hot-sync",
        "Stage the complete Dagster definitions tree outside the watched checkout",
    ):
        rsync_arguments = hot_sync_tasks_by_name[task_name]["ansible.builtin.command"][
            "argv"
        ]

        for _definitions_path, project_path in PYTHONIC_DBT_PROJECTS:
            target_prefix = project_path.relative_to(DEFS_ROOT.parent).as_posix()

            assert f"--include=**/{target_prefix}/target/" in rsync_arguments
            assert (
                f"--include=**/{target_prefix}/target/manifest.json"
                in rsync_arguments
            )

        assert "--exclude=**/dbt/target/***" in rsync_arguments
        assert "--exclude=**/dbt/logs/" in rsync_arguments

    full_sync_tasks_by_name = {task["name"]: task for task in full_sync_tasks}
    for task_name in (
        "Preview the local-to-host Dagster synchronization",
        "Synchronize changed Dagster source files",
    ):
        rsync_arguments = full_sync_tasks_by_name[task_name]["ansible.builtin.command"][
            "argv"
        ]

        for _definitions_path, project_path in PYTHONIC_DBT_PROJECTS:
            target_prefix = project_path.relative_to(DEFS_ROOT.parent).as_posix()

            assert f"--include=**/{target_prefix}/target/" in rsync_arguments
            assert (
                f"--include=**/{target_prefix}/target/manifest.json"
                in rsync_arguments
            )

    assert "**/dbt/target/***" in full_sync_excludes
    assert "**/dbt/target/*/" not in full_sync_excludes
