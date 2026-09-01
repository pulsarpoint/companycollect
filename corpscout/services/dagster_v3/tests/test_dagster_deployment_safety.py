from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).parents[1]
TASKS_PATH = PROJECT_ROOT / "ansible" / "roles" / "dagster_dev" / "tasks" / "main.yml"
UNIT_TEMPLATE_PATH = (
    PROJECT_ROOT
    / "ansible"
    / "roles"
    / "dagster_dev"
    / "templates"
    / "corpscout-dagster-dev.service.j2"
)


def test_direct_systemd_stop_is_refused() -> None:
    unit_template = UNIT_TEMPLATE_PATH.read_text(encoding="utf-8")

    assert "RefuseManualStop=yes" in unit_template


def test_deployment_stop_is_authorized_only_after_the_run_preflight() -> None:
    tasks = yaml.safe_load(TASKS_PATH.read_text(encoding="utf-8"))
    task_names = [task["name"] for task in tasks]

    preflight_index = task_names.index("Refuse to interrupt active Dagster runs")
    stop_index = task_names.index("Stop Dagster after the active-run preflight")
    assert preflight_index < stop_index

    stop_task = tasks[stop_index]
    stop_block = {task["name"]: task for task in stop_task["block"]}
    override = stop_block["Temporarily allow the validated Dagster deployment stop"][
        "ansible.builtin.copy"
    ]
    assert "RefuseManualStop=no" in override["content"]
    assert "active-run preflight passed" in override["content"]
    assert "/zz-ansible-validated-stop.conf" in override["dest"]

    cleanup = {task["name"]: task for task in stop_task["always"]}
    cleanup_file = cleanup["Remove the transient Dagster stop permission"][
        "ansible.builtin.file"
    ]
    assert cleanup_file["path"] == override["dest"]
    assert cleanup_file["state"] == "absent"
    assert (
        cleanup["Reload the protected Dagster unit"]["ansible.builtin.systemd_service"][
            "daemon_reload"
        ]
        is True
    )
