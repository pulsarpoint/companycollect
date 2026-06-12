import dagster as dg


def test_definitions_load_with_automation_sensor():
    from dagster_corpscout.definitions import defs

    assert defs.resolve_asset_graph() is not None
    sensor_names = {sensor.name for sensor in defs.sensors}
    assert "automation_condition_sensor" in sensor_names


def test_automation_sensor_defaults_to_stopped():
    from dagster_corpscout.definitions import defs

    sensor = next(s for s in defs.sensors if s.name == "automation_condition_sensor")
    assert sensor.default_status == dg.DefaultSensorStatus.STOPPED
