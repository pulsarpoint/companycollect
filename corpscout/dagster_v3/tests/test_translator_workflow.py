"""Import-sanity tests confirming the new per-source workflow modules are importable and well-formed."""
from translator.norway_brreg.workflows import (
    BuildQueueWorkflow,
    BuildQueueWorkflowInput,
    BuildQueueWorkflowOutput,
    TranslateWorkflow,
    TranslateWorkflowInput,
    TranslateWorkflowOutput,
)


def test_build_queue_workflow_defn_importable():
    assert BuildQueueWorkflow is not None


def test_translate_workflow_defn_importable():
    assert TranslateWorkflow is not None


def test_build_queue_workflow_input_has_no_scan_or_flush_timeout():
    """scan_timeout_seconds / flush_timeout_seconds must not exist on the new input."""
    fields = {f.name for f in BuildQueueWorkflowInput.__dataclass_fields__.values()}
    assert "scan_timeout_seconds" not in fields
    assert "flush_timeout_seconds" not in fields


def test_translate_workflow_input_has_no_scan_or_flush_timeout():
    fields = {f.name for f in TranslateWorkflowInput.__dataclass_fields__.values()}
    assert "scan_timeout_seconds" not in fields
    assert "flush_timeout_seconds" not in fields
