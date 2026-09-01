import json

from dagster_v3.defs.xbrl_common.parity import compare_document_facts


def _fact(concept, context, value, mcy=""):
    return {
        "concept_qname": concept, "context_id": context, "value_kind": "numeric",
        "numeric_value": value, "mcy_member_code": mcy, "ref_member_code": "",
    }


def test_identical_facts_match():
    old = [_fact("fi_met:md103", "cur", "500000", "fi_MC:x673")]
    new = [_fact("fi_met:md103", "cur", "500000.000000", "fi_MC:x673")]
    result = compare_document_facts(document_key="d1", old_facts=old, new_facts=new)
    assert result.status == "match"


def test_value_mismatch_reported():
    old = [_fact("fi_met:md103", "cur", "500000")]
    new = [_fact("fi_met:md103", "cur", "999")]
    result = compare_document_facts(document_key="d1", old_facts=old, new_facts=new)
    assert result.status == "mismatch"
    assert result.value_mismatches == 1
    assert "fi_met:md103" in json.loads(result.details)[0]["key"]


def test_explained_rule_downgrades_mismatch():
    old = [_fact("fi_met:md103", "cur", "500000")]
    new = [_fact("fi_met:md103", "cur", "999")]
    rule = lambda fact: fact["concept_qname"] == "fi_met:md103"
    result = compare_document_facts(
        document_key="d1", old_facts=old, new_facts=new, explained_rules=[rule]
    )
    assert result.status == "explained"


def test_missing_keys_counted():
    old = [_fact("fi_met:a", "cur", "1"), _fact("fi_met:b", "cur", "2")]
    new = [_fact("fi_met:a", "cur", "1"), _fact("fi_met:c", "cur", "3")]
    result = compare_document_facts(document_key="d1", old_facts=old, new_facts=new)
    assert result.missing_in_new == 1
    assert result.missing_in_old == 1
    assert result.status == "mismatch"
