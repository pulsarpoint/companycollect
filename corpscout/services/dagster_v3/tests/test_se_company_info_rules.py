from dagster_v3.defs.se_company.info_rules import evidence_set_hash_for


def test_evidence_set_hash_for_is_order_independent() -> None:
    # Must equal the address final's MATERIALIZED expression:
    # lower(hex(SHA256(arrayStringConcat(arraySort(arrayMap(x -> toString(x), evidence_hashes)), '\n')))).
    forward = evidence_set_hash_for(["a" * 64, "b" * 64])
    reverse = evidence_set_hash_for(["b" * 64, "a" * 64])
    assert forward == reverse
    assert len(forward) == 64 and forward == forward.lower()
