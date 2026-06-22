from commoncrawl_enrich import llm


def fake_chat(system: str, user: str) -> str:
    if "industry" in system.lower():
        return '<think>looks like an accountant</think>{"label":"Accounting","nace_hint":"69.20","confidence":85}'
    return '{"emails":["skryta@firma.sk"],"phones":["+421911000000"]}'


def test_classify_industry_parses_thinking_response():
    arm = llm.LLMArm(chat=fake_chat)
    guess = arm.classify_industry("We do bookkeeping and tax for SMEs")
    assert guess.label == "Accounting" and guess.nace_hint == "69.20"
    assert guess.confidence == 85 and guess.method == "llm"


def test_recover_contacts_tags_source_method_llm():
    arm = llm.LLMArm(chat=fake_chat)
    emails, phones = arm.recover_contacts("contact text")
    assert emails[0].email == "skryta@firma.sk" and emails[0].source_method == "llm"
    assert phones[0].phone_e164 == "+421911000000" and phones[0].source_method == "llm"


def test_industry_returns_none_method_on_bad_json():
    arm = llm.LLMArm(chat=lambda s, u: "no json here")
    guess = arm.classify_industry("text")
    assert guess.method == "none" and guess.label == ""
