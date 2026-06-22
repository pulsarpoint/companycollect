"""Offline tests for the WappalyzerClient (fake HTTP session)."""
from commoncrawl_enrich.wappalyzer_client import WappalyzerClient


class FakeResp:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, results_by_id):
        self._results = results_by_id
        self.calls: list = []

    def post(self, url, json=None, timeout=None):
        self.calls.append((url, json))
        ids = [it["id"] for it in json["items"]]
        return FakeResp({"results": [{"id": i, "technologies": self._results.get(i, [])} for i in ids]})


def test_analyze_batch_maps_to_technology():
    session = FakeSession({"p1": [
        {"name": "PHP", "categories": ["Programming languages"], "version": "8.1.2", "confidence": 100},
        {"name": "Nginx", "categories": ["Web servers", "Reverse proxies"], "version": ""},
    ]})
    client = WappalyzerClient("http://svc:9876", session=session)
    out = client.analyze_batch([("p1", {"Server": ["nginx"]}, "<html></html>")])
    techs = {t.technology: t for t in out["p1"]}
    assert techs["PHP"].version == "8.1.2" and techs["PHP"].category == "Programming languages"
    assert techs["Nginx"].category == "Web servers"  # primary category only
    assert session.calls[0][0] == "http://svc:9876/analyze"


def test_analyze_batch_respects_batch_size():
    session = FakeSession({})
    client = WappalyzerClient("http://svc", session=session, batch_size=2)
    items = [(f"id{i}", {}, "body") for i in range(5)]
    client.analyze_batch(items)
    assert len(session.calls) == 3  # 2 + 2 + 1


def test_analyze_single():
    session = FakeSession({"0": [{"name": "WordPress", "categories": ["CMS"], "version": "6.1"}]})
    client = WappalyzerClient("http://svc", session=session)
    techs = client.analyze({"Server": ["apache"]}, "<html>wp-content</html>")
    assert len(techs) == 1 and techs[0].technology == "WordPress" and techs[0].version == "6.1"


def test_from_env_none_when_unset(monkeypatch):
    monkeypatch.delenv("COMMONCRAWL_WAPPALYZER_URL", raising=False)
    assert WappalyzerClient.from_env() is None


def test_from_env_builds_client(monkeypatch):
    monkeypatch.setenv("COMMONCRAWL_WAPPALYZER_URL", "http://svc:9876")
    client = WappalyzerClient.from_env()
    assert client is not None and client._base == "http://svc:9876"
