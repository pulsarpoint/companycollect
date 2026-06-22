import json

import duckdb

from commoncrawl_enrich import run


def test_load_targets_from_parquet(tmp_path):
    manifest = tmp_path / "m.parquet"
    duckdb.connect().execute(
        f"copy (select 'a.sk' root_domain, 1 source_rank, 9.0 open_page_rank "
        f"union all select 'b.sk', 2, 8.0) to '{manifest}' (format parquet)")
    targets = run.load_targets(str(manifest), limit=1)
    assert len(targets) == 1 and targets[0].root_domain == "a.sk"


def test_run_pipeline_writes_parquet_and_metrics(tmp_path, monkeypatch):
    from commoncrawl_enrich.models import DomainEnrichment, DomainTarget

    def fake_enrich_domains(targets, **_):
        return [DomainEnrichment(target=t, fetch_status="not_in_index") for t in targets]

    monkeypatch.setattr(run.enrich, "enrich_domains", fake_enrich_domains)
    targets = [DomainTarget("a.sk", 1, 9.0)]
    report = run.run_pipeline(targets, out_dir=tmp_path, resolve=None, fetch=None, llm=None,
                              crawl_id="c", max_workers=1)
    assert report["total"] == 1
    assert (tmp_path / "domain_enrichment.parquet").exists()
    assert json.loads((tmp_path / "metrics.json").read_text())["total"] == 1
