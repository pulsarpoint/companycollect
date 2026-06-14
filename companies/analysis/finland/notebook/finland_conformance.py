import marimo

app = marimo.App()


@app.cell
def _():
    import datetime as dt
    import json
    import pathlib

    import polars as pl

    from conformance import download
    from conformance import structured as st
    from conformance.build_company import build_registrations, build_company
    from conformance.build_financials import build_financials
    from conformance.build_websites import build_websites
    from conformance import schemas
    from conformance.validate import validate_table

    OUT = pathlib.Path("output")
    RUN_ID = "ref-2025-01"
    NOW = dt.datetime(2026, 6, 14)
    # Bounded reference sample (keep small; widen later for a fuller run).
    MAX_COMPANIES = 500
    XBRL_START, XBRL_END = "2025-01-01", "2025-01-08"
    return (build_company, build_financials, build_registrations, build_websites,
            dt, download, json, pl, schemas, st, validate_table, OUT, RUN_ID, NOW,
            MAX_COMPANIES, XBRL_START, XBRL_END)


@app.cell
def _(download, RUN_ID, MAX_COMPANIES, XBRL_START, XBRL_END):
    # Phase 3 assets: download raw -> S3 (bounded reference sample).
    ytj_meta = download.download_prh_ytj(RUN_ID, max_companies=MAX_COMPANIES)
    xbrl_meta = download.download_prh_xbrl(RUN_ID, XBRL_START, XBRL_END)
    ytj_meta, xbrl_meta
    return ytj_meta, xbrl_meta


@app.cell
def _(download, json, st, dt, RUN_ID, pl, OUT, ytj_meta, xbrl_meta):
    # Load raw from S3 and run the parsers -> structured Parquet.
    s3 = download.s3_client()
    ndjson = s3.get_object(Bucket=download.BUCKET, Key=ytj_meta["snapshot_key"])["Body"].read()
    ytj_tables = st.ytj_structured_from_ndjson(ndjson)

    listing = json.loads(s3.get_object(Bucket=download.BUCKET, Key=xbrl_meta["listing_key"])["Body"].read())
    statements = []
    for d in listing["documents"]:
        body = s3.get_object(Bucket=download.BUCKET, Key=d["object_key"])["Body"].read()
        statements.append({**d, "body": body})
    xbrl_tables = st.xbrl_structured_from_statements(statements, run_id=RUN_ID, parsed_at=dt.datetime(2026, 6, 14))

    (OUT / "structured").mkdir(parents=True, exist_ok=True)
    _written = set()
    for _name, _df in {**ytj_tables, **xbrl_tables}.items():
        if _df.height and _name not in _written:
            _df.write_parquet(OUT / "structured" / f"{_name}.parquet")
            _written.add(_name)
    return ytj_tables, xbrl_tables


@app.cell
def _(build_registrations, build_company, build_financials, build_websites,
      validate_table, schemas, ytj_tables, xbrl_tables, NOW, RUN_ID, OUT):
    # Build canonical tables and validate against the contract.
    regs = build_registrations(ytj_tables, run_id=RUN_ID, now=NOW)
    comp = build_company(regs, now=NOW)
    fin = build_financials(xbrl_tables["fi_prh_xbrl_facts"],
                           xbrl_tables["fi_prh_xbrl_statement_documents"], run_id=RUN_ID, now=NOW)
    sites = build_websites(ytj_tables["fi_prhytj_websites"], run_id=RUN_ID, now=NOW)

    validate_table(regs, schemas.REGISTRATIONS, unique_key="registration_uid")
    validate_table(comp, schemas.COMPANY, unique_key="company_uid")
    validate_table(fin, schemas.FINANCIALS)
    validate_table(sites, schemas.COMPANY_WEBSITES, unique_key="website_uid")

    (OUT / "canonical").mkdir(parents=True, exist_ok=True)
    for _name, _df in {"registrations": regs, "company": comp,
                       "financials": fin, "company_websites": sites}.items():
        _df.write_parquet(OUT / "canonical" / f"{_name}.parquet")
    return comp, fin, regs, sites


@app.cell
def _(comp, fin, regs, sites):
    # Cardinalities for the partition doc. The other 4 contract tables
    # (persons, company_people, company_contacts, company_relationships) are
    # KNOWN-ABSENT in Finland open data.
    {
        "company": comp.height, "registrations": regs.height,
        "financials": fin.height, "company_websites": sites.height,
        "distinct_companies": comp["company_uid"].n_unique(),
        "financial_periods": fin["period_end"].n_unique() if fin.height else 0,
    }
    return


if __name__ == "__main__":
    app.run()
