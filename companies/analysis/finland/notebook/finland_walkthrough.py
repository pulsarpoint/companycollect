import marimo

app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    mo.md(
        """
        # Finland conformance — inline walkthrough

        The **whole pipeline in one place**, with every step written out in cells —
        no `conformance` package imports. Downloads a small live sample straight into
        memory (the production notebook persists raw to S3 first), then:

        1. **prh_ytj JSONL → structured** with native Polars
        2. **prh_xbrl XML → facts** with an inline lxml parser
        3. **build the 4 canonical tables** (`registrations`, `company`, `financials`, `company_websites`)
        4. **validate + write Parquet**

        The other 4 canonical tables (`persons`, `company_people`, `company_contacts`,
        `company_relationships`) are *known-absent* in Finland open data.
        """
    )
    return (mo,)


@app.cell
def _():
    import datetime as dt
    import hashlib
    import json
    import time

    import polars as pl
    import requests
    from lxml import etree

    return dt, etree, hashlib, json, pl, requests, time


@app.cell
def _(dt):
    # --- Config & the financial metric map -------------------------------------
    COUNTRY = "FI"
    RUN_ID = "ref-inline"
    NOW = dt.datetime(2026, 6, 14)

    PRH_YTJ_COMPANIES_URL = "https://avoindata.prh.fi/opendata-ytj-api/v3/companies"
    PRH_XBRL_BASE_URL = "https://avoindata.prh.fi/opendata-xbrl-api/v3"
    USER_AGENT = "corpscout-conformance/0.1 (finland-walkthrough)"

    # Bounded so the live download finishes in a minute or two. Widen freely.
    MAX_COMPANIES = 200
    XBRL_START, XBRL_END = "2025-01-01", "2025-01-03"

    # (concept_qname, fi_dim:MCY member) -> canonical metric_code.
    # From companies/analysis/finland/prh_xbrl_schema_spike/schema_analysis.md
    METRIC_MAP = {
        ("fi_met:md103", "fi_MC:x673"): "revenue",
        ("fi_met:md103", "fi_MC:x689"): "operating_profit_loss",
        ("fi_met:md103", "fi_MC:x740"): "profit_loss",
        ("fi_met:mi53", "fi_MC:x360"): "total_assets",
        ("fi_met:mi53", "fi_MC:x376"): "equity",
        ("fi_met:mi53", "fi_MC:x424"): "liabilities",
        ("fi_met:mi53", "fi_MC:x399"): "cash_and_bank",
        ("fi_met:mi53", "fi_MC:x435"): "current_assets",
        ("fi_met:mi53", "fi_MC:x1768"): "current_receivables",
        ("fi_met:mi53", "fi_MC:x1811"): "current_liabilities",
        ("fi_met:md103", "fi_MC:x5"): "personnel_expenses",
        ("fi_met:md103", "fi_MC:x6"): "wages_and_salaries",
    }
    DURATION_METRICS = {
        "revenue", "operating_profit_loss", "profit_loss",
        "personnel_expenses", "wages_and_salaries",
    }
    return (COUNTRY, DURATION_METRICS, MAX_COMPANIES, METRIC_MAP, NOW,
            PRH_XBRL_BASE_URL, PRH_YTJ_COMPANIES_URL, RUN_ID, USER_AGENT,
            XBRL_END, XBRL_START)


@app.cell
def _(mo):
    mo.md("## 1. Download a bounded live sample (in-memory)")
    return


@app.cell
def _(json, time, requests, MAX_COMPANIES, PRH_XBRL_BASE_URL,
      PRH_YTJ_COMPANIES_URL, USER_AGENT, XBRL_END, XBRL_START):
    _session = requests.Session()
    _session.headers["User-Agent"] = USER_AGENT

    def _get(url, params, throttle=0.0):
        for attempt in range(5):
            if throttle:
                time.sleep(throttle)
            r = _session.get(url, params=params, timeout=120)
            if r.status_code == 429 or r.status_code >= 500:
                time.sleep(2 ** attempt)
                continue
            r.raise_for_status()
            return r
        r.raise_for_status()
        return r

    # prh_ytj: paginate companies -> NDJSON bytes (the idiomatic JSONL form).
    _lines, _page, _seen = [], 1, 0
    while _seen < MAX_COMPANIES:
        _payload = _get(PRH_YTJ_COMPANIES_URL, {"page": _page}).json()
        _companies = _payload.get("companies") or []
        if not _companies:
            break
        for _c in _companies:
            _lines.append(json.dumps(_c, ensure_ascii=False))
            _seen += 1
            if _seen >= MAX_COMPANIES:
                break
        _page += 1
    ytj_ndjson = ("\n".join(_lines) + "\n").encode("utf-8")

    # prh_xbrl: discover the window, download each statement XML.
    xbrl_statements = []
    _page = 1
    while True:
        _disc = _get(PRH_XBRL_BASE_URL + "/all_financial_statements",
                     {"registeredDateStart": XBRL_START, "registeredDateEnd": XBRL_END, "page": _page}).json()
        _items = _disc.get("financials", [])
        if not _items:
            break
        for _it in _items:
            _bid = str(_it.get("businessId") or "").strip()
            _fdate = str(_it.get("financialDate") or "").strip()
            if not _bid or not _fdate:
                continue
            _body = _get(PRH_XBRL_BASE_URL + "/financial",
                         {"businessId": _bid, "financialDate": _fdate}, throttle=1.0).content
            xbrl_statements.append({"business_id": _bid, "financial_date": _fdate, "body": _body})
        _total = int(_disc.get("totalResults") or 0)
        if _total and _page * 100 >= _total:
            break
        _page += 1

    {"ytj_bytes": len(ytj_ndjson), "xbrl_statements": len(xbrl_statements)}
    return xbrl_statements, ytj_ndjson


@app.cell
def _(mo):
    mo.md(
        """
        ## 2. prh_ytj JSONL → structured (native Polars)

        `pl.read_ndjson` reads the JSONL into nested struct/list columns; we reshape
        with vectorized expressions. Domain rules are expressed inline: liveness from
        `tradeRegisterStatus`/`endDate` (never the constant `status` field), current
        primary name = `type==1 & endDate null`, URL normalization, lowercased host.
        """
    )
    return


@app.cell
def _(pl, ytj_ndjson):
    _df = pl.read_ndjson(ytj_ndjson, infer_schema_length=None).with_columns(
        pl.col("businessId").struct.field("value").alias("business_id")
    )

    statuses = (
        _df.select(
            "business_id",
            pl.col("tradeRegisterStatus").alias("trade_register_status"),
            pl.col("registrationDate").alias("registration_date"),
            pl.col("endDate").fill_null("").alias("end_date"),
        )
        .with_columns(
            pl.when((pl.col("end_date") != "") | (pl.col("trade_register_status") == "3"))
            .then(pl.lit("ceased")).otherwise(pl.lit("active")).alias("lifecycle_status")
        )
        .with_columns((pl.col("lifecycle_status") == "active").alias("is_active"))
    )

    names_df = (
        _df.select("business_id", "names")
        .explode("names").drop_nulls("names").unnest("names")
        .select(
            "business_id", "name",
            pl.col("type").alias("name_type_code"),
            pl.col("endDate").is_null().alias("is_current"),
        )
    )

    websites_df = (
        _df.select("business_id", "website")
        .with_columns(
            pl.when(pl.col("website").is_not_null())
            .then(pl.col("website").struct.field("url"))
            .otherwise(pl.lit(None, dtype=pl.Utf8)).alias("url")
        )
        .filter(pl.col("url").is_not_null() & (pl.col("url") != ""))
        .with_columns(
            pl.when(pl.col("url").str.contains("://")).then(pl.col("url"))
            .otherwise(pl.concat_str([pl.lit("https://"), pl.col("url")])).alias("normalized_url")
        )
        .with_columns(
            pl.col("normalized_url").str.replace(r"^https?://", "").str.split("/").list.first().str.to_lowercase().alias("host"),
            pl.lit(True).alias("is_primary"),
        )
        .select("business_id", "url", "normalized_url", "host", "is_primary")
    )

    _addr = (
        _df.select("business_id", "addresses")
        .explode("addresses").drop_nulls("addresses").unnest("addresses")
        .with_columns(
            pl.col("postOffices").list.eval(pl.element().struct.field("city")).list.first().alias("city"),
            pl.col("postOffices").list.eval(pl.element().struct.field("municipalityCode")).list.first().alias("municipality_code"),
        )
    )
    _country = pl.col("country") if "country" in _addr.columns else pl.lit("FI")
    addresses_df = _addr.select(
        "business_id",
        pl.col("type").alias("address_type_code"),
        "street", pl.col("postCode").alias("post_code"),
        "city", "municipality_code", _country.alias("country"),
    )

    lines_df = (
        _df.select("business_id", "mainBusinessLine")
        .with_columns(
            pl.col("mainBusinessLine").struct.field("type").alias("business_line_type"),
            pl.col("mainBusinessLine").struct.field("typeCodeSet").alias("business_line_code_set"),
        )
        .filter(pl.col("business_line_type").is_not_null())
        .select("business_id", "business_line_type", "business_line_code_set")
    )

    {"statuses": statuses.height, "names": names_df.height, "websites": websites_df.height,
     "addresses": addresses_df.height, "business_lines": lines_df.height}
    return addresses_df, lines_df, names_df, statuses, websites_df


@app.cell
def _(mo):
    mo.md(
        """
        ## 3. prh_xbrl XML → facts (inline lxml parser)

        XML is not tabular, so this is a real parser (not Polars). Each fact is an
        `fi_met:*` element whose financial-statement line item lives in the referenced
        context as an `fi_dim:MCY` member; `fi_dim:REF` marks current vs prior period.
        """
    )
    return


@app.cell
def _(etree, hashlib, pl, xbrl_statements):
    _XBRLI = "http://www.xbrl.org/2003/instance"
    _XBRLDI = "http://xbrl.org/2006/xbrldi"
    _MET_NS = "http://www.suomi.fi/xbrl/crr/dict/met"
    _CANON = {
        "http://www.suomi.fi/xbrl/crr/dict/met": "fi_met",
        "http://www.suomi.fi/xbrl/crr/dict/dim": "fi_dim",
        "http://www.suomi.fi/xbrl/crr/dict/dom/MC": "fi_MC",
        "http://www.suomi.fi/xbrl/crr/dict/dom/RF": "fi_RF",
    }

    def _canon(text, nsmap):
        if not text or ":" not in text:
            return text
        prefix, local = text.split(":", 1)
        return f"{_CANON.get(nsmap.get(prefix), prefix)}:{local}"

    def _parse_facts(body, business_id, financial_date):
        root = etree.fromstring(body, parser=etree.XMLParser(resolve_entities=False))
        contexts = {}
        for ctx in root.findall(f"{{{_XBRLI}}}context"):
            mcy = ref = None
            for member in ctx.findall(f".//{{{_XBRLDI}}}explicitMember"):
                dim = _canon(member.get("dimension", ""), member.nsmap)
                mem = _canon((member.text or "").strip(), member.nsmap)
                if dim.endswith("MCY"):
                    mcy = mem
                elif dim.endswith("REF"):
                    ref = mem
            contexts[ctx.get("id")] = (mcy, ref)
        statement_key = hashlib.sha256(
            f"{business_id}:{financial_date}:{hashlib.sha256(body).hexdigest()}".encode()
        ).hexdigest()
        rows = []
        for el in root.iter():
            if not isinstance(el.tag, str) or el.get("contextRef") is None:
                continue
            qname = etree.QName(el)
            if qname.namespace != _MET_NS:
                continue
            try:
                value = float((el.text or "").strip())
            except ValueError:
                continue
            mcy, ref = contexts.get(el.get("contextRef"), (None, None))
            rows.append({
                "statement_key": statement_key,
                "business_id": business_id,
                "financial_date": financial_date,
                "concept_qname": f"fi_met:{qname.localname}",
                "mcy_member_code": mcy,
                "ref_member_code": ref,
                "numeric_value": value,
            })
        return rows

    _all = []
    for _s in xbrl_statements:
        _all.extend(_parse_facts(_s["body"], _s["business_id"], _s["financial_date"]))
    facts_df = pl.DataFrame(_all) if _all else pl.DataFrame(schema={
        "statement_key": pl.Utf8, "business_id": pl.Utf8, "financial_date": pl.Utf8,
        "concept_qname": pl.Utf8, "mcy_member_code": pl.Utf8, "ref_member_code": pl.Utf8,
        "numeric_value": pl.Float64,
    })
    {"facts": facts_df.height, "statements": len(xbrl_statements)}
    return (facts_df,)


@app.cell
def _(mo):
    mo.md(
        """
        ## 4. Build the canonical tables

        `company_uid` = `"c:" + sha1("FI:" + business_id)` (no LEI in YTJ open data);
        `registration_uid` = `"FI:" + business_id`. We compute them once over the
        unique business ids and join them in (Polars has no native sha1).
        """
    )
    return


@app.cell
def _(hashlib, pl, statuses):
    _bids = statuses["business_id"].unique().to_list()
    uid_map = pl.DataFrame({
        "business_id": _bids,
        "company_uid": ["c:" + hashlib.sha1(f"FI:{b}".encode()).hexdigest() for b in _bids],
        "registration_uid": [f"FI:{b}" for b in _bids],
    })
    uid_map.head()
    return (uid_map,)


@app.cell
def _(NOW, RUN_ID, addresses_df, lines_df, names_df, pl, statuses, uid_map, websites_df):
    # one-per-company reductions
    _primary_name = (
        names_df.filter(pl.col("name_type_code") == "1")
        .unique(subset="business_id", keep="first")
        .select("business_id", pl.col("name").alias("legal_name"))
    )
    _primary_web = (
        websites_df.unique(subset="business_id", keep="first")
        .select("business_id", pl.col("normalized_url").alias("primary_website"))
    )
    _primary_addr = addresses_df.unique(subset="business_id", keep="first").select(
        "business_id",
        pl.col("street").alias("addr_street"),
        pl.col("post_code").alias("addr_post_code"),
        pl.col("city").alias("addr_city"),
        pl.col("municipality_code").alias("addr_municipality_code"),
        pl.col("country").alias("addr_country"),
    )
    _primary_line = lines_df.unique(subset="business_id", keep="first").select(
        "business_id",
        pl.col("business_line_type").alias("activity_code"),
        pl.col("business_line_code_set").alias("activity_scheme"),
    )

    regs_df = (
        statuses.select("business_id", "lifecycle_status", "is_active", "registration_date", "end_date")
        .join(uid_map, on="business_id", how="left")
        .join(_primary_name, on="business_id", how="left")
        .join(_primary_web, on="business_id", how="left")
        .join(_primary_addr, on="business_id", how="left")
        .join(_primary_line, on="business_id", how="left")
        .with_columns(
            pl.lit("FI").alias("country"),
            pl.col("business_id").alias("registration_number"),
            pl.lit("finland_prhytj").alias("registry_source"),
            pl.lit(1).cast(pl.UInt8).alias("is_primary"),
            pl.lit("domestic").alias("entity_role"),
            pl.lit(None, dtype=pl.Utf8).alias("legal_form_code"),
            pl.col("is_active").cast(pl.UInt8).alias("is_active"),
            pl.col("registration_date").str.to_date(strict=False).alias("incorporation_date"),
            pl.when(pl.col("end_date") != "").then(pl.col("end_date").str.to_date(strict=False))
            .otherwise(None).alias("dissolution_date"),
            pl.col("addr_country").fill_null("FI").alias("addr_country"),
            pl.lit(None, dtype=pl.Utf8).alias("vat_number"),
            pl.lit(None, dtype=pl.Utf8).alias("eu_id"),
            pl.lit(None, dtype=pl.Utf8).alias("lei"),
            pl.lit(RUN_ID).alias("source_run_id"),
            pl.lit(NOW).alias("ingested_at"),
            pl.lit(NOW).alias("updated_at"),
        )
        .select(
            "registration_uid", "company_uid", "country", "registration_number", "registry_source",
            "is_primary", "entity_role", "legal_name", "legal_form_code", "lifecycle_status", "is_active",
            "incorporation_date", "dissolution_date", "addr_street", "addr_post_code", "addr_city",
            "addr_municipality_code", "addr_country", "activity_code", "activity_scheme",
            "vat_number", "eu_id", "lei", "primary_website", "source_run_id", "ingested_at", "updated_at",
        )
    )
    regs_df.head()
    return (regs_df,)


@app.cell
def _(NOW, pl, regs_df):
    company_df = (
        regs_df.group_by("company_uid").agg(
            pl.col("legal_name").first().alias("primary_name"),
            pl.col("legal_form_code").first().alias("legal_form_code"),
            pl.col("incorporation_date").first().alias("incorporation_date"),
            pl.col("dissolution_date").first().alias("dissolution_date"),
            pl.col("primary_website").first().alias("primary_website"),
            pl.col("is_active").max().alias("_any_active"),
            pl.col("country").unique().alias("operating_countries"),
            pl.len().alias("_n"),
        )
        .with_columns(
            pl.lit("surrogate").alias("uid_scheme"),
            pl.lit(None, dtype=pl.Utf8).alias("lei"),
            pl.when(pl.col("_any_active") == 1).then(pl.lit("active")).otherwise(pl.lit("inactive")).alias("status"),
            pl.lit("FI").alias("home_country"),
            pl.col("_n").cast(pl.UInt16).alias("registration_count"),
            pl.concat_list([pl.lit("finland_prhytj")]).alias("sources"),
            pl.lit("finland-v1").alias("resolution_version"),
            pl.lit(NOW).alias("first_seen_at"),
            pl.lit(NOW).alias("updated_at"),
        )
        .select(
            "company_uid", "uid_scheme", "lei", "primary_name", "status", "legal_form_code",
            "home_country", "incorporation_date", "dissolution_date", "registration_count",
            "operating_countries", "primary_website", "sources", "resolution_version",
            "first_seen_at", "updated_at",
        )
    )
    company_df.head()
    return (company_df,)


@app.cell
def _(DURATION_METRICS, METRIC_MAP, NOW, RUN_ID, facts_df, pl, uid_map):
    _map_df = pl.DataFrame(
        [{"concept_qname": k[0], "mcy_member_code": k[1], "metric_code": v} for k, v in METRIC_MAP.items()]
    )
    financials_df = (
        facts_df.join(_map_df, on=["concept_qname", "mcy_member_code"], how="inner")
        .join(uid_map, on="business_id", how="left")
        .with_columns(
            pl.lit("FI").alias("country"),
            pl.col("statement_key").alias("statement_id"),
            pl.lit(None, dtype=pl.Date).alias("period_start"),
            pl.col("financial_date").str.to_date(strict=False).alias("period_end"),
            pl.when(pl.col("metric_code").is_in(list(DURATION_METRICS)))
            .then(pl.lit("duration")).otherwise(pl.lit("instant")).alias("period_type"),
            pl.when(pl.col("ref_member_code").is_not_null())
            .then(pl.lit("prior")).otherwise(pl.lit("current")).alias("period_reference"),
            pl.lit("individual").alias("basis"),
            pl.lit("EUR").alias("currency"),
            pl.col("numeric_value").cast(pl.Float64).alias("value"),
            pl.concat_str([pl.col("concept_qname"), pl.lit("/"), pl.col("mcy_member_code")]).alias("source_metric_id"),
            pl.lit("finland_prh_xbrl").alias("registry_source"),
            pl.lit("finland-fin-v1").alias("mapping_version"),
            pl.lit(RUN_ID).alias("source_run_id"),
            pl.lit(NOW).alias("ingested_at"),
            pl.lit(NOW).alias("updated_at"),
        )
        .select(
            "company_uid", "registration_uid", "country", "statement_id", "period_start", "period_end",
            "period_type", "period_reference", "basis", "currency", "metric_code", "value",
            "source_metric_id", "registry_source", "mapping_version", "source_run_id", "ingested_at", "updated_at",
        )
    )
    financials_df.head()
    return (financials_df,)


@app.cell
def _(NOW, hashlib, pl, uid_map, websites_df):
    _w = websites_df.join(uid_map, on="business_id", how="left")
    _website_uid = [
        hashlib.sha1(f"registration:{r}:{u}".encode()).hexdigest()
        for r, u in zip(_w["registration_uid"].to_list(), _w["normalized_url"].to_list())
    ]
    websites_canon_df = _w.with_columns(
        pl.Series("website_uid", _website_uid),
        pl.lit("FI").alias("country"),
        pl.lit("registration").alias("scope"),
        pl.col("is_primary").cast(pl.UInt8).alias("is_primary"),
        pl.lit("registry").alias("source_kind"),
        pl.lit("registry_field").alias("discovery_method"),
        pl.lit("finland_prhytj").alias("registry_source"),
        pl.lit(1.0).cast(pl.Float32).alias("confidence"),
        pl.lit(0).cast(pl.UInt8).alias("is_live"),
        pl.lit(NOW).alias("first_seen_at"),
        pl.lit(NOW).alias("last_seen_at"),
        pl.lit(NOW).alias("updated_at"),
    ).select(
        "website_uid", "company_uid", "registration_uid", "country", "scope", "url", "normalized_url",
        "host", "is_primary", "source_kind", "discovery_method", "registry_source", "confidence",
        "is_live", "first_seen_at", "last_seen_at", "updated_at",
    )
    websites_canon_df.head()
    return (websites_canon_df,)


@app.cell
def _(mo):
    mo.md("## 5. Validate keys + write canonical Parquet")
    return


@app.cell
def _(company_df, financials_df, pl, regs_df, websites_canon_df):
    import pathlib

    _tables = {
        "registrations": (regs_df, "registration_uid"),
        "company": (company_df, "company_uid"),
        "financials": (financials_df, None),
        "company_websites": (websites_canon_df, "website_uid"),
    }
    _out = pathlib.Path("output/canonical_inline")
    _out.mkdir(parents=True, exist_ok=True)

    _report = {}
    for _name, (_df, _key) in _tables.items():
        if _key is not None:
            _nn = _df.filter(pl.col(_key).is_not_null())
            assert _nn.height == _nn.select(_key).n_unique(), f"{_name}: duplicate {_key}"
        _df.write_parquet(_out / f"{_name}.parquet")
        _report[_name] = _df.shape

    _report["financial_metrics"] = financials_df["metric_code"].unique().to_list()
    _report
    return


if __name__ == "__main__":
    app.run()
