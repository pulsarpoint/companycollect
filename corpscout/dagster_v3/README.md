# dagster_v3

Small local Dagster spike for the Finland PRH YTJ base dataset. This project has
no Docker setup and no custom persistent Dagster instance; use it with `uv run dg
...` from this directory.

## Finland YTJ Assets

The current graph is intentionally small:

```text
PRH YTJ /all_companies
  -> dlt source
  -> finland_ytj_all_companies_duckdb
```

- `finland_ytj_all_companies_duckdb` runs a dlt source for PRH YTJ
  `/all_companies` and loads it into local DuckDB at
  `data/finland_ytj.duckdb`, replacing the
  `finland_prhytj.all_companies` table on each materialization.

The PRH response may be a zip file, so the dlt source handles unzip plus row
projection before loading into DuckDB.

## Exchange Rate Reference Asset

`dagster_v3.defs.exchange_rates` loads online exchange rates into ClickHouse table
`reference.exchange_rates`. This section only owns online sync and storage of
exchange-rate reference rows.

The asset key is `exchange_rates`, group `exchange_rates`.

Source:

- ECB Data Portal EXR API for EUR-to-currency reference rates.
- Identity rows for EUR-to-EUR.

Financial metric table changes that retain original currency values alongside
converted USD values are handled by country-specific pipelines.

## Exchange Rate Client

`dagster_v3.exchange_rates` is a plain Python package for reading exchange rates
from ClickHouse. It does not depend on Dagster resources, so Finland, Norway, and
other pipelines can use it directly after the `exchange_rates` asset has loaded
`reference.exchange_rates`.

```python
from decimal import Decimal

from dagster_v3.exchange_rates import ExchangeRateClient

fx = ExchangeRateClient.from_env()
rate = fx.usd_rate(currency="NOK", rate_date="2024-12-31")
amount_usd = fx.convert_to_usd(
    amount=Decimal("1250000"),
    currency="NOK",
    rate_date="2024-12-31",
)
```

The client resolves the latest common ECB rate date on or before the requested
date, so weekends and holidays use the previous available reference-rate date.
Returned rate objects include the effective rate date and source components used
for the USD conversion.

## Norway Brreg Assets

`dagster_v3.defs.norway_brreg` starts the Norway company spine from the Brreg
Enhetsregisteret bulk JSON gzip endpoint:

```text
https://data.brreg.no/enhetsregisteret/api/enheter/lastned
  -> norway_brreg_entities_duckdb
  -> norway_brreg_financial_statements_duckdb
```

The asset loads `norway_brreg.entities` in `data/norway_brreg.duckdb` with dlt.
Rows preserve the raw source record and extract stable company-spine fields such
as organization number, legal name, legal form, NACE-like activity codes,
business address, status flags, employee count, and latest submitted accounts
year. Original Norwegian descriptions are retained next to `_en` translation
columns for legal form, industry descriptions, articles purpose, activity text,
and company description.

`norway_brreg_financial_statements_duckdb` reads active entities with websites
and a latest submitted accounts year from `norway_brreg.entities`, then calls
Regnskapsregisteret per organization number. It writes
`norway_brreg.financial_statements` with original-currency financial amounts,
USD conversions from the local exchange-rate package, FX metadata, and the raw
financial record JSON.

Attribution: `Kilde: Bronnoysundregistrene (NLOD 2.0)`.

## Local Environment

Create a local `.env` from the example and fill in the MinIO/S3 credentials:

```bash
cp .env.example .env
```

Dagster's `dg` commands load `.env` from the project root automatically.

Validate definitions:

```bash
uv run dg check defs
uv run dg list defs
```

Materialize the assets locally:

```bash
uv run dg launch --assets finland_ytj_all_companies_duckdb
```

## Finland XBRL Raw XML

The XBRL raw step uses the YTJ DuckDB table to find active
companies with websites, then discovers PRH XBRL statements by registration-date
window and downloads matching XML documents.
The eligible-company filter is executed with DuckDB SQL directly against the
local dlt-loaded `finland_prhytj.all_companies` table.
The raw step also updates `raw/fi_prh_xbrl_xml_documents.parquet`, a Parquet
catalog with one row per XML document available in object storage. That catalog
is modeled as its own Dagster asset, `fi_prh_xbrl_xml_documents`, and parsed
XBRL assets use it as their default input.

```text
finland_ytj_all_companies_duckdb
finland_xbrl_financial_reports_duckdb
  -> finland_xbrl_eligible_financial_reports
  -> finland_xbrl_raw_xml_documents
  -> fi_prh_xbrl_xml_documents
  -> fi_prh_xbrl_statement_documents + fi_prh_xbrl_facts_raw
  -> fi_prh_xbrl_financial_metrics
```

Config behavior:

- `registered_date_start` / `registered_date_end`: configure the paginated XBRL
  financial report listing asset
- `financial_start_date`: optional, defaults to two years before the run date
- `max_reports`: optional, caps accepted eligible reports for local testing
- existing XML files are reused unless `refresh_existing` is true

Load financial report listings for a registration window:

```bash
uv run dg launch --assets finland_xbrl_financial_reports_duckdb --config-json '{"ops":{"finland_xbrl_financial_reports_duckdb":{"config":{"registered_date_start":"2026-01-01","registered_date_end":"2026-01-31"}}}}'
```

Download XML for eligible reports, optionally forcing XML refresh:

```bash
uv run dg launch --assets finland_xbrl_raw_xml_documents --config-json '{"ops":{"finland_xbrl_raw_xml_documents":{"config":{"refresh_existing":true}}}}'
```

Limit accepted reports for a small local run:

```bash
uv run dg launch --assets finland_xbrl_raw_xml_documents --config-json '{"ops":{"finland_xbrl_raw_xml_documents":{"config":{"max_reports":10}}}}'
```

Materialize the XML document catalog asset after raw XML download:

```bash
uv run dg launch --assets fi_prh_xbrl_xml_documents
```

Parse existing raw XML into DuckDB statement and fact tables:

By default, the parsed assets read `raw/fi_prh_xbrl_xml_documents.parquet`,
which is written by the raw XML asset and represented by
`fi_prh_xbrl_xml_documents`. Materialize those upstream assets first so that the
catalog exists:

```bash
uv run dg launch --assets 'fi_prh_xbrl_statement_documents,fi_prh_xbrl_facts_raw'
```

To parse a specific XML-document manifest without using the latest pointer, pass
`documents_key`:

```bash
uv run dg launch --assets 'fi_prh_xbrl_statement_documents,fi_prh_xbrl_facts_raw' --config-json '{"ops":{"finland_xbrl_parsed_tables":{"config":{"documents_key":"raw/fi_prh_xbrl_xml_documents.parquet"}}}}'
```

Parsed-layer quality and concept-profile counts are emitted as materialization
metadata on the parsed assets. They are not persisted as separate tables.

Build the first statement-level financial metrics table from parsed facts:

```bash
uv run dg launch --assets '+fi_prh_xbrl_financial_metrics'
```

The metrics asset writes `finland_prh_xbrl.fi_prh_xbrl_financial_metrics` in the
local DuckDB database. It starts with a narrow Finnish XBRL concept mapping for
current-period revenue, profit/loss, balance sheet, and personnel expense fields.
If the parsed statement/fact tables already exist in DuckDB, selecting only
`fi_prh_xbrl_financial_metrics` is enough.

## Getting started

### Installing dependencies

**Option 1: uv**

Ensure [`uv`](https://docs.astral.sh/uv/) is installed following their [official documentation](https://docs.astral.sh/uv/getting-started/installation/).

Create a virtual environment, and install the required dependencies using _sync_:

```bash
uv sync
```

Then, activate the virtual environment:

| OS | Command |
| --- | --- |
| MacOS | ```source .venv/bin/activate``` |
| Windows | ```.venv\Scripts\activate``` |

**Option 2: pip**

Install the python dependencies with [pip](https://pypi.org/project/pip/):

```bash
python3 -m venv .venv
```

Then activate the virtual environment:

| OS | Command |
| --- | --- |
| MacOS | ```source .venv/bin/activate``` |
| Windows | ```.venv\Scripts\activate``` |

Install the required dependencies:

```bash
pip install -e ".[dev]"
```

### Running Dagster

Start the Dagster UI web server:

```bash
dg dev
```

Open http://localhost:3000 in your browser to see the project.

## Learn more

To learn more about this template and Dagster in general:

- [Dagster Documentation](https://docs.dagster.io/)
- [Dagster University](https://courses.dagster.io/)
- [Dagster Slack Community](https://dagster.io/slack)





CREATE OR REPLACE SECRET local_s3 (

    TYPE s3,

    KEY_ID 'minioadmin',

    SECRET 'minioadmin',

    REGION 'us-east-1',

    ENDPOINT 'localhost:9000',

    USE_SSL false,

    URL_STYLE 'path'

);
